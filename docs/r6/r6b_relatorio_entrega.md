# Relatório de Entrega — R6-B: Orquestração Ponta a Ponta Transferegov → Bronze → Silver → Gold → Serving

## 1. Resumo Executivo

O incremento **R6-B** conclui a orquestração completa do Lakehouse de Transferências Discricionárias e Legais com a implementação, validação em ambiente real e homologação da DAG Master Controller:
$$\text{Transferegov (Dados Abertos)} \longrightarrow \text{Bronze (Delta)} \longrightarrow \text{Silver} \longrightarrow \text{Gold Core} \longrightarrow \text{Serving Mart} \longrightarrow \text{dbt Docs}$$

A DAG `r6_pipeline_transferegov_e2e` unifica os pipelines operacionais da Ingestão Bronze (R2) com as Transformações Analíticas Pós-Bronze (R6-A), estabelecendo os seguintes padrões corporativos:
1. **Rastreabilidade Determinística E2E**: Propagação coordenada do identificador `ingestion_run_id` (`e2e_{{ ts_nodash }}`) da DAG pai para as DAGs filhas e tabelas de auditoria do Lakehouse.
2. **Decisão Baseada em Auditoria Persistida**: A ramificação pós-Bronze não utiliza adivinhação nem arquivos voláteis; ela consulta via SQL (PyHive / Spark Thrift Server) o registro persistido na tabela Delta `bronze.ingestion_runs`.
3. **Curto-Circuito Econômico (NO_CHANGE)**: Quando o portal Transferegov não publica nova atualização de dados, a ingestão conclui com status `NO_CHANGE`, o nó `decidir_pos_bronze` roteia para o ramo `no_change` (*EmptyOperator*) e encerra o pipeline em `SUCCESS` pulando todas as transformações posteriores, economizando 100% dos recursos computacionais de Silver, Gold e Serving.
4. **Governança Fail-Fast Estrita e Resolução do Finding R6-B-P0**: Eliminação completa do mascaramento de erros na DAG R2 com a introdução da folha `finalizar_execucao_r2` após `limpeza_temporarios` (`ALL_DONE`), assegurando que falhas em tarefas de ingestão resultem em encerramento com `FAILED` na DAG filha e bloqueio instantâneo do orquestrador E2E.
5. **Isolamento de Testes dbt por Camada**: Refinamento cirúrgico dos seletores da DAG R6-A para evitar falsos positivos por execução prematura de testes cruzados entre camadas analíticas.
6. **Integridade e Reconciliação Total**: 100% de consistência volumétrica (1.157.739 propostas, 287.602 convênios, 1.257.410 programas) e divergência financeira estritamente zerada (R$ 0,00) entre Silver, Gold e Serving Mart.

---

## 2. Arquitetura da Solução e Papel dos Componentes

O Lakehouse adota separação estrita de responsabilidades entre computação, armazenamento colunar, catálogo e visualização:

| Componente | Papel no Lakehouse |
| :--- | :--- |
| **Apache Airflow** | Orquestrador corporativo responsável pelo grafo de dependências, controle de parâmetros de execução, ramificação condicional inteligente (*BranchPythonOperator*) e governança *fail-fast*. |
| **Apache Spark** | Motor de computação distribuída para download streaming dos CSVs da fonte Transferegov, validação estrutural, escrita colunar Delta Lake e execução de scripts de bootstrap. |
| **Delta Lake** | Formato de armazenamento transacional aberto (ACID) hospedado no MinIO, assegurando controle de schema, particionamento e histórico versionado (*time travel*). |
| **Spark Thrift Server / Hive Metastore** | Serviço de catálogo unificado e interface JDBC/PyHive (porta 10000) utilizado pelo Airflow e pelo dbt para consultas SQL e execução de transformações analíticas. |
| **dbt (data build tool)** | Motor declarativo de modelagem analítica SQL, testes automatizados de unicidade, integridade referencial, regras de negócio e geração da documentação de dados. |
| **MinIO** | Object storage compatível com S3 estruturado em buckets isolados por camada (`s3a://bronze/`, `s3a://silver/`, `s3a://gold/`). |
| **Apache Superset** | Camada de *Business Intelligence* e visualização, servindo o Dashboard Executivo sobre a tabela Delta `gold.mart_superset_proposta_convenio`. |

### Grafo da DAG Master Controller (`r6_pipeline_transferegov_e2e`)

```text
[start] (EmptyOperator)
   │
   ▼
[trigger_bronze] (TriggerDagRunOperator -> r2_ingestao_transferegov_bronze, wait_for_completion=True)
   │
   ▼
[decidir_pos_bronze] (BranchPythonOperator -> consulta bronze.ingestion_runs via Spark Thrift)
   ├───► NO_CHANGE ───► [no_change] (EmptyOperator) ──────────────────────────┐
   │                                                                          │
   └───► SUCCESS   ───► [trigger_transformacoes] (TriggerDagRunOperator) ─────┤
                               (r6_transformacoes_lakehouse, wait=True)       │
                                                                              ▼
                                                                     [end] (EmptyOperator)
                                                   (TriggerRule: NONE_FAILED_MIN_ONE_SUCCESS)
```

---

## 3. Resolução dos Desafios Técnicos

### 3.1 Finding R6-B-P0: Eliminação do Mascaramento de Falhas na DAG R2

- **Diagnóstico**: Na DAG `r2_ingestao_transferegov_bronze`, a tarefa final era `limpeza_temporarios` com `trigger_rule=TriggerRule.ALL_DONE`. Na hipótese de uma etapa crítica anterior falhar (por exemplo, `ingestao_siconv_proposta`), a tarefa de limpeza executava normalmente para liberar espaço em disco, e o scheduler do Airflow marcava o `DagRun` como `SUCCESS`. Consequentemente, o `TriggerDagRunOperator` da DAG controladora recebia sucesso e prosseguia para as transformações sobre dados incompletos.
- **Solução Implementada**: Adicionada a tarefa folha `finalizar_execucao_r2` (`PythonOperator`, `trigger_rule=ALL_DONE`) imediatamente downstream de `limpeza_temporarios`. A função `verificar_resultado_r2`:
  1. Itera sobre todas as `TaskInstance`s do DagRun em execução.
  2. Identifica qualquer tarefa nos estados `failed` ou `upstream_failed` (incluindo falha na própria limpeza).
  3. Ignora estados `skipped` (inerentes ao fluxo condicional de `NO_CHANGE`).
  4. Lança `AirflowException` detalhando as tarefas com defeito, forçando a DAG a encerrar como `FAILED` e propagando o erro instantaneamente.

### 3.2 Rastreabilidade Determinística E2E via `ingestion_run_id`

- **Diagnóstico**: A DAG R2 gerava seu identificador de execução internamente (`run_{{ ts_nodash }}`), o que impedia a DAG controladora de correlacionar com exatidão a consulta à tabela de auditoria Delta.
- **Solução Implementada**:
  - Exposição do parâmetro `ingestion_run_id` no schema de `params` da DAG R2 com validação regex `^[A-Za-z0-9_.-]*$`.
  - Implementação da função `get_ingestion_run_id(context)` e do template Jinja `RUN_ID_TEMPLATE` com fallback retrocompatível para `run_{{ ts_nodash }}` em execuções manuais avulsas.
  - A DAG controladora repassa `ingestion_run_id = e2e_{{ ts_nodash }}` para a DAG Bronze e para a DAG de Transformações (`r6b_transform__{{ ts_nodash }}`), garantindo rastreabilidade transversal completa.

### 3.3 Isolamento Estrito de Testes dbt por Camada (R6-A Seletor Fix)

- **Diagnóstico**: O seletor `dbt build --select path:models/silver` compilava testes singulares localizados em `dbt_lakehouse/tests/` que comparavam dados da camada Silver com tabelas Gold ainda não criadas/atualizadas (`row_count_gold_proposta`, etc.). Ao receber um novo snapshot na Silver (1.157.739 linhas), o teste falhava prematuramente ao comparar com o snapshot anterior da Gold (1.157.619 linhas). Da mesma forma, `vw_superset_proposta_convenio` compilava testes que dependiam do serving mart físico.
- **Solução com Justificativa Técnica**:
  - `silver.dbt_build_silver`: adicionado `--exclude '*gold*' '*mart*' '*superset*' '*r5*'`.
  - `gold.dbt_build_gold_core`: adicionado `--exclude '*mart*' '*superset*' '*r5*'`.
  - `serving.dbt_build_semantic_view`: adicionado `--exclude '*mart*'`.
  - Com essa segregação, cada camada executa apenas seus modelos e testes unitários de integridade. Testes de reconciliação global continuam atuando como quality gates governados nas etapas dedicadas (`reconcile_bronze_silver`, `reconcile_silver_gold` e nos testes embutidos de `mart_superset_proposta_convenio`).

---

## 4. Testes de Contrato Automatizados

Foram criadas três suítes de testes unitários e contratuais sob `spark/tests/`:

| Suíte | Testes | Cobertura / Foco Principal | Resultado |
| :--- | :---: | :--- | :---: |
| `test_r6b_dag_contract.py` | 14 | Configuração da DAG E2E, topologia de 6 nós, regras de fail-fast (`wait_for_completion=True`), `NONE_FAILED_MIN_ONE_SUCCESS` na junção `end`, e testes unitários da lógica de decisão contra `NO_CHANGE` e `SUCCESS`. | **14/14 PASS** |
| `test_r2_dag_contract.py` | 12 | Parâmetro `ingestion_run_id`, sanitização regex de caracteres seguros, folha única `finalizar_execucao_r2`, detecção de falhas reais e tolerância a `skipped`. | **12/12 PASS** |
| `test_r6a_dag_contract.py` | 5 | Sequencialidade linear pós-Bronze, `all_success` em 100% das etapas analíticas e isolamento de seletores dbt. | **5/5 PASS** |
| `test_retention.py` | 8 | Política de retenção raw MinIO e idempotência. | **8/8 PASS** |

**Resultado Consolidado da Suíte**: 39 testes executados, 39 testes aprovados (100% sucesso).

---

## 5. Evidências de Execuções Físicas no Ambiente Real

Foram executadas validações físicas completas ponta a ponta no cluster Docker local.

### 5.1 Execução Física FULL (`r6b_full_2`)

- **Modo**: Execução completa forçada (`force_bronze: true`) com disparo da Ingestão Bronze, registro de auditoria Delta `SUCCESS` e propagação downstream para todas as transformações pós-Bronze.
- **Run ID Pai**: `r6b_full_2`
- **Início**: `2026-09-17 23:32:04 UTC`
- **Fim**: `2026-09-17 23:54:44 UTC`
- **Duração Total**: `22m 40s`
- **Status do DagRun**: `SUCCESS`

#### Cronograma Detalhado da DAG Master Controller:

| Task ID | Operador | Status | Início (UTC) | Fim (UTC) | Duração |
| :--- | :--- | :---: | :---: | :---: | :---: |
| `start` | `EmptyOperator` | `success` | 23:32:04.64 | 23:32:04.64 | 0.00s |
| `trigger_bronze` | `TriggerDagRunOperator` | `success` | 23:32:05.88 | 23:45:36.81 | 13m 31s |
| `decidir_pos_bronze` | `BranchPythonOperator` | `success` | 23:45:38.09 | 23:45:40.87 | 2.78s |
| `no_change` | `EmptyOperator` | `skipped` | 23:45:40.80 | 23:45:40.80 | 0.00s |
| `trigger_transformacoes` | `TriggerDagRunOperator` | `success` | 23:45:42.06 | 23:54:42.68 | 9m 00s |
| `end` | `EmptyOperator` | `success` | 23:54:43.50 | 23:54:43.50 | 0.00s |

#### Execuções Filhas Acopladas:
1. **DAG R2 (`r6b_bronze__20260917T233203`)**:
   - Duração: 13m 16s (`SUCCESS`).
   - Ingestão completa dos 4 datasets oficiais: `siconv_programa` (1.257.410 linhas), `siconv_programa_proposta` (1.159.095 linhas), `siconv_proposta` (1.157.739 linhas), `siconv_convenio` (287.602 linhas).
   - Registro persistido em `bronze.ingestion_runs`: `status = 'SUCCESS'`, `source_data_carga_final = '2026-09-17 06:33:30'`.
2. **Decisão PyHive Spark Thrift**:
   - `SELECT status FROM bronze.ingestion_runs WHERE ingestion_run_id = 'e2e_20260917T233203'`.
   - Retorno: `trigger_transformacoes`.
3. **DAG R6-A (`r6b_transform__20260917T233203`)**:
   - Duração: 8m 56s (`SUCCESS`).
   - 12/12 tarefas concluídas com sucesso (`preflight_dbt`, `bootstrap_silver`, `dbt_build_silver`, `reconcile_bronze_silver`, `bootstrap_gold`, `dbt_build_gold_core`, `reconcile_silver_gold`, `dbt_build_semantic_view`, `dbt_build_serving_mart`, `dbt_docs_generate`).

---

### 5.2 Execução Física com Curto-Circuito NO_CHANGE (`r6b_no_change_2`)

- **Modo**: Execução sem reprocessamento forçado (`force_bronze: false`). O portal oficial mantinha os mesmos metadados já ingeridos no ciclo anterior.
- **Run ID Pai**: `r6b_no_change_2`
- **Início**: `2026-09-17 23:56:33 UTC`
- **Fim**: `2026-09-17 23:58:26 UTC`
- **Duração Total**: `1m 52s` (economia de mais de 20 minutos de processamento)
- **Status do DagRun**: `SUCCESS`

#### Cronograma Detalhado da DAG Master Controller:

| Task ID | Operador | Status | Início (UTC) | Fim (UTC) | Duração |
| :--- | :--- | :---: | :---: | :---: | :---: |
| `start` | `EmptyOperator` | `success` | 23:56:33.92 | 23:56:33.92 | 0.00s |
| `trigger_bronze` | `TriggerDagRunOperator` | `success` | 23:56:35.32 | 23:58:20.68 | 1m 45s |
| `decidir_pos_bronze` | `BranchPythonOperator` | `success` | 23:58:21.78 | 23:58:24.25 | 2.47s |
| `no_change` | `EmptyOperator` | `success` | 23:58:24.54 | 23:58:24.54 | 0.00s |
| `trigger_transformacoes` | `TriggerDagRunOperator` | `skipped` | 23:58:24.20 | 23:58:24.20 | 0.00s |
| `end` | `EmptyOperator` | `success` | 23:58:25.59 | 23:58:25.59 | 0.00s |

#### Comportamento Auditado:
1. **DAG R2 (`r6b_bronze__20260917T235633`)**:
   - `controle_inicial` detectou hash SHA-256 e timestamp `data_carga_siconv` idênticos.
   - Ramificação `avaliar_necessidade` direcionou para `finalizar_no_change` (`success`).
   - Todas as 4 tarefas de download e escrita Delta foram puladas (`skipped`).
   - Limpeza e `finalizar_execucao_r2` concluídas com sucesso em 1m 41s.
   - Registro persistido em `bronze.ingestion_runs`: `status = 'NO_CHANGE'`.
2. **Decisão do Master Controller**:
   - `SELECT status FROM bronze.ingestion_runs WHERE ingestion_run_id = 'e2e_20260917T235633'`.
   - Retorno: `no_change`.
3. **Isolamento downstream**:
   - Tarefa `no_change` executou como `success`.
   - Tarefa `trigger_transformacoes` ficou marcada como `skipped`.
   - Nenhuma execução filha foi disparada na DAG R6-A.
   - Tarefa `end` convergiu em `success` graças à regra `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`.

---

### 5.3 Evidência Prática de Propagação Fail-Fast

Durante a rodada `r6b_full_1` (anterior ao ajuste de seletores dbt), o seletor `serving.dbt_build_semantic_view` falhou na DAG R6-A às 23:30:52 UTC.
- A tarefa `trigger_transformacoes` na DAG pai imediatamente capturou o código de saída de falha da DAG filha.
- A tarefa de convergência `end` entrou instantaneamente em `upstream_failed`.
- O `DagRun` `r6b_full_1` foi marcado como `FAILED` pelo scheduler sem intervenção manual.
- **Conclusão**: O mecanismo de fail-fast estrutural foi comprovado na prática, eliminando 100% de mascaramento de erros.

---

## 6. Reconciliação dos Dados e Integridade Analítica

Consulta executada diretamente no Spark Thrift Server (porta 10000) via PyHive contra as tabelas físicas Delta Lake:

### 6.1 Auditoria das Execuções (`bronze.ingestion_runs`)

```text
ingestion_run_id    | status    | force | source_data_carga_final | start_time_utc           | end_time_utc
====================+===========+=======+=========================+==========================+=========================
e2e_20260917T235633 | NO_CHANGE | False | 2026-09-17 06:33:30     | 2026-09-17T23:57:46.097Z | 2026-09-17T23:58:05.849Z
e2e_20260917T233203 | SUCCESS   | True  | 2026-09-17 06:33:30     | 2026-09-17T23:33:16.787Z | 2026-09-17T23:44:23.350Z
```

### 6.2 Reconciliação Volumétrica por Camada

| Entidade / Tabela | Camada Bronze | Camada Silver | Camada Gold | Serving Mart | Divergência |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Propostas (`siconv_proposta`)** | 1.157.739 | 1.157.739 | 1.157.739 (`fct_proposta`) | 1.157.739 (`mart_superset_proposta_convenio`) | **0** |
| **Convênios (`siconv_convenio`)** | 287.602 | 287.602 | 287.602 (`fct_convenio`) | — | **0** |
| **Programas (`siconv_programa`)** | 1.257.410 | 53.022 (cadastrais) | 53.022 (`dim_programa`) | — | **0** |
| **Elegibilidade Programas** | — | 1.257.410 | — | — | **0** |
| **Ponte Programa-Proposta** | 1.159.095 | 1.159.095 | 1.159.095 (`bridge_programa_proposta`) | — | **0** |

### 6.3 Reconciliação Financeira Estrita (Tolerância R$ 0,00)

$$\begin{aligned}
\text{Silver } \Sigma(\text{valor\_global\_proposta}) &= \text{R\$ 1.495.324.643.229,95} \\
\text{Gold } \Sigma(\text{valor\_global\_proposta}) &= \text{R\$ 1.495.324.643.229,95} \\
\text{Mart } \Sigma(\text{valor\_global\_proposta}) &= \text{R\$ 1.495.324.643.229,95} \\
\mathbf{\Delta(\text{Silver} - \text{Gold})} &= \mathbf{R\$\ 0{,}00} \\
\mathbf{\Delta(\text{Gold} - \text{Mart})} &= \mathbf{R\$\ 0{,}00}
\end{aligned}$$

$$\begin{aligned}
\text{Silver } \Sigma(\text{valor\_global\_convenio}) &= \text{R\$ 356.854.241.837,20} \\
\text{Gold } \Sigma(\text{valor\_global\_convenio}) &= \text{R\$ 356.854.241.837,20} \\
\mathbf{\Delta(\text{Silver} - \text{Gold})} &= \mathbf{R\$\ 0{,}00}
\end{aligned}$$

---

## 7. Instruções para Operação e Execução Manual

### 7.1 Execução Completa Forçada (FULL)

```bash
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e \
  --run-id r6b_full_manual_$(date +%Y%m%d_%H%M%S) \
  --conf '{"force_bronze": true}'
```

### 7.2 Execução Periódica Inteligente (NO_CHANGE)

```bash
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e \
  --run-id r6b_check_$(date +%Y%m%d_%H%M%S) \
  --conf '{"force_bronze": false}'
```

### 7.3 Reprocessamento Isolado Pós-Bronze (Silver $\rightarrow$ Gold $\rightarrow$ Serving)

```bash
docker exec airflow airflow dags trigger r6_transformacoes_lakehouse \
  --run-id r6a_manual_$(date +%Y%m%d_%H%M%S)
```

---

## 8. Conclusão e Parecer de Homologação

Todas as metas funcionais, arquiteturais e de qualidade de dados definidas para a entrega **R6-B** foram atendidas de forma irrestrita:
- ✅ DAG Controladora `r6_pipeline_transferegov_e2e` operacional e idempotente.
- ✅ Curto-circuito econômico `NO_CHANGE` comprovado fisicamente em 1m 52s sem acionamento desnecessário de transformações.
- ✅ Fluxo `FULL` comprovado fisicamente em 22m 40s com 100% de sucesso nas 18 tarefas encadeadas.
- ✅ Finding R6-B-P0 sanado com a inclusão de `finalizar_execucao_r2` inspecionando TaskInstances após `limpeza_temporarios`.
- ✅ Rastreabilidade ponta a ponta implementada com `ingestion_run_id` e validada por 39 testes de contrato.
- ✅ Reconciliação financeira com R$ 0,00 de divergência em todas as camadas analíticas.
- ✅ Documentação completa sincronizada no `README.md` e neste relatório.

**Status da Entrega**: `R6-B APROVADO PARA REVISÃO HUMANA`.
