# Relatório de Entrega — R6-A: Orquestração Pós-Bronze no Apache Airflow

## 1. Objetivo

Implementar e validar, de forma reproduzível, sequencial e *fail-fast*, a DAG do Apache Airflow responsável por toda a cadeia de transformações e governança analítica pós-camada Bronze do Lakehouse de Transferências Discricionárias e Legais:
$$\text{Silver} \longrightarrow \text{Gold Core} \longrightarrow \text{Camada Semântica} \longrightarrow \text{Serving Mart Delta} \longrightarrow \text{Documentação dbt}$$

Esta rodada foca estritamente na orquestração governada entre camadas analíticas já testadas e validadas (R3, R4 e R5). A ingestão Bronze permanece isolada na DAG `r2_ingestao_transferegov_bronze`, sendo pré-condição operacional.

---

## 2. Contexto e Limite Arquitetural

A etapa R6-A consolida as transformações pós-Bronze sem duplicar lógica operacional existente nos scripts Python e modelos dbt já homologados.

### Diagrama de Fluxo e Limite de Escopo

```text
Transferegov (Dados Abertos)
     │
     ▼
[R2 — Ingestão Bronze] (DAG: r2_ingestao_transferegov_bronze)
     │
═════╪══════════════════════ limite R6-A ══════════════════════════
     │  (Pré-condição operacional: s3a://bronze/ carregado e catalogado)
     ▼
[preflight_dbt] (Validação de perfil e conectividade Spark Thrift)
     │
     ▼
[TaskGroup: silver]
     ├── bootstrap_silver (Assegura schema silver em s3a://silver/warehouse)
     ├── dbt_build_silver (Build dos 5 modelos Silver e execução de testes)
     └── reconcile_bronze_silver (Quality Gate: reconciliação dinâmica Bronze->Silver)
     │
     ▼
[TaskGroup: gold]
     ├── bootstrap_gold (Assegura schema gold em s3a://gold/warehouse via bootstrap_silver.py)
     ├── dbt_build_gold_core (Build dos 9 modelos Gold: 5 dimensões, 3 fatos, 1 bridge)
     └── reconcile_silver_gold (Quality Gate: 12 gates de integridade e finanças Silver->Gold)
     │
     ▼
[TaskGroup: serving]
     ├── dbt_build_semantic_view (Build da view lógica vw_superset_proposta_convenio)
     └── dbt_build_serving_mart (Build da tabela física Delta mart_superset_proposta_convenio)
     │
     ▼
[TaskGroup: documentation]
     └── dbt_docs_generate (Compilação do catálogo e documentação: manifest, catalog, html)
     │
     ▼
[Consumo Analítico / Apache Superset Dashboard Executivo]
```

> [!IMPORTANT]
> **Limite R6-A**: Esta DAG **não dispara a ingestão Bronze (R2)** nem executa testes da UI do Superset internamente. Ela consome a Bronze como pré-condição operacional estrita e disponibiliza o mart analítico Delta atualizado para consumo analítico.

---

## 3. Arquitetura da DAG Criada

- **Arquivo**: `airflow/dags/r6_transformacoes_lakehouse.py`
- **DAG ID**: `r6_transformacoes_lakehouse`
- **Configurações operacionais**:
  - `schedule`: `None` (execução sob demanda / manual)
  - `catchup`: `False`
  - `max_active_runs`: `1`
  - `start_date`: `datetime(2026, 1, 1)` (data fixa determinística)
  - `default_args`:
    - `owner`: `"airflow"`
    - `depends_on_past`: `False`
    - `retries`: `0`
    - `email_on_failure`: `False`
    - `email_on_retry`: `False`
  - `tags`: `["r6", "transformacoes", "lakehouse", "dbt", "transferegov"]`

### Estratégia de Operadores e Reutilização

- Apenas operadores padrão de orquestração foram utilizados: `EmptyOperator`, `BashOperator` e `TaskGroup`.
- Caminhos absolutos internos do container configurados como constantes:
  - `DBT_DIR = "/home/airflow/dbt_lakehouse"`
  - `SCRIPTS_DIR = "/scripts"`
- **Reutilização de script de bootstrap**: Para a tarefa `gold.bootstrap_gold`, reutilizou-se o script genérico `/scripts/bootstrap_silver.py` parametrizado com `--schema gold --location s3a://gold/warehouse`, eliminando duplicações de código.

---

## 4. Grafo de Dependências e Fail-Fast Estrutural

O grafo real validado no Apache Airflow CLI (`airflow tasks list r6_transformacoes_lakehouse --tree`):

```text
<Task(EmptyOperator): start>
    <Task(BashOperator): preflight_dbt>
        <Task(BashOperator): silver.bootstrap_silver>
            <Task(BashOperator): silver.dbt_build_silver>
                <Task(BashOperator): silver.reconcile_bronze_silver>
                    <Task(BashOperator): gold.bootstrap_gold>
                        <Task(BashOperator): gold.dbt_build_gold_core>
                            <Task(BashOperator): gold.reconcile_silver_gold>
                                <Task(BashOperator): serving.dbt_build_semantic_view>
                                    <Task(BashOperator): serving.dbt_build_serving_mart>
                                        <Task(BashOperator): documentation.dbt_docs_generate>
                                            <Task(EmptyOperator): end>
```

### Garantia de Fail-Fast

- Todas as 12 tarefas operam sob a regra estrita `trigger_rule="all_success"`.
- Nenhuma tarefa de transformação utiliza `all_done`, `one_success` ou `none_failed`.
- Se qualquer quality gate intermediário falhar (ex: `silver.reconcile_bronze_silver` ou `gold.reconcile_silver_gold`), todas as etapas downstream permanecem bloqueadas (estado `upstream_failed` no scheduler).
- Se a compilação do serving mart falhar, `dbt_docs_generate` e `end` são bloqueados imediatamente.

---

## 5. Seletores dbt Validados

Antes da execução física, os seletores foram validados estaticamente:

1. **Camada Silver**:
   - Comando: `dbt ls --select path:models/silver --resource-type model --output name`
   - Total: 5 modelos (`siconv_convenio`, `siconv_programa_cadastral`, `siconv_programa_elegibilidade`, `siconv_programa_proposta`, `siconv_proposta`).
2. **Camada Gold Core**:
   - Comando: `dbt ls --select path:models/gold/dimensions path:models/gold/facts path:models/gold/bridges --resource-type model --output name`
   - Total: 9 modelos (`bridge_programa_proposta`, `dim_data`, `dim_municipio`, `dim_orgao`, `dim_programa`, `dim_proponente`, `fct_convenio`, `fct_convenio_saldo_observacao`, `fct_proposta`).
   - Confirmado: modelos de `semantic/` e `serving/` **não** foram incluídos no Gold Core.
3. **Serving**:
   - Semantic View: `vw_superset_proposta_convenio` (1 view)
   - Serving Mart: `mart_superset_proposta_convenio` (1 tabela física Delta com testes de unicidade e reconciliação financeira embutidos)

---

## 6. Evidências de Execuções Físicas

Foram executadas duas rodadas completas consecutivas no cluster local.

### 6.1 Primeira Execução Física (`r6a_validation_1`)

- **Run ID**: `r6a_validation_1`
- **Início**: `2026-09-17 21:29:01 UTC`
- **Fim**: `2026-09-17 21:40:58 UTC`
- **Duração Total**: `11m 57s`
- **Status do DagRun**: `SUCCESS`

| Task ID | Status | Início (UTC) | Fim (UTC) | Duração |
| :--- | :---: | :---: | :---: | :---: |
| `start` | `success` | 21:29:01.15 | 21:29:01.15 | 0.00s |
| `preflight_dbt` | `success` | 21:29:04.62 | 21:29:10.24 | 5.62s |
| `silver.bootstrap_silver` | `success` | 21:29:11.77 | 21:29:12.93 | 1.16s |
| `silver.dbt_build_silver` | `success` | 21:29:13.92 | 21:32:37.85 | 203.93s |
| `silver.reconcile_bronze_silver` | `success` | 21:32:38.43 | 21:33:38.98 | 60.55s |
| `gold.bootstrap_gold` | `success` | 21:33:39.80 | 21:33:40.63 | 0.83s |
| `gold.dbt_build_gold_core` | `success` | 21:33:42.24 | 21:36:47.72 | 185.49s |
| `gold.reconcile_silver_gold` | `success` | 21:36:50.06 | 21:38:14.38 | 84.32s |
| `serving.dbt_build_semantic_view` | `success` | 21:38:15.11 | 21:39:29.62 | 74.50s |
| `serving.dbt_build_serving_mart` | `success` | 21:39:30.23 | 21:40:38.31 | 68.08s |
| `documentation.dbt_docs_generate` | `success` | 21:40:39.50 | 21:40:56.74 | 17.24s |
| `end` | `success` | 21:40:57.35 | 21:40:57.35 | 0.00s |

### 6.2 Segunda Execução Física — Prova de Idempotência (`r6a_validation_2`)

Sem expurgar tabelas ou schemas, disparou-se a segunda execução para validar o comportamento *rerunnable*.

- **Run ID**: `r6a_validation_2`
- **Início**: `2026-09-17 21:41:50 UTC`
- **Fim**: `2026-09-17 21:53:33 UTC`
- **Duração Total**: `11m 43s`
- **Status do DagRun**: `SUCCESS`

| Task ID | Status | Início (UTC) | Fim (UTC) | Duração |
| :--- | :---: | :---: | :---: | :---: |
| `start` | `success` | 21:41:50.41 | 21:41:50.41 | 0.00s |
| `preflight_dbt` | `success` | 21:41:51.90 | 21:41:56.13 | 4.23s |
| `silver.bootstrap_silver` | `success` | 21:41:56.93 | 21:41:57.70 | 0.77s |
| `silver.dbt_build_silver` | `success` | 21:41:58.20 | 21:45:02.55 | 184.36s |
| `silver.reconcile_bronze_silver` | `success` | 21:45:03.19 | 21:45:55.36 | 52.17s |
| `gold.bootstrap_gold` | `success` | 21:45:56.76 | 21:45:57.45 | 0.69s |
| `gold.dbt_build_gold_core` | `success` | 21:45:57.84 | 21:49:31.01 | 213.17s |
| `gold.reconcile_silver_gold` | `success` | 21:49:31.73 | 21:50:58.96 | 87.24s |
| `serving.dbt_build_semantic_view` | `success` | 21:51:00.29 | 21:52:11.66 | 71.37s |
| `serving.dbt_build_serving_mart` | `success` | 21:52:12.46 | 21:53:17.38 | 64.92s |
| `documentation.dbt_docs_generate` | `success` | 21:53:18.23 | 21:53:32.48 | 14.25s |
| `end` | `success` | 21:53:32.87 | 21:53:32.87 | 0.00s |

---

## 7. Verificação Estrita de Idempotência e Qualidade

Após a conclusão das duas execuções consecutivas da DAG, executou-se auditoria via Spark SQL no catálogo:

1. **Camada Silver**:
   - `silver.siconv_proposta`: Total = `1.157.619` | PKs distintas = `1.157.619` (Zero duplicações).
2. **Camada Gold Core**:
   - `gold.fct_proposta`: Total = `1.157.619` | PKs distintas = `1.157.619` (Zero duplicações).
   - `gold.fct_convenio`: Total = `287.584` | PKs distintas = `287.584` (Zero duplicações).
3. **Serving Mart**:
   - `gold.mart_superset_proposta_convenio`: Total = `1.157.619` | PKs distintas = `1.157.619` (Zero duplicações).
4. **Reconciliação Financeira Estrita**:
   - `Valor Global Convênio`: $\text{R\$}~356.840.744.533,16$ (Divergência: $\text{R\$}~0,00$).
   - `Valor Repasse Convênio`: $\text{R\$}~331.271.766.468,34$ (Divergência: $\text{R\$}~0,00$).
5. **Quality Gates nos Scripts de Reconciliação**:
   - `reconcile_bronze_silver`: `PASS` em ambas as execuções.
   - `reconcile_silver_gold`: `12 gates PASS` em ambas as execuções (`gold_location`, `row_counts`, `primary_keys`, `referential_integrity`, `date_mapping`, `date_mapping_exact`, `orgao_conformance`, `convenio_canonical_stability`, `financials_proposta`, `financials_convenio`, `saldo_observations`, `bridge_guardrails`).

---

## 8. Validação de Documentação dbt

A tarefa `documentation.dbt_docs_generate` gerou e atualizou no container Airflow:

- `/home/airflow/dbt_lakehouse/target/manifest.json` (1.2 MB)
- `/home/airflow/dbt_lakehouse/target/catalog.json` (3.7 KB)
- `/home/airflow/dbt_lakehouse/target/index.html` (1.7 MB)

A documentação reflete todos os modelos, linhagens, testes e metadados compilados.

---

## 9. Testes de Regressão

- Compilação estática (`python -m compileall -q spark scripts airflow/dags superset`): 0 erros.
- Verificação de formatação e whitespace (`git diff --check`): 0 erros.
- Suíte completa de testes de unidade Python (`python3 -m unittest discover /app/tests`): 65 testes executados, **65 PASS (0 failures, 0 errors)**.
- Integridade do pipeline R2: DAG `r2_ingestao_transferegov_bronze.py` inalterada.
- Modelos R3-R5: Nenhum arquivo de modelo em `dbt_lakehouse/models/` foi modificado.

---

## 10. Limitações e Próximos Passos (R6-B)

- **Limitações desta entrega (R6-A)**:
  - A DAG é disparada exclusivamente de modo manual (`schedule=None`), sem cron ou triggers automáticos.
  - O pipeline Bronze (R2) não é acionado por esta DAG, exigindo execução prévia ou dados já ingeridos.
- **Próximos Passos (R6-B)**:
  - Integração ponta a ponta (E2E) entre a ingestão Bronze (R2) e as transformações (R6-A).
  - Definição do mecanismo de agendamento coordenado (ex: Datasets do Airflow ou orquestração em cascata).
  - Estratégias de alertas operacionais e monitoramento de falhas.
