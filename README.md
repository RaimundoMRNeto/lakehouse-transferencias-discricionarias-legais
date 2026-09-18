# Lakehouse de Transferências Discricionárias e Legais

Projeto de Engenharia de Dados baseado em arquitetura Lakehouse moderna para ingestão, tratamento, modelagem dimensional e análise visual de dados públicos de transferências discricionárias e legais da União (Transferegov / SICONV).

---

## 1. Arquitetura e Papel dos Componentes

O Lakehouse adota separação estrita entre computação, armazenamento, orquestração e visualização:

| Componente | Função / Papel no Lakehouse |
| :--- | :--- |
| **MinIO** | Armazenamento de objetos (Object Storage compatível com S3) distribuído em buckets organizados por camada (`bronze`, `silver`, `gold`). |
| **Apache Spark** | Engine de processamento distribuído para ingestão da camada Bronze, validação e execução de scripts de bootstrap de catálogos. |
| **Delta Lake** | Formato de armazenamento colunar aberto com transações ACID, versionamento temporal (*time travel*), enforcement de schema e alta performance de leitura. |
| **Spark Thrift Server / Hive Metastore** | Catálogo de metadados unificado e interface SQL (porta 10000) permitindo que o dbt e o Superset consultem tabelas Delta via PyHive/Thrift. |
| **Apache Airflow** | Orquestrador corporativo responsável pelo agendamento, controle de dependências, ramificação condicional inteligente (*BranchPythonOperator*) e governança *fail-fast*. |
| **dbt (data build tool)** | Motor de modelagem, transformação SQL, testes analíticos automatizados e geração de documentação de dados. |
| **Apache Superset** | Camada de *Business Intelligence* e visualização, servindo dashboards executivos e métricas financeiras sobre o Serving Mart Delta. |

### Fluxo Analítico de Ponta a Ponta

```text
Transferegov (Dados Abertos)
      │
      ▼
 MinIO: s3a://bronze/ (Delta Lake)
      │
      ▼
 MinIO: s3a://silver/ (dbt + Spark)
      │
      ▼
 MinIO: s3a://gold/ (dbt Dimensional)
      │
      ▼
 Serving Mart (mart_superset_proposta_convenio)
      │
      ▼
 Apache Superset (Dashboard Executivo)
```

---

## 2. Estrutura do Armazenamento (MinIO)

Os dados são organizados no MinIO sob o prefixo `s3a://`:

- **`s3a://bronze/`**:
  - `warehouse/siconv_programa/`: tabela Delta Bronze do dataset de programas.
  - `warehouse/siconv_programa_proposta/`: tabela Delta Bronze da associação programa-proposta.
  - `warehouse/siconv_proposta/`: tabela Delta Bronze de propostas.
  - `warehouse/siconv_convenio/`: tabela Delta Bronze de convênios.
  - `warehouse/ingestion_runs/`: tabela Delta de auditoria de execuções, indexada por `ingestion_run_id`.
  - `warehouse/ingestion_manifest/`: manifesto Delta com proveniência, SHA-256, arquivo RAW, contagens e versão Delta por dataset.
  - `raw/transferegov/<dataset>/`: versões dos arquivos ZIP originais baixados da fonte oficial, identificadas por SHA-256 e submetidas à política de retenção.
- **`s3a://silver/`**:
  - `warehouse/`: tabelas Delta limpas, tipadas, padronizadas e deduplicadas (`siconv_convenio`, `siconv_programa_cadastral`, `siconv_programa_elegibilidade`, `siconv_programa_proposta`, `siconv_proposta`).
- **`s3a://gold/`**:
  - `warehouse/`: modelos dimensionais (star schema: dimensões `dim_data`, `dim_municipio`, `dim_orgao`, `dim_programa`, `dim_proponente`; fatos `fct_convenio`, `fct_convenio_saldo_observacao`, `fct_proposta`; ponte `bridge_programa_proposta`).
  - Camada de Serving: tabela física Delta `mart_superset_proposta_convenio` e view semântica `vw_superset_proposta_convenio`.

---

## 3. DAGs do Apache Airflow

O ambiente conta com 3 DAGs governadas:

```text
r6_pipeline_transferegov_e2e (Master Controller)
      ├── [Trigger] r2_ingestao_transferegov_bronze (Ingestão Bronze)
      └── [Decisão] ──► NO_CHANGE ──► end (curto-circuito econômico)
                    └──► SUCCESS   ──► [Trigger] r6_transformacoes_lakehouse (Silver/Gold/Serving) ──► end
```

1. **`r2_ingestao_transferegov_bronze`**:
   - Ingestão dos 4 datasets oficiais do portal Transferegov.
   - Detecção de modificação por hash e timestamp `data_carga_siconv`.
   - Limpeza idempotente e registro de auditoria em `bronze.ingestion_runs`.
   - Leaf task `finalizar_execucao_r2` que preserva o status real de falha (eliminando mascaramento de erros após cleanup).
   - Suporta parâmetro `ingestion_run_id` para rastreabilidade ponta a ponta.

2. **`r6_transformacoes_lakehouse`**:
   - Orquestração sequencial e *fail-fast* pós-Bronze:
     `preflight_dbt >> TaskGroup(silver) >> TaskGroup(gold) >> TaskGroup(serving) >> TaskGroup(documentation) >> end`.
   - Reconciliações analíticas intermediárias (Bronze $\rightarrow$ Silver e Silver $\rightarrow$ Gold) com 0 divergência tolerada.
   - Isolamento de testes dbt por camada para garantir execução independente.

3. **`r6_pipeline_transferegov_e2e`** *(DAG Master Controller)*:
   - Orquestra todo o fluxo analítico de ponta a ponta.
   - Dispara a ingestão R2 repassando parâmetros e aguarda conclusão com fail-fast.
   - Inspeciona o registro de auditoria em `bronze.ingestion_runs` via Spark Thrift Server para decidir o fluxo downstream.
   - Aplica curto-circuito elegante no ramo `no_change` caso o portal oficial não tenha sofrido alterações, evitando processamentos redundantes.
   - Dispara a DAG `r6_transformacoes_lakehouse` apenas quando houver novos dados ou reprocessamento forçado.
   - Unifica os caminhos na tarefa `end` com `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`.

---

## 4. Fluxo de Execução: FULL vs. NO_CHANGE

| Característica | Fluxo FULL (`force_bronze=true` ou novos dados) | Fluxo NO_CHANGE (`force_bronze=false` e sem novos dados) |
| :--- | :--- | :--- |
| **Gatilho Bronze** | Executado via `r2_ingestao_transferegov_bronze`. | Executado via `r2_ingestao_transferegov_bronze`. |
| **Comportamento R2** | Baixa CSVs, valida e escreve nas tabelas Delta Bronze. | Detecta hash idêntico em `controle_inicial`, marca `NO_CHANGE`. |
| **Status em Auditoria** | `SUCCESS` gravado em `bronze.ingestion_runs`. | `NO_CHANGE` gravado em `bronze.ingestion_runs`. |
| **Decisão E2E** | `decidir_pos_bronze` roteia para `trigger_transformacoes`. | `decidir_pos_bronze` roteia para `no_change` (*EmptyOperator*). |
| **DAG Transformações** | `r6_transformacoes_lakehouse` disparada e concluída. | **Pulada (*skipped*)** — zero custo computacional. |
| **Tarefa `end`** | Finaliza como `success`. | Finaliza como `success`. |
| **Resultado Final** | `SUCCESS` (Lakehouse 100% atualizado). | `SUCCESS` (Auditado sem reprocessamento desnecessário). |

### Rastreabilidade via `ingestion_run_id`

O Master Controller correlaciona três identificadores complementares:
- **DagRun pai E2E**: por exemplo, `r6b_full_2`.
- **DagRun filho R2**: `r6b_bronze__<timestamp>`.
- **`ingestion_run_id` persistente**: `e2e_<timestamp>`, usado como chave em `bronze.ingestion_runs` e no manifesto Bronze.
- **DagRun filho R6-A**: `r6b_transform__<timestamp>`.

Essa cadeia permite relacionar logs do Airflow, auditoria Bronze, manifestos, tabelas Delta e transformações downstream sem depender de arquivos temporários.

---

## 5. Como Disparar Manualmente

### 5.1 Execução Ponta a Ponta Completa (FULL)

Para forçar a ingestão da Bronze e executar todas as transformações subsequentes:

```bash
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e \
  --run-id r6b_full_manual_$(date +%Y%m%d_%H%M%S) \
  --conf '{"force_bronze": true}'
```

### 5.2 Execução com Detecção Automática (NO_CHANGE se não houver dados novos)

Para executar o pipeline respeitando o estado da fonte (curto-circuito econômico):

```bash
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e \
  --run-id r6b_check_$(date +%Y%m%d_%H%M%S) \
  --conf '{"force_bronze": false}'
```

### 5.3 Execução Isolada de Transformações (Pós-Bronze)

Caso a camada Bronze já esteja carregada e seja necessário reprocessar apenas Silver, Gold e Serving:

```bash
docker exec airflow airflow dags trigger r6_transformacoes_lakehouse \
  --run-id r6a_manual_$(date +%Y%m%d_%H%M%S)
```

---

## 6. Status do Projeto

- **R1** — Infraestrutura Lakehouse (Docker, MinIO, Spark, Thrift, Airflow, Superset) ✅ *merged*
- **R2** — Ingestão e camada Bronze dos dados oficiais do Transferegov ✅ *merged*
- **R3** — Camada Silver implementada e validada ✅ *merged*
- **R4** — Camada Gold dimensional implementada e validada ✅ *merged*
- **R5** — Serving analítico e dashboard executivo Superset ✅ *merged*
- **R6-A** — Orquestração pós-Bronze (Silver $\rightarrow$ Gold $\rightarrow$ Serving) ✅ *merged*
- **R6-B** — Orquestração Ponta a Ponta (Master Controller E2E) 🚧 *concluído para revisão*
- **R7** — Governança e documentação consolidada ⏳
