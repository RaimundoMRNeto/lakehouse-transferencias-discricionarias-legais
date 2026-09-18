# Lakehouse de Transferências Discricionárias e Legais da União

Projeto de Engenharia de Dados baseado em arquitetura Lakehouse moderna para ingestão, tratamento, modelagem dimensional e análise visual de dados públicos de transferências discricionárias e legais da União (Transferegov / SICONV).

---

## 1. Objetivo

Consolidar uma plataforma analítica robusta, reprodutível e governada sobre os dados abertos de transferências da União, garantindo fidelidade à fonte primária, rastreabilidade criptográfica, modelagem dimensional em conformidade com as regras de negócio e consumo analítico de alta performance.

---

## 2. Arquitetura

O projeto adota uma arquitetura em duas visões complementares:

### 2.1 Arquitetura Lógica
```text
Transferegov (Dados Abertos)
     ↓
RAW (ZIP Imutável + SHA-256)
     ↓
Bronze (Delta Lake, StringType Fiel, Metadados Técnicos)
     ↓
Staging (dbt Ephemeral, Tipagem e Normalização)
     ↓
Silver (Delta Lake, Grãos Padronizados, Desacoplamento Cadastral)
     ↓
Gold Core (Delta Lake, Esquema Estrela: Fatos, Dimensões e Bridge)
     ↓
Semantic View (dbt View, Contrato Lógico para BI)
     ↓
Serving Mart (Delta Lake, Materialização Física de Alta Performance)
     ↓
Apache Superset (Visualização Executiva e BI)
```

### 2.2 Arquitetura Tecnológica
```text
                     Apache Airflow (Orquestração E2E)
                                    │
                                    │ orquestra
                                    ▼
                         Apache Spark + dbt Core
                                    │
                  ┌─────────────────┴─────────────────┐
                  │                                   │
                  ▼                                   ▼
          MinIO Object Storage              Spark Thrift / Hive
        (Armazenamento Delta Lake)          (Catálogo de Metadados)
                  │                                   │
                  └─────────────────┬─────────────────┘
                                    ▼
                             Apache Superset
```

---

## 3. Stack Tecnológica

| Componente | Função Primária | Papel no Lakehouse |
| :--- | :--- | :--- |
| **Apache Airflow** | Orquestração de Pipelines | Controle sequencial, branching condicional inteligente e fail-fast operacional (execução sob demanda, `schedule=None`). |
| **Apache Spark 3.4** | Processamento Distribuído | Processamento distribuído da ingestão Bronze, escrita Delta e suporte às rotinas de reconciliação analítica. |
| **Delta Lake** | Formato Transacional de Tabelas | Transações ACID, versionamento temporal (*time travel*), enforcement de schema e compactação colunar Parquet. |
| **MinIO** | Object Storage S3-Compatível | Repositório físico nos buckets `bronze`, `silver` e `gold`; os ZIPs RAW ficam sob o prefixo `bronze/raw/transferegov/`. |
| **dbt Core (v1.10)** | Transformação e Modelagem | Camadas Staging (ephemeral), Silver, Gold, testes de integridade analítica e compilação do catálogo técnico. |
| **Spark Thrift Server** | Catálogo e Interface SQL | Exposição JDBC/ODBC (porta 10000) e Hive Metastore conectando dbt e Superset ao Delta Lake. |
| **Apache Superset** | Consumo Analítico e BI | Dashboards executivos com métricas oficiais e tempos de resposta sub-segundo via Serving Mart. |

---

## 4. Pipeline e Linhagem Ponta a Ponta

A rastreabilidade é mantida em duas granularidades complementares:
- **Origem**: os ZIPs oficiais recebem SHA-256 e são armazenados em `s3://bronze/raw/transferegov/<dataset>/...`; o manifesto registra arquivo, hash, contagens e versão Delta.
- **Linhagem por linha**: `__ingestion_run_id`, `__source_file` e `__source_sha256` são preservados na Bronze e na Silver; `fct_convenio_saldo_observacao` também preserva esses metadados por representar a observação física.
- **Linhagem de modelo/execução**: Gold canônica, Semantic e Serving são rastreadas pelos `ref()` do dbt, quality gates e correlação entre DagRuns e `ingestion_run_id`; essas tabelas não carregam uniformemente os metadados técnicos por linha.
- **Identificador de correlação**: `ingestion_run_id` é um identificador determinístico e seguro, como `e2e_<timestamp>`, persistido em `bronze.ingestion_runs` e `bronze.ingestion_manifest`.
- **Detecção de Alteração (`NO_CHANGE`)**: a DAG controladora consulta o estado persistido da ingestão após a verificação de `data_carga_siconv`; se a fonte estiver inalterada, o pipeline encerra sem reconstruir as camadas analíticas.

---

## 5. Camadas de Dados

1. **RAW**: Pacotes ZIP originais compactados, com retenção das 2 versões mais recentes (`raw_versions_per_dataset = 2`).
2. **Bronze**: Tabelas Delta brutas com todos os campos oficiais preservados como `StringType`, adicionados de colunas técnicas de proveniência.
3. **Staging**: Modelos ephemerais no dbt para conversão controlada de tipos (`try_cast`), parsing de datas brasileiras (`parse_date_br`) e valores monetários para `DECIMAL(17,2)`.
4. **Silver**: Tabelas Delta padronizadas e desacopladas (`siconv_proposta`, `siconv_programa_cadastral`, `siconv_programa_elegibilidade`, `siconv_programa_proposta`, `siconv_convenio`).
5. **Gold Core**: Esquema dimensional em estrela com dimensões conformadas (`dim_data`, `dim_proponente`, `dim_municipio`, `dim_orgao`, `dim_programa`), tabelas fato canônicas (`fct_proposta`, `fct_convenio`), tabela fato observacional de saldos (`fct_convenio_saldo_observacao`) e tabela ponte N:N (`bridge_programa_proposta`).
6. **Semantic View**: View dbt (`vw_superset_proposta_convenio`) preservando propostas sem convênio via LEFT JOIN e blindando regras de aditividade.
7. **Serving Mart**: Tabela física Delta (`mart_superset_proposta_convenio`) materializada para reduzir o custo de joins em tempo de consulta e mitigar concorrência no BI.

---

## 6. Orquestração no Apache Airflow

O ambiente opera com 3 DAGs integradas com governança fail-fast:
- **`r6_pipeline_transferegov_e2e`**: DAG Master Controller que gerencia o fluxo global, inspeciona o log de auditoria da ingestão e decide entre o ramo `no_change` ou o disparo da transformação downstream.
- **`r2_ingestao_transferegov_bronze`**: Ingestão física, validação de ZIPs, escrita Delta Bronze e registro em `bronze.ingestion_runs`.
- **`r6_transformacoes_lakehouse`**: Execução sequencial pós-Bronze com checkpoints intermediários de reconciliação analítica: `Bronze → Silver` (`reconcile_bronze_silver.py`) e `Silver → Gold` (`reconcile_silver_gold.py`).

> [!NOTE]
> O deployment atual opera estritamente **sob demanda (`schedule=None`)**. O Airflow provê capacidade de agendamento, mas não há execução cron automática ativa no escopo acadêmico deste projeto.

---

## 7. Governança, Contratos e Qualidade

O projeto conta com um módulo formal de governança documentado em [docs/governanca/README.md](docs/governanca/README.md), abrangendo:
- **Contratos de Grão e Aditividade**: A relação entre Programa e Proposta é estritamente N:N; a `bridge_programa_proposta` **não é caminho aditivo** para valores financeiros (somente contagem distinta). A coluna `valor_saldo_conta` é uma **medida observacional não aditiva** e está isolada fora do Serving Mart.
- **Classificação e Minimização de Dados**: Classificação dos atributos em 4 níveis (Público Oficial, Público com Identificador Pessoal Potencial, Metadado Técnico e Dado Derivado Analítico). O atributo `identificacao_proponente` pode conter CPFs de pessoas físicas, exigindo diretrizes de minimização e não reprodução em documentações abertas.
- **Matriz de Qualidade**: 12 dimensões ativas de controle (integridade criptográfica SHA-256, completude, unicidade, validade, integridade referencial, reconciliação relacional, rastreabilidade e consistência financeira).
- **Catálogo dbt**: 100% de cobertura documental de modelos e fontes Bronze, livre de contagens voláteis de snapshots históricos.

---

## 8. Dashboard e Visualização (Superset)

O Apache Superset consome diretamente o Serving Mart Delta através do Spark Thrift Server (porta 10000):
- **Painel Executivo**: Visão consolidada de transferências discricionárias e legais da União.
- **Indicadores Chave**: Total de propostas submetidas, propostas conveniadas, valores globais, repasses pactuados, valores empenhados e desembolsados.
- **Filtros Interativos**: Segmentação por ano, Unidade da Federação (UF), órgão superior e modalidade do instrumento.

---

## 9. Como Executar e Reproduzir

### 9.1 Inicializar a Stack
```bash
docker compose up -d
docker compose ps
```

### 9.2 Endpoints Locais
- **Apache Airflow**: `http://localhost:8080`
- **Apache Superset**: `http://localhost:8088`
- **MinIO Console**: `http://localhost:9001`
- **Spark Master UI**: `http://localhost:8081`
- **Spark Thrift Server**: `localhost:10000`
- **dbt Docs**: `http://localhost:8091` (quando ativado)

### 9.3 Disparar o Pipeline E2E
```bash
# Execução padrão com detecção de alteração (NO_CHANGE se não houver dados novos):
docker exec airflow airflow dags trigger r6_pipeline_transferegov_e2e

# Execução forçando reprocessamento integral (FULL):
docker exec airflow airflow dags trigger -c '{"force_bronze": true}' r6_pipeline_transferegov_e2e
```

### 9.4 Gerar e Servir o Catálogo dbt Docs
```bash
# Geração dos metadados do catálogo:
docker exec airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs generate --no-partial-parse --profiles-dir ."

# Servir temporariamente na porta 8091:
docker exec -d airflow bash -lc "cd /home/airflow/dbt_lakehouse && dbt docs serve --host 0.0.0.0 --port 8091 --profiles-dir ."
```
Acesse em: `http://localhost:8091`.

### 9.5 Parar os Serviços
```bash
docker compose down
```
> [!CAUTION]
> Não utilize `docker compose down -v` sob risco de exclusão permanente dos dados do MinIO e dos bancos de metadados.

---

## 10. Documentação Completa

Para aprofundamento técnico, consulte os relatórios estruturados:
- [Módulo de Governança de Dados](docs/governanca/README.md) (R7)
  - [Políticas de Governança](docs/governanca/governanca_dados.md)
  - [Catálogo e Contratos de Dados](docs/governanca/catalogo_contratos.md)
  - [Matriz de Qualidade e Linhagem](docs/governanca/qualidade_linhagem.md)
  - [Glossário de Termos](docs/governanca/glossario.md)
  - [Guia de Operação e Reprodutibilidade](docs/governanca/operacao_reprodutibilidade.md)
  - [Relatório de Entrega R7](docs/governanca/r7_relatorio_entrega.md)
- [Relatório de Ingestão e Camada Bronze](docs/r2_relatorio_validacao.md) (R2)
- [Relatórios da Camada Silver](docs/r3/) (R3)
- [Relatórios da Camada Gold Dimensional](docs/r4/) (R4)
- [Relatórios da Camada Serving e Superset](docs/r5/) (R5)
- [Relatórios da Orquestração E2E e Transformações](docs/r6/) (R6-A e R6-B)

---

## 11. Status do Projeto

- **R1** — Infraestrutura Lakehouse (Docker, MinIO, Spark, Thrift, Airflow, Superset) ✅ *merged*
- **R2** — Ingestão e Camada Bronze dos Dados Oficiais do Transferegov ✅ *merged*
- **R3** — Camada Silver Implementada e Validada ✅ *merged*
- **R4** — Camada Gold Dimensional Implementada e Validada ✅ *merged*
- **R5** — Serving Analítico e Dashboard Executivo no Superset ✅ *merged*
- **R6-A** — Orquestração Pós-Bronze (Silver $\rightarrow$ Gold $\rightarrow$ Serving) ✅ *merged*
- **R6-B** — Orquestração Ponta a Ponta (Master Controller E2E) ✅ *merged*
- **R7** — Governança, catálogo, linhagem e reprodutibilidade ✅ *concluído e aprovado para PR*
- **R8** — Auditoria / entrega final ⏳
