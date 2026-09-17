# Lakehouse de Transferências Discricionárias e Legais

Projeto de Engenharia de Dados baseado em arquitetura Lakehouse para ingestão,
tratamento, modelagem e análise de dados públicos de transferências discricionárias
e legais.

## Arquitetura

- MinIO — armazenamento de objetos
- Apache Spark — processamento de dados
- Delta Lake — formato de armazenamento
- Spark Thrift / Hive — catálogo e interface SQL
- Apache Airflow — orquestração
- dbt — transformação e testes
- Apache Superset — visualização

### Fluxo Analítico de Ponta a Ponta

```text
Transferegov
     ↓
Bronze
     ↓
Silver
     ↓
Gold
     ↓
vw_superset_proposta_convenio
     ↓
Apache Superset
     ↓
Dashboard Executivo
```

## Camadas

- Bronze — dados brutos provenientes das fontes oficiais
- Silver — dados tratados, tipados e validados
- Gold — modelos analíticos destinados ao consumo

## Status do projeto

- **R1** — Configuração da infraestrutura Lakehouse (concluído / merged).
- **R2** — Ingestão e camada Bronze dos dados oficiais do Transferegov (concluído / merged).
- **R3** — Camada Silver implementada e validada (concluído / merged).
- **R4** — Camada Gold dimensional implementada e validada (concluído / merged).
- **R5** — Serving analítico e dashboard executivo Superset (concluído / merged).
- **R6-A** — Orquestração pós-Bronze implementada na branch e aguardando revisão humana.
