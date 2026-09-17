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

## Camadas

- Bronze — dados brutos provenientes das fontes oficiais
- Silver — dados tratados, tipados e validados
- Gold — modelos analíticos destinados ao consumo

## Status do projeto

- **R1** — Configuração da infraestrutura Lakehouse (concluído).
- **R2** — Ingestão e camada Bronze dos dados oficiais do Transferegov (concluído).
- **R3** — Camada Silver implementada na branch e aguardando revisão humana (modelagem relacional de grão preservado, 5 entidades Delta, reconciliação financeira DECIMAL exata e 42 testes dbt aprovados).