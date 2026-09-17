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

- **R1** — Configuração da infraestrutura Lakehouse (concluído / merged).
- **R2** — Ingestão e camada Bronze dos dados oficiais do Transferegov (concluído / merged).
- **R3** — Camada Silver implementada e validada (concluído / merged).
- **R4-A / R4-A.1** — Descoberta analítica, modelagem dimensional e contratos aprovados (concluído).
- **R4-B** — Camada Gold materializada em Delta Lake via dbt + Spark (5 dimensões confirmadas, 3 fatos, 1 bridge, 11 gates de reconciliação dinâmica aprovados com R$ 0,00 de divergência, 81 testes dbt e idempotência comprovada) — aguardando revisão humana na branch `feat/r4b-camada-gold`.

