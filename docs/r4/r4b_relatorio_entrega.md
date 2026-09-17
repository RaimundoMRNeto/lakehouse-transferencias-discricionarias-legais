# R4-B — Relatório Técnico de Entrega da Camada Gold

## 1. Resumo Executivo da Entrega

A etapa **R4-B (Implementação, Qualidade e Validação da Camada Gold)** materializou formalmente a camada analítica dimensional em **Delta Lake** sobre o storage de objetos MinIO (`s3a://gold/warehouse`), orquestrada via **dbt** (versão 1.10.9) e processada pelo **Apache Spark 3.4.3 / Spark Thrift Server**.

A entrega é estritamente aderente aos contratos analíticos aprovados nas etapas **R4-A** e **R4-A.1**, mantendo integridade matemática exata (tolerância R$ 0,00) em relação à camada Silver, sem qualquer mutação ou impacto nos dados das camadas Silver e Bronze.

### Principais Marcos Alcançados
- **9 Entidades Delta Materializadas**: 5 dimensões confirmadas, 3 tabelas fato e 1 tabela ponte (*bridge*).
- **dbt build: 81/81 checks PASS (100% de sucesso)**: 9 modelos de tabela Delta e 72 testes de dados (57 testes de esquema e 15 testes singulares de integridade e reconciliação).
- **Idempotência Comprovada**: Execução consecutiva de dois ciclos completos de `dbt build` resultando em zero erros e nenhuma alteração nos volumes ou checksums.
- **Reconciliação Dinâmica Exata**: 12 gates automatizados executados via script de reconciliação analítica (`scripts/reconcile_silver_gold.py`), incluindo auditoria temporal exata (`date_mapping_exact`), atingindo R$ 0,00 de divergência financeira e 0 discrepâncias de contagem.
- **Integridade do Repositório**: Toda a suíte de testes unitários legados (60 testes em `tests/`) mantida com 100% de aprovação.

---

## 2. Arquitetura Física e Estrutura de Armazenamento

A camada Gold foi provisionada no Spark Catalog com apontamento explícito para o bucket S3A dedicado:

- **Database**: `gold`
- **Location**: `s3a://gold/warehouse`
- **Formato**: Delta Lake (`USING delta`)
- **Provedor de Metadados**: Hive Metastore / Spark Thrift Server

```mermaid
flowchart TD
    subgraph Silver["Camada Silver (s3a://silver/warehouse)"]
        S_PROP["silver.siconv_proposta<br/>(1.157.619)"]
        S_CONV["silver.siconv_convenio<br/>(287.586)"]
        S_PROG["silver.siconv_programa_cadastral<br/>(53.018)"]
        S_PROG_PROP["silver.siconv_programa_proposta<br/>(1.158.975)"]
    end

    subgraph Gold["Camada Gold (s3a://gold/warehouse)"]
        D_DATA["dim_data<br/>(22.282)"]
        D_MUN["dim_municipio<br/>(5.570)"]
        D_PROPON["dim_proponente<br/>(29.325)"]
        D_ORG["dim_orgao<br/>(184)"]
        D_PROG["dim_programa<br/>(53.018)"]

        F_PROP["fct_proposta<br/>(1.157.619)"]
        F_CONV["fct_convenio<br/>(287.584)"]
        F_SALDO["fct_convenio_saldo_observacao<br/>(287.586)"]
        B_PROG_PROP["bridge_programa_proposta<br/>(1.158.975)"]
    end

    S_PROP --> D_MUN
    S_PROP --> D_PROPON
    S_PROP --> D_ORG
    S_PROG --> D_ORG
    S_PROG --> D_PROG

    S_PROP --> F_PROP
    S_CONV --> F_CONV
    S_CONV --> F_SALDO
    S_PROG_PROP --> B_PROG_PROP

    D_DATA -.-> F_PROP
    D_MUN -.-> F_PROP
    D_PROPON -.-> F_PROP
    D_ORG -.-> F_PROP

    D_DATA -.-> F_CONV
    F_PROP -.-> F_CONV
    F_CONV -.-> F_SALDO

    D_PROG -.-> B_PROG_PROP
    F_PROP -.-> B_PROG_PROP
```

---

## 3. Especificação das Entidades Materializadas

### 3.1. Dimensões Confirmadas

| Entidade | Granularidade / PK | Contagem de Linhas | Descrição e Regras |
| :--- | :--- | :--- | :--- |
| **`dim_data`** | 1 dia civil / `data_sk` (INT) | **22.282** | Calendário contínuo de `1990-01-01` a `2050-12-31` (22.280 dias) + 2 registros sentinela: `-1` (Data Não Informada / NULL) e `-2` (Data Fora da Janela Analítica). Contém atributos como ano, mês, dia, trimestre, semestre, dia da semana, flag de fim de semana e mês civil. |
| **`dim_municipio`** | 1 município IBGE / `codigo_municipio_ibge` (STRING 7 dígitos) | **5.570** | Dimensão cadastral baseada nos proponentes de propostas na Silver (`silver.siconv_proposta`). Contém nome do município e UF. |
| **`dim_proponente`** | 1 proponente / `identificacao_proponente` (STRING) | **29.325** | Dimensão cadastral de proponentes (órgãos municipais, estaduais e entidades privadas sem fins lucrativos) com nome e tipo do proponente. |
| **`dim_orgao`** | 1 órgão / `codigo_orgao` (STRING) | **184** | Dimensão conformada de órgãos federais concedentes e superiores consolidados de `siconv_proposta` e `siconv_programa_cadastral`, com flags `is_orgao_superior` e `is_orgao_concedente`. |
| **`dim_programa`** | 1 programa / `id_programa` (BIGINT) | **53.018** | Cadastro completo de programas do Transferegov a partir de `silver.siconv_programa_cadastral`. |

### 3.2. Fatos e Ponte

| Entidade | Granularidade / PK | Contagem de Linhas | Métricas e Regras |
| :--- | :--- | :--- | :--- |
| **`fct_proposta`** | 1 proposta / `id_proposta` (BIGINT) | **1.157.619** | Fato transacional de propostas. Chaves estrangeiras para `dim_data` (`data_proposta_sk`, `data_inicio_vigencia_proposta_sk`, `data_fim_vigencia_proposta_sk`), `dim_municipio`, `dim_proponente` e `dim_orgao`. Métricas: `valor_global_proposta`, `valor_repasse_proposta`, `valor_contrapartida_proposta` em `DECIMAL(17,2)`. |
| **`fct_convenio`** | 1 convênio canônico / `numero_convenio` (STRING) | **287.584** | Fato analítica canônica consolidada (resolvendo a duplicação dos 2 convênios com 4 linhas na Silver). Chaves estrangeiras para `fct_proposta` e `dim_data`. Métricas financeiras canônicas preservadas (`valor_global_convenio`, `valor_repasse_convenio`, `valor_contrapartida_convenio`, `valor_empenhado_convenio`, `valor_desembolsado_convenio`, etc.). **Exclusão estrita de `valor_saldo_conta`**. |
| **`fct_convenio_saldo_observacao`** | 1 observação física / `id_convenio_observacao` (STRING) | **287.586** | Fato observacional isolada para histórico e auditoria bancária de saldo em conta (`valor_saldo_conta`), com flag `has_source_conflict` para identificar registros com registros divergentes na origem. |
| **`bridge_programa_proposta`** | 1 par / `(id_programa, id_proposta)` | **1.158.975** | Tabela de relacionamento N:N puro entre Programas e Propostas. **Totalmente isenta de medidas financeiras ou rateios artificiais**. |

---

## 4. Decisões Arquiteturais e Guardrails Implementados

### 4.1. Mapeamento de Datas e Sentinelas (`date_to_gold_sk`)
Foi implementada a macro dbt `macros/date_to_gold_sk.sql` para conversão de colunas de data em chaves inteiras no padrão `yyyyMMdd`:
- Se a data for `NULL`: retorna `-1` (`Data Não Informada`).
- Se a data for `< 1990-01-01` ou `> 2050-12-31`: retorna `-2` (`Data Fora da Janela Analítica`).
- Caso contrário: converte para inteiro formatado `yyyyMMdd`.

### 4.2. Isolamento de Saldos Bancários Observacionais
Na camada Silver, foram detectados 2 números de convênio (`949286` e `956078`) com 2 linhas cada, decorrentes de múltiplas observações de saldos de contas correntes divergentes na extração da fonte original (`949286` com saldos R$ 9.915,04 e R$ 9.918,59; `956078` com saldos R$ 3.164.224,05 e R$ 3.165.629,92).
- Em `gold.fct_convenio`, os atributos estáveis foram agregados via `SELECT DISTINCT`, gerando exatamente 287.584 convênios canônicos.
- A métrica `valor_saldo_conta` foi rigorosamente removida de `fct_convenio` para evitar dupla contagem em relatórios gerenciais e financeiros.
- Todas as 287.586 observações físicas foram acomodadas em `gold.fct_convenio_saldo_observacao`, referenciando `numero_convenio`.

### 4.3. Proteção contra Cardinalidade N:N e Rateios Indevidos
A cardinalidade entre Programas e Propostas é comprovadamente N:N (uma proposta pode se vincular a mais de um programa). Para blindar a camada analítica contra erros de agregação (duplicação de valores globais ou desembolsados):
- `gold.bridge_programa_proposta` contém exclusivamente as chaves `id_programa` e `id_proposta`.
- Nenhuma métrica financeira ou coluna de rateio existe na bridge.
- As 3 propostas órfãs de cadastro identificadas no R4-A (`321453`, `1427146`, `296629`) foram mapeadas na macro `known_bridge_orphan_proposals` e validadas no teste singular `no_unexpected_gold_bridge_proposal_orphans.sql`.

---

## 5. Resultados da Reconciliação Dinâmica Silver-Gold

O script oficial `scripts/reconcile_silver_gold.py` realizou a reconciliação analítica completa comparando dinamicamente a camada Silver contra a Gold e contra o baseline histórico aprovado no R4-A. Todos os 12 gates foram aprovados com 100% de conformidade:

| # | Gate de Reconciliação | Silver / Canônico | Gold Materializado | Diferença | Baseline R4-A | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | Location no Storage | `s3a://gold/warehouse` | `s3a://gold/warehouse` | 0 | Conforme | **PASS** |
| **2** | Contagem de Linhas (9 tabelas) | Conforme esperado | Conforme esperado | 0 | 100% match | **PASS** |
| **3** | Unicidade de Chaves Primárias | 100% únicas | 100% únicas | 0 dup | 0 dup | **PASS** |
| **4** | Integridade Referencial | 0 órfãos inesperados | 0 órfãos inesperados | 0 | Conforme | **PASS** |
| **5** | Mapeamento Sentinela de Datas | Válidas / Sentinelas | Sem órfãos em `dim_data` | 0 | Conforme | **PASS** |
| **5.1**| Mapeamento Exato de Datas | 9 colunas temporais | 0 divergências exatas | 0 | Conforme | **PASS** |
| **6** | Conformidade de Órgãos | 184 órgãos distintos | 184 órgãos em `dim_orgao` | 0 | 184 | **PASS** |
| **7** | Estabilidade de Convênios | 287.584 canônicos | 287.584 em `fct_convenio` | 0 | 287.584 | **PASS** |
| **8** | Finanças de Proposta (Global) | R$ 1.495.209.875.334,42 | R$ 1.495.209.875.334,42 | **R$ 0,00** | R$ 1.495.209.875.334,42 (MATCH) | **PASS** |
| **8** | Finanças de Proposta (Repasse) | R$ 1.425.758.135.735,63 | R$ 1.425.758.135.735,63 | **R$ 0,00** | R$ 1.425.758.135.735,63 (MATCH) | **PASS** |
| **8** | Finanças de Proposta (Contrapartida) | R$ 69.451.779.598,79 | R$ 69.451.779.598,79 | **R$ 0,00** | R$ 69.451.779.598,79 (MATCH) | **PASS** |
| **9** | Finanças de Convênio (Global) | R$ 356.840.744.533,16 | R$ 356.840.744.533,16 | **R$ 0,00** | R$ 356.840.744.533,16 (MATCH) | **PASS** |
| **9** | Finanças de Convênio (Repasse) | R$ 331.271.766.468,34 | R$ 331.271.766.468,34 | **R$ 0,00** | R$ 331.271.766.468,34 (MATCH) | **PASS** |
| **9** | Finanças de Convênio (Contrapartida) | R$ 23.709.764.114,58 | R$ 23.709.764.114,58 | **R$ 0,00** | R$ 23.709.764.114,58 (MATCH) | **PASS** |
| **9** | Finanças de Convênio (Empenhado) | R$ 192.064.543.060,79 | R$ 192.064.543.060,79 | **R$ 0,00** | R$ 192.064.543.060,79 (MATCH) | **PASS** |
| **9** | Finanças de Convênio (Desembolsado) | R$ 153.205.012.397,29 | R$ 153.205.012.397,29 | **R$ 0,00** | R$ 153.205.012.397,29 (MATCH) | **PASS** |
| **10** | Fato Observacional de Saldo | 287.586 linhas | 287.586 linhas | 0 | 287.586 | **PASS** |
| **10** | Checksum Saldo Conta (Técnico) | R$ 18.079.072.724,97 | R$ 18.079.072.724,97 | **R$ 0,00** | R$ 18.079.072.724,97 (MATCH) | **PASS** |
| **11** | Guardrail de Métricas na Bridge | 0 colunas de valor | 2 colunas de chave | 0 | Conforme | **PASS** |

---

## 6. Bateria de Testes dbt e Validação de Idempotência

### 6.1. Cobertura do dbt Build
A execução de `dbt build --select path:models/gold` executa exatamente **81 checks (100% de aprovação)**:
- **9 modelos de tabela Delta** (`bridge_programa_proposta`, `dim_data`, `dim_municipio`, `dim_orgao`, `dim_programa`, `dim_proponente`, `fct_convenio`, `fct_convenio_saldo_observacao`, `fct_proposta`).
- **72 data tests**, decompostos em:
  - **57 testes de esquema** (`unique`, `not_null`, `relationships` em `models/gold/schema.yml`).
  - **15 testes singulares** em `dbt_lakehouse/tests/` (integridade estrutural, conformidade de órgãos, estabilidade canônica, integridade da bridge e reconciliação dinâmica de linhas e métricas financeiras).

### 6.2. Prova de Idempotência
Foram executadas duas rodadas completas de `dbt build --select path:models/gold`:
- **Rodada 1**: Criação dos 9 modelos Delta e aprovação dos 72 data tests (`dbt build: 81/81 checks PASS`).
- **Rodada 2**: Reexecução completa sobre os dados materializados com pre-hooks de rebase Delta (`dbt build: 81/81 checks PASS`), sem alteração nos checksums ou estado das tabelas.
- **Catálogo dbt**: `dbt docs generate` gerou `target/catalog.json` e documentação completa com 100% de sucesso.

---

## 7. Integridade do Sistema e Não-Regressão

A execução da suíte completa de testes unitários do repositório (`python -m unittest`) confirmou a integridade das camadas inferiores e ferramentas de infraestrutura:
```text
Ran 60 tests in 0.232s
OK
```
- Validação de compilação de código Python (`compileall`) em `/app`, `/scripts` e `/opt/airflow/dags` concluída sem advertências.
- Verificação de formatação e espaços em branco no Git (`git diff --check`) sem inconformidades.

---

## 8. Limitações Conhecidas e Recomendações

1. **Camadas de Consumo Visual (Superset)**: A camada Gold encontra-se pronta e indexada no catálogo Hive Metastore / Spark SQL para conexão e criação de dashboards. Nenhuma alteração foi realizada em assets do Superset neste marco, conforme diretriz.
2. **Orquestração Contínua (Airflow)**: O bootstrap e os scripts de reconciliação foram validados isoladamente. A criação de DAGs de agendamento periódico faz parte de marcos operacionais subsequentes.
3. **Ausência de SCD Tipo 2**: Conforme estabelecido no R4-A.1, as dimensões cadastrais adotam chaves naturais com grão preservado, sem implementação de SCD Tipo 2 no escopo atual.

---

## 9. R4-B.1 — Fechamento de Rastreabilidade, Baseline e Evidências

Durante a revisão de pré-abertura de Pull Request (R4-B.1), foi realizada uma auditoria independente, rigorosa e de ponta a ponta para fechar a rastreabilidade entre a fonte física, a camada Silver, a camada Gold, o baseline histórico do R4-A e as evidências documentadas.

### 9.1. Identidade e Metadados do Snapshot Silver Auditado
A consulta aos metadados de auditoria das 5 tabelas da camada Silver atesta que o snapshot ativo no momento da materialização da Gold é **rigorosamente o mesmo snapshot utilizado e homologado no R4-A**:

| Tabela Silver | Contagem de Linhas | Run ID de Ingestão | SHA-256 da Fonte | Timestamp de Ingestão (UTC) |
| :--- | :--- | :--- | :--- | :--- |
| `silver.siconv_proposta` | 1.157.619 | `run_20260916T182321` | `a60687d706c4c17b95cf400485a9c48b2ec2ebd1bfd07f9463fca4b40e6d3e27` | 2026-09-16T18:30:39.993116+00:00 |
| `silver.siconv_programa_cadastral` | 53.018 | `run_20260916T182321` | `617c9b75de5aa5c03e97fb90c6f34b175e3dabbd532e715c2889e19b1607f45c` | 2026-09-16T18:25:45.596562+00:00 |
| `silver.siconv_programa_elegibilidade` | 1.257.350 | `run_20260916T182321` | `617c9b75de5aa5c03e97fb90c6f34b175e3dabbd532e715c2889e19b1607f45c` | 2026-09-16T18:25:45.596562+00:00 |
| `silver.siconv_programa_proposta` | 1.158.975 | `run_20260916T182321` | `f8723df70f84c0489f7e3db2302106e8ff0ef7015d36e28e8a33c83923077006` | 2026-09-16T18:27:44.257753+00:00 |
| `silver.siconv_convenio` | 287.586 | `run_20260916T182321` | `4ba907602d1f6eb47399b5911565b53ce849bf34086835ff80744ce7e3d2a6ab` | 2026-09-16T18:33:25.679585+00:00 |

### 9.2. Diagnóstico e Resolução da Divergência Financeira de Propostas
- **Classificação**: **Caso C** (Snapshot idêntico, baseline histórico correto, divergência gerada por erro tipográfico na redação preliminar do relatório R4-B).
- **Evidência Empírica**:
  - Consulta direta a `silver.siconv_proposta`:
    - Valor Global: `R$ 1.495.209.875.334,42`
    - Valor Repasse: `R$ 1.425.758.135.735,63`
    - Valor Contrapartida: `R$ 69.451.779.598,79`
  - Consulta direta a `gold.fct_proposta`:
    - Valor Global: `R$ 1.495.209.875.334,42`
    - Valor Repasse: `R$ 1.425.758.135.735,63`
    - Valor Contrapartida: `R$ 69.451.779.598,79`
  - Diferença Silver ↔ Gold: **R$ 0,00** (tolerância zero estritamente respeitada).
  - Comparação com `HISTORICAL_BASELINE_R4A`: **100% MATCH**. O script `reconcile_silver_gold.py --check-baseline-r4a` aprovou o gate financeiro com status `[PASS] ... Baseline=R$ 1,495,209,875,334.42 (MATCH)`.
  - Os valores preliminares anotados anteriormente (`1.542...`) decorreram exclusivamente de falha de transcrição textual no markdown, tendo sido plenamente corrigidos nesta revisão.

### 9.3. Confirmação dos IDs Reais dos Convênios Conflitantes
A execução de consulta independente sobre `silver.siconv_convenio` agrupando por `numero_convenio` com `HAVING COUNT(*) > 1` confirmou com precisão matemática os dois únicos convênios com duplicidade física de observações:
- **`numero_convenio = '949286'`**: 2 observações físicas, com saldos de R$ 9.915,04 e R$ 9.918,59.
- **`numero_convenio = '956078'`**: 2 observações físicas, com saldos de R$ 3.164.224,05 e R$ 3.165.629,92.
- O gate `convenio_canonical_stability` comprovou que **apenas a métrica `valor_saldo_conta` varia** entre as observações. Todos os demais 27 atributos contratuais, temporais e cadastrais são 100% idênticos, permitindo a consolidação segura dos 287.584 convênios canônicos em `fct_convenio` e o isolamento das 287.586 linhas em `fct_convenio_saldo_observacao`. Os números equivocados citados em versões anteriores (`704406` e `732158`) foram devidamente eliminados da documentação.

### 9.4. Auditoria Dinâmica de Mapeamento de Datas (`date_mapping_exact`)
Foi incorporado ao script oficial `scripts/reconcile_silver_gold.py` o gate de auditoria temporal exata (`date_mapping_exact`), que compara diretamente, linha a linha e sem risco de joins cartesianos:
- `dim_programa.data_disponibilizacao_sk` vs `silver.siconv_programa_cadastral.data_disponibilizacao` (53.018 registros): **0 divergências**.
- `fct_proposta`: `data_proposta_sk`, `data_inicio_vigencia_proposta_sk`, `data_fim_vigencia_proposta_sk` vs `silver.siconv_proposta` (1.157.619 registros): **0 divergências**.
- `fct_convenio`: `data_assinatura_sk`, `data_publicacao_sk`, `data_inicio_vigencia_sk`, `data_fim_vigencia_sk`, `data_limite_prestacao_contas_sk` vs tupla canônica de `silver.siconv_convenio` (287.584 registros): **0 divergências**.
- Resultado global do gate: **PASS (0 divergências em todas as 9 colunas temporais)**.
