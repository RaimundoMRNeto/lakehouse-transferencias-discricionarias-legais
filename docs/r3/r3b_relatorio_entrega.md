# Relatório de Entrega — R3-B: Implementação, Qualidade e Validação da Camada Silver

**Data:** 17/09/2026
**Branch:** `feat/r3-silver-qualidade-modelagem`
**Status do Marco:** `R3-B APROVADO PARA REVISÃO HUMANA`
**Escopo:** Camada Silver com dbt + Spark 3.4.3 / Delta Lake no MinIO.

---

## 1. Arquitetura da Solução

A implementação da camada Silver seguiu a arquitetura em três camadas fundamentais com dbt sobre Apache Spark / Thrift Server e Delta Lake:

```
+-----------------------------------------------------------------------------------+
| CAMADA BRONZE (Delta Lake - S3A: s3a://bronze/warehouse)                          |
|  - bronze.siconv_proposta (1.157.619 linhas)                                      |
|  - bronze.siconv_programa_proposta (1.158.975 linhas)                             |
|  - bronze.siconv_programa (1.257.350 linhas)                                      |
|  - bronze.siconv_convenio (287.586 linhas)                                        |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| CAMADA STAGING (dbt - Materialização: ephemeral)                                 |
|  - stg_siconv_proposta                                                            |
|  - stg_siconv_programa_proposta                                                   |
|  - stg_siconv_programa                                                            |
|  - stg_siconv_convenio                                                            |
|  (Limpeza, tipagem semântica, cálculo de chaves SHA-256 e flags de auditoria)      |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
| CAMADA SILVER (Delta Lake - S3A: s3a://silver/warehouse)                          |
|  - silver.siconv_proposta (1.157.619 linhas)                                      |
|  - silver.siconv_programa_cadastral (53.018 linhas)                               |
|  - silver.siconv_programa_elegibilidade (1.257.350 linhas)                        |
|  - silver.siconv_programa_proposta (1.158.975 linhas)                             |
|  - silver.siconv_convenio (287.586 linhas)                                        |
+-----------------------------------------------------------------------------------+
```

### Princípios Inegociáveis Respeitados:
- **Separação Estrita de Grãos:** Nenhum join explosivo desnormalizado (ex.: Programa $\times$ Proposta $\times$ Convênio que geraria 24,24 milhões de linhas e inflaria artificialmente de R$ 356,85 bilhões para R$ 25,96 trilhões) foi criado na Silver. A camada Silver preserva as entidades relacionais em seu grão real.
- **Formato Delta Lake:** Todas as 5 tabelas foram materializadas como tabelas Delta Lake com catálogo Spark/Hive em `s3a://silver/warehouse/<tabela>`.

---

## 2. Entidades Físicas Criadas, Grãos e Chaves

| Entidade Física Silver | Grão da Entidade | Chave Primária / Identidade | Chaves de Negócio / Estrangeiras | Volume Físico |
| :--- | :--- | :--- | :--- | :--- |
| `silver.siconv_proposta` | 1 linha por proposta de trabalho | `id_proposta` (BIGINT, única, not null) | `numero_proposta` (STRING, não única) | 1.157.619 |
| `silver.siconv_programa_cadastral` | 1 linha por programa governamental | `id_programa` (BIGINT, única, not null) | `codigo_programa`, `nome_programa` | 53.018 |
| `silver.siconv_programa_elegibilidade` | 1 linha por observação de elegibilidade | `id_programa_elegibilidade` (SHA-256, única, not null) | FK `id_programa` $\rightarrow$ `programa_cadastral` | 1.257.350 |
| `silver.siconv_programa_proposta` | 1 vínculo associativo Programa $\times$ Proposta | Par `(id_programa, id_proposta)` (único, not null) | FKs para `programa_cadastral` e `proposta` | 1.158.975 |
| `silver.siconv_convenio` | 1 observação do instrumento no snapshot oficial | `id_convenio_observacao` (SHA-256, única, not null) | `numero_convenio` (não único), FK `id_proposta` | 287.586 |

---

## 3. Chaves Técnicas e Canonicalização Determinística

As chaves substitutas (*surrogate keys*) técnicas foram criadas com a macro customizada `sha256_surrogate_key`:
- **Algoritmo:** SHA-256 (uso de MD5 estritamente vedado).
- **Canonicalização por Comprimento:** Para prevenir colisões causadas por delimitadores ou ambiguidade entre `NULL` e string vazia `''`, cada campo é transformado deterministicamente:
  - `NULL` $\rightarrow$ `'-1:'`
  - `''` $\rightarrow$ `'0:'`
  - `'ABC'` $\rightarrow$ `'3:ABC'`
- **`id_programa_elegibilidade`:** Calculado sobre a superchave de 5 atributos: `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA, ACAO_ORCAMENTARIA)` sobre os valores brutos da fonte.
- **`id_convenio_observacao`:** Calculado deterministicamente sobre **todas as 40 colunas oficiais** da fonte `siconv_convenio`, garantindo unicidade perfeita mesmo para os dois convênios conflitantes que compartilham o mesmo `numero_convenio`.

---

## 4. Tratamento das Transformações e Tipagem Semântica

1. **Strings e Sentinelas:**
   - Padronização via macro `clean_string`: `NULLIF(TRIM(col), '')`.
   - Limpeza de sentinelas textuais conhecidas:
     - `OBJETO_PROPOSTA = '-'` $\rightarrow$ `NULL`.
     - `CD_AGENCIA = '-'` $\rightarrow$ `NULL`.
     - `NM_BANCO = 'NÃO INFORMADO'` preservado como categoria textual.
     - `ENVIADA_MANDATARIA = 'NÃO APLICÁVEL'` preservado como categoria de negócio.
   - Preservação de zeros à esquerda como `STRING`: `codigo_municipio_ibge`, `codigo_orgao_superior`, `identificacao_proponente`, `cep_proponente`, `codigo_agencia`, `codigo_conta`, `unidade_gestora_emitente`.
2. **Datas:**
   - Padronização via macro `parse_date_br`: `TO_DATE(NULLIF(TRIM(col), ''), 'dd/MM/yyyy')`.
   - Datas com anos fora do intervalo comum (ex.: `0001`, `0006`, `0011`, `3012`, `5008`) foram preservadas no tipo `DATE` sem descarte arbitrário (regra candidata), suportadas pela configuração `datetimeRebaseModeInWrite=CORRECTED`.
3. **Valores Monetários:**
   - Padronização via macro `parse_decimal_br`: `CAST(REPLACE(NULLIF(TRIM(col), ''), ',', '.') AS DECIMAL(17,2))`.
   - Proibição absoluta de tipos `FLOAT` ou `DOUBLE`.
   - Todas as métricas de reconciliação agregadas calculadas estritamente em `DECIMAL(38,2)`.
4. **Booleanos:**
   - Conversão segura via macro `parse_boolean_sim_nao`:
     ```sql
     CASE
       WHEN UPPER(TRIM(col)) = 'SIM' THEN TRUE
       WHEN UPPER(TRIM(col)) = 'NÃO' THEN FALSE
       ELSE NULL
     END
     ```
   - Proibição de `ELSE FALSE` arbitrário.

---

## 5. Qualidade, Conflitos de Convênio e Auditoria de Orfandade

### 5.1. Conflitos em `siconv_convenio`
- A investigação confirmou a presença de 2 chaves de negócio (`949286` e `956078`) com 2 linhas cada, idênticas em run, SHA-256 e datas oficiais, divergindo unicamente em `valor_saldo_conta`.
- **Tratamento Não Destrutivo:** Nenhuma linha foi descartada. Todas as **287.586 linhas** foram preservadas.
- **Sinalização Genérica:** Foram adicionadas as colunas:
  - `source_conflict_count = COUNT(*) OVER (PARTITION BY numero_convenio)`
  - `has_source_conflict = (source_conflict_count > 1)`
- **Auditoria no Snapshot Atual:**
  - 287.582 observações sem conflito (`has_source_conflict = false`, `count = 1`).
  - 4 observações com conflito de fonte (`has_source_conflict = true`, `count = 2`).
  - 2 convênios conflitantes (`949286` e `956078`).

### 5.2. Auditoria de Orfandade e Integridade Referencial
- `siconv_convenio` $\rightarrow$ `siconv_proposta`: **100% de integridade referencial** (0 órfãos em 287.586 linhas).
- `siconv_programa_elegibilidade` $\rightarrow$ `siconv_programa_cadastral`: **100% de integridade referencial** (0 órfãos em 1.257.350 linhas).
- `siconv_programa_proposta` $\rightarrow$ `siconv_programa_cadastral`: **100% de integridade referencial** (0 órfãos em 1.158.975 linhas).
- `siconv_programa_proposta` $\rightarrow$ `siconv_proposta`: Presença exclusiva dos **3 órfãos históricos conhecidos** (`321453`, `1427146`, `296629`), com **0 órfãos inesperados**.
- **Propostas Sem Programa:** Confirmadas as **78 propostas legítimas** da base que não possuem vínculo na tabela ponte, preservadas integralmente.

---

## 6. Reconciliação Dinâmica e Exata (Bronze vs Silver)

Todas as contagens e agregações foram validadas pelo script distribuído `scripts/reconcile_bronze_silver.py`:

### 6.1. Contagem de Linhas

| Entidade | Contagem Bronze | Contagem Silver | Diferença | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Proposta** | 1.157.619 | 1.157.619 | 0 | **PASS** |
| **Ponte Programa-Proposta** | 1.158.975 | 1.158.975 | 0 | **PASS** |
| **Programa Elegibilidade** | 1.257.350 | 1.257.350 | 0 | **PASS** |
| **Programa Cadastral** | 53.018 (distintos) | 53.018 | 0 | **PASS** |
| **Convênio** | 287.586 | 287.586 | 0 | **PASS** |

### 6.2. Reconciliação Financeira em `DECIMAL(38, 2)`

| Métrica / Coluna Financeira | Bronze (R$) | Silver (R$) | Divergência | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Convênio — Valor Global** | R$ 356.856.636.504,87 | R$ 356.856.636.504,87 | **R$ 0,00** | **PASS** |
| **Convênio — Valor Repasse** | R$ 331.287.339.337,88 | R$ 331.287.339.337,88 | **R$ 0,00** | **PASS** |
| **Convênio — Valor Contrapartida**| R$ 23.710.083.216,75 | R$ 23.710.083.216,75 | **R$ 0,00** | **PASS** |
| **Convênio — Valor Empenhado** | R$ 192.071.916.388,96 | R$ 192.071.916.388,96 | **R$ 0,00** | **PASS** |
| **Convênio — Valor Desembolsado**| R$ 153.212.385.725,46 | R$ 153.212.385.725,46 | **R$ 0,00** | **PASS** |
| **Proposta — Valor Global** | R$ 1.495.209.875.334,42 | R$ 1.495.209.875.334,42 | **R$ 0,00** | **PASS** |
| **Proposta — Valor Repasse** | R$ 1.425.758.135.735,63 | R$ 1.425.758.135.735,63 | **R$ 0,00** | **PASS** |
| **Proposta — Valor Contrapartida**| R$ 69.451.779.598,79 | R$ 69.451.779.598,79 | **R$ 0,00** | **PASS** |

---

## 7. Testes Automatizados e Idempotência

- **Suíte de Testes dbt:** 42 testes executados (testes genéricos de `not_null`, `unique`, `accepted_values`, `relationships` e 10 testes singulares de integridade, contagem e reconciliação financeira).
  - Resultado: **42 PASS / 0 WARN / 0 ERROR / 0 SKIP**.
- **Teste de Idempotência:**
  - A execução de `dbt build` foi realizada duas vezes consecutivas sobre a mesma base Bronze.
  - Ambas as execuções resultaram em `47 of 47 PASS` (5 modelos + 42 testes).
  - A execução posterior do script de reconciliação confirmou zero duplicações, zero variações financeiras e hashes perfeitamente determinísticos.
- **Performance de Materialização:**
  - `siconv_convenio`: 20,50 s
  - `siconv_programa_cadastral`: 7,13 s
  - `siconv_programa_elegibilidade`: 16,37 s
  - `siconv_programa_proposta`: 6,09 s
  - `siconv_proposta`: 33,51 s
  - **Tempo Total dos Modelos Silver:** 1 min 24 s.

---

## 8. Não-Regressão dos Marcos R1 e R2

- **Infraestrutura Docker:** 9 serviços operacionais e saudáveis (`airflow`, `airflow-db`, `mc`, `minio`, `spark-master`, `spark-thrift-server`, `spark-worker-1`, `spark-worker-2`, `superset`).
- **Conectividade dbt:** `dbt debug` com 100% de sucesso (*All checks passed!*).
- **Testes Unitários:** **56 de 56 testes unitários** do módulo de ingestão Bronze aprovados com sucesso no Spark (`OK in 128.3s`).
- **Integridade da Camada Bronze:** Tabelas intactas na versão 0 (`siconv_programa`: 1.257.350, `siconv_programa_proposta`: 1.158.975, `siconv_proposta`: 1.157.619, `siconv_convenio`: 287.586, `ingestion_runs`: 4, `ingestion_manifest`: 12).

---

## 9. Limitações e Escopo Reservado para a Camada Gold

- Modelagens multidimensionais (fatos de execução financeira e dimensões conformadas).
- Métricas agregadas por município, estado, ministério e ano fiscal.
- Políticas analíticas para tratamento do saldo nos dois convênios conflitantes em relatórios de fechamento contábil.
- Painéis e dashboards no Apache Superset.
