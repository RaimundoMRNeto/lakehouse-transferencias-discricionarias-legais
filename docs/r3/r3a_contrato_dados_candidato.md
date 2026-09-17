# R3-A — Contrato de Dados Candidato para a Camada Silver

**Versão:** 1.0.0-draft
**Data:** 2026-09-16
**Status dos Contratos:** CANDIDATO / SUJEITO À REVISÃO HUMANA
**Base:** Evidências empíricas levantadas no snapshot ativo `16/09/2026 06:32:11` (`run_20260916T185220`).

---

## 1. Princípios Gerais de Modelagem da Camada Silver

A camada Silver do Lakehouse Transferegov tem por finalidade transformar dados textuais brutos e desestruturados da camada Bronze em entidades tabulares **tipadas, padronizadas, limpas e com integridade verificável**, sem contudo realizar desnormalizações destrutivas ou antecipar agregações da camada Gold.

### Princípios Inegociáveis:
1. **Preservação de Entidades:** Não achatar as tabelas em um tabelão único. A relação N:N entre Programas e Propostas e a relação 1:0..1 entre Propostas e Convênios devem ser mantidas como entidades relacionais distintas.
2. **Tipagem Semântica Rígida:** Nomes em `snake_case`, datas no tipo `DATE`, valores monetários em `DECIMAL(17, 2)`, contadores e anos em `SMALLINT` ou `INT`, e indicadores binários em `BOOLEAN`.
3. **Preservação de Identificadores:** Identificadores com zeros à esquerda (CNPJ/CPF, CEP, Agência, Conta, IBGE) **permanecem obrigatoriamente como `STRING`**.
4. **Deduplicação Responsável:** Nenhuma linha será descartada com `DISTINCT` arbitrário. Deduplicações devem ter justificativa empírica e chave de desempate documentada.

---

## 2. Estratégia de Tratamento de NULL, Vazios e Sentinelas

Com base no profiling empírico do snapshot:
- **Strings Vazias (`''`) e Espaços:** Devem ser convertidos para `NULL` via `NULLIF(TRIM(col), '')`.
- **Sentinelas Conhecidas:**
  - `'-'` em colunas cadastrais ou de descrição (`OBJETO_PROPOSTA`, `CD_AGENCIA`): converter para `NULL`.
  - `'NÃO INFORMADO'` em `NM_BANCO`: manter como string categórica descritiva ou converter para `NULL` conforme diretriz de negócio (Status: CANDIDATO).
  - `'NÃO APLICÁVEL'` em `ENVIADA_MANDATARIA`: manter como categoria válida de processo, pois indica formalmente que o instrumento não passa por mandatária (Caixa Econômica).
- **Datas Aberrantes / Impossíveis:**
  - Datas com anos fora do intervalo histórico aceitável (ex: `< 1990` ou `> 2050`, como `0001-01-01` ou `5008-12-15`) devem ser preservadas ou convertidas para `NULL` mediante regra de validação no R3-B.
- **Formatação Decimal Segura:**
  - Substituição idempotente de vírgula por ponto: `CAST(REPLACE(col, ',', '.') AS DECIMAL(17, 2))`.

---

## 3. Contratos de Dados Candidatos por Entidade

---

### 3.1. Entidade: `silver.siconv_proposta` (ou `silver_proposta`)

- **Fonte Bronze:** `bronze.siconv_proposta`
- **Grão:** 1 linha por Proposta de Trabalho submetida.
- **Chave Primária Lógica:** `id_proposta` (100% ÚNICA e NOT NULL comprovada).
- **Volume Esperado:** 1.157.619 linhas (idêntico à Bronze).
- **Estratégia de Deduplicação:** **NÃO DEDUPLICAR** (zero duplicatas observadas).

| Coluna Bronze | Coluna Silver Candidata | Tipo Bronze | Tipo Silver Candidato | Nullable | Regra de Transformação | Regra de Qualidade | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ID_PROPOSTA` | `id_proposta` | `string` | `BIGINT` | NÃO | `CAST(ID_PROPOSTA AS BIGINT)` | `not_null`, `unique` | **CONFIRMADA** |
| `UF_PROPONENTE` | `uf_proponente` | `string` | `STRING` | NÃO | `UPPER(TRIM(UF_PROPONENTE))` | `accepted_values` (27 UFs) | **CONFIRMADA** |
| `MUNIC_PROPONENTE` | `municipio_proponente` | `string` | `STRING` | NÃO | `TRIM(MUNIC_PROPONENTE)` | `not_null` | **CONFIRMADA** |
| `COD_MUNIC_IBGE` | `codigo_municipio_ibge` | `string` | `STRING` | NÃO | `TRIM(COD_MUNIC_IBGE)` | `length = 7` | **CONFIRMADA** |
| `COD_ORGAO_SUP` | `codigo_orgao_superior` | `string` | `STRING` | NÃO | `TRIM(COD_ORGAO_SUP)` | `length = 5` | **CONFIRMADA** |
| `DESC_ORGAO_SUP` | `descricao_orgao_superior` | `string` | `STRING` | NÃO | `TRIM(DESC_ORGAO_SUP)` | `not_null` | **CONFIRMADA** |
| `NATUREZA_JURIDICA` | `natureza_juridica` | `string` | `STRING` | NÃO | `TRIM(NATUREZA_JURIDICA)` | `not_null` | **CONFIRMADA** |
| `NR_PROPOSTA` | `numero_proposta` | `string` | `STRING` | NÃO | `TRIM(NR_PROPOSTA)` | `not_null` (NÃO é única) | **CONFIRMADA** |
| `DIA_PROP` | `dia_proposta` | `string` | `SMALLINT` | NÃO | `CAST(DIA_PROP AS SMALLINT)` | `values BETWEEN 1 AND 31` | **CONFIRMADA** |
| `MES_PROP` | `mes_proposta` | `string` | `SMALLINT` | NÃO | `CAST(MES_PROP AS SMALLINT)` | `values BETWEEN 1 AND 12` | **CONFIRMADA** |
| `ANO_PROP` | `ano_proposta` | `string` | `SMALLINT` | NÃO | `CAST(ANO_PROP AS SMALLINT)` | `values BETWEEN 2008 AND 2030` | **CONFIRMADA** |
| `DIA_PROPOSTA` | `data_proposta` | `string` | `DATE` | NÃO | `TO_DATE(DIA_PROPOSTA, 'dd/MM/yyyy')` | `not_null` | **CONFIRMADA** |
| `COD_ORGAO` | `codigo_orgao` | `string` | `STRING` | NÃO | `TRIM(COD_ORGAO)` | `length = 5` | **CONFIRMADA** |
| `DESC_ORGAO` | `descricao_orgao` | `string` | `STRING` | NÃO | `TRIM(DESC_ORGAO)` | `not_null` | **CONFIRMADA** |
| `MODALIDADE` | `modalidade` | `string` | `STRING` | NÃO | `TRIM(MODALIDADE)` | `not_null` | **CONFIRMADA** |
| `IDENTIF_PROPONENTE` | `identificacao_proponente` | `string` | `STRING` | NÃO | `TRIM(IDENTIF_PROPONENTE)` | Preservar zeros à esquerda (`length = 14`) | **CONFIRMADA** |
| `NM_PROPONENTE` | `nome_proponente` | `string` | `STRING` | NÃO | `TRIM(NM_PROPONENTE)` | `not_null` | **CONFIRMADA** |
| `CEP_PROPONENTE` | `cep_proponente` | `string` | `STRING` | NÃO | `TRIM(CEP_PROPONENTE)` | Preservar zeros à esquerda (`length = 8`) | **CONFIRMADA** |
| `ENDERECO_PROPONENTE` | `endereco_proponente` | `string` | `STRING` | NÃO | `TRIM(ENDERECO_PROPONENTE)` | `not_null` | **CONFIRMADA** |
| `BAIRRO_PROPONENTE` | `bairro_proponente` | `string` | `STRING` | NÃO | `TRIM(BAIRRO_PROPONENTE)` | `not_null` | **CONFIRMADA** |
| `NM_BANCO` | `nome_banco` | `string` | `STRING` | NÃO | `TRIM(NM_BANCO)` | `not_null` | **CONFIRMADA** |
| `SITUACAO_CONTA` | `situacao_conta` | `string` | `STRING` | NÃO | `TRIM(SITUACAO_CONTA)` | `not_null` | **CONFIRMADA** |
| `SITUACAO_PROJETO_BASICO` | `situacao_projeto_basico` | `string` | `STRING` | SIM | `NULLIF(TRIM(SITUACAO_PROJETO_BASICO), '')` | Taxa nulos esperada: ~13,8% | **CONFIRMADA** |
| `SIT_PROPOSTA` | `situacao_proposta` | `string` | `STRING` | SIM | `NULLIF(TRIM(SIT_PROPOSTA), '')` | Taxa nulos esperada: 0,025% | **CONFIRMADA** |
| `DIA_INIC_VIGENCIA_PROPOSTA`| `data_inicio_vigencia_proposta` | `string` | `DATE` | SIM | `TO_DATE(DIA_INIC_VIGENCIA_PROPOSTA, 'dd/MM/yyyy')` | Tratar anos anômalos (`< 1990`) | **CANDIDATA** |
| `DIA_FIM_VIGENCIA_PROPOSTA` | `data_fim_vigencia_proposta` | `string` | `DATE` | SIM | `TO_DATE(DIA_FIM_VIGENCIA_PROPOSTA, 'dd/MM/yyyy')` | Tratar anos anômalos (`> 2050`) | **CANDIDATA** |
| `OBJETO_PROPOSTA` | `objeto_proposta` | `string` | `STRING` | SIM | `CASE WHEN TRIM(OBJETO_PROPOSTA) IN ('', '-') THEN NULL ELSE TRIM(OBJETO_PROPOSTA) END` | Limpeza de sentinela `'-'` | **CONFIRMADA** |
| `ITEM_INVESTIMENTO` | `item_investimento` | `string` | `STRING` | SIM | `NULLIF(TRIM(ITEM_INVESTIMENTO), '')` | Taxa nulos esperada: ~62,2% | **CONFIRMADA** |
| `ENVIADA_MANDATARIA` | `enviada_mandataria` | `string` | `STRING` | NÃO | `TRIM(ENVIADA_MANDATARIA)` | Valores: SIM, NÃO, NÃO APLICÁVEL | **CONFIRMADA** |
| `NOME_SUBTIPO_PROPOSTA` | `nome_subtipo_proposta` | `string` | `STRING` | SIM | `NULLIF(TRIM(NOME_SUBTIPO_PROPOSTA), '')` | Nulos: 99,15% | **CONFIRMADA** |
| `DESCRICAO_SUBTIPO_PROPOSTA`| `descricao_subtipo_proposta` | `string` | `STRING` | SIM | `NULLIF(TRIM(DESCRICAO_SUBTIPO_PROPOSTA), '')` | Nulos: 99,15% | **CONFIRMADA** |
| `VL_GLOBAL_PROP` | `valor_global_proposta` | `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_GLOBAL_PROP, ',', '.') AS DECIMAL(17,2))` | `>= 0` | **CONFIRMADA** |
| `VL_REPASSE_PROP` | `valor_repasse_proposta` | `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_REPASSE_PROP, ',', '.') AS DECIMAL(17,2))` | `>= 0` | **CONFIRMADA** |
| `VL_CONTRAPARTIDA_PROP` | `valor_contrapartida_proposta`| `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_CONTRAPARTIDA_PROP, ',', '.') AS DECIMAL(17,2))` | Aceita negativos raros | **CONFIRMADA** |
| `CD_AGENCIA` | `codigo_agencia` | `string` | `STRING` | SIM | `CASE WHEN TRIM(CD_AGENCIA) IN ('', '-') THEN NULL ELSE TRIM(CD_AGENCIA) END` | Preservar zeros à esquerda | **CONFIRMADA** |
| `CD_CONTA` | `codigo_conta` | `string` | `STRING` | SIM | `NULLIF(TRIM(CD_CONTA), '')` | Preservar zeros à esquerda (76,8% nulos) | **CONFIRMADA** |

---

### 3.2. Entidade: `silver.siconv_convenio` (ou `silver_convenio`)

- **Fonte Bronze:** `bronze.siconv_convenio`
- **Grão:** 1 linha por Convênio / Instrumento de Transferência formalizado.
- **Chave Primária Lógica:** `numero_convenio` (`NR_CONVENIO`).
- **Volume Esperado:** 287.586 linhas (preservando integralmente as 287.586 observações da Bronze, sem descartar os dois conflitos por ausência de critério objetivo de recência).
- **Estratégia de Deduplicação:** **NÃO DEDUPLICAR**. A investigação R3-A.1 comprovou que os dois convênios com 2 linhas (`949286` e `956078`) possuem exatamente o mesmo run, mesmo SHA-256, mesmo timestamp técnico e datas oficiais idênticas, divergindo unicamente em `VL_SALDO_CONTA`. Não existe critério objetivo de recência. É expressamente proibido desempate arbitrário por maior/menor saldo ou `ROW_NUMBER()`. As duas duplicidades históricas são preservadas na Silver, e o teste `unique(numero_convenio)` NÃO é teste aprovado para o R3-B.

| Coluna Bronze | Coluna Silver Candidata | Tipo Bronze | Tipo Silver Candidato | Nullable | Regra de Transformação | Regra de Qualidade | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NR_CONVENIO` | `numero_convenio` | `string` | `STRING` | NÃO | `TRIM(NR_CONVENIO)` | `not_null` (2 conflitos conhecidos preservados) | **CONFIRMADA** |
| `ID_PROPOSTA` | `id_proposta` | `string` | `BIGINT` | NÃO | `CAST(ID_PROPOSTA AS BIGINT)` | `not_null` | **CONFIRMADA** |
| `DIA` | `dia_assinatura` | `string` | `SMALLINT` | SIM | `CAST(DIA AS SMALLINT)` | Nulo se não assinado | **CONFIRMADA** |
| `MES` | `mes_assinatura` | `string` | `SMALLINT` | SIM | `CAST(MES AS SMALLINT)` | Nulo se não assinado | **CONFIRMADA** |
| `ANO` | `ano_assinatura` | `string` | `SMALLINT` | SIM | `CAST(ANO AS SMALLINT)` | Nulo se não assinado | **CONFIRMADA** |
| `DIA_ASSIN_CONV` | `data_assinatura_convenio` | `string` | `DATE` | SIM | `TO_DATE(DIA_ASSIN_CONV, 'dd/MM/yyyy')` | Nulo se não assinado | **CONFIRMADA** |
| `SIT_CONVENIO` | `situacao_convenio` | `string` | `STRING` | SIM | `NULLIF(TRIM(SIT_CONVENIO), '')` | Nulos: 6,25% | **CONFIRMADA** |
| `SUBSITUACAO_CONV` | `subsituacao_convenio` | `string` | `STRING` | SIM | `NULLIF(TRIM(SUBSITUACAO_CONV), '')` | Nulos: 99,45% | **CONFIRMADA** |
| `SITUACAO_PUBLICACAO` | `situacao_publicacao` | `string` | `STRING` | NÃO | `TRIM(SITUACAO_PUBLICACAO)` | `not_null` | **CONFIRMADA** |
| `INSTRUMENTO_ATIVO` | `is_instrumento_ativo` | `string` | `BOOLEAN` | SIM | `CASE WHEN UPPER(TRIM(INSTRUMENTO_ATIVO)) = 'SIM' THEN TRUE WHEN UPPER(TRIM(INSTRUMENTO_ATIVO)) = 'NÃO' THEN FALSE ELSE NULL END` | `accepted_values: [true, false]` | **CONFIRMADA** |
| `IND_OPERA_OBTV` | `is_opera_obtv` | `string` | `BOOLEAN` | SIM | `CASE WHEN UPPER(TRIM(IND_OPERA_OBTV)) = 'SIM' THEN TRUE WHEN UPPER(TRIM(IND_OPERA_OBTV)) = 'NÃO' THEN FALSE ELSE NULL END` | `accepted_values: [true, false]` | **CONFIRMADA** |
| `NR_PROCESSO` | `numero_processo` | `string` | `STRING` | SIM | `NULLIF(TRIM(NR_PROCESSO), '')` | 1 nulo observado | **CONFIRMADA** |
| `UG_EMITENTE` | `unidade_gestora_emitente` | `string` | `STRING` | SIM | `NULLIF(TRIM(UG_EMITENTE), '')` | Preservar zeros à esquerda (12% nulos) | **CONFIRMADA** |
| `DIA_PUBL_CONV` | `data_publicacao_convenio` | `string` | `DATE` | SIM | `TO_DATE(DIA_PUBL_CONV, 'dd/MM/yyyy')` | Nulos: 17,5% | **CONFIRMADA** |
| `DIA_INIC_VIGENC_CONV` | `data_inicio_vigencia_convenio`| `string` | `DATE` | NÃO | `TO_DATE(DIA_INIC_VIGENC_CONV, 'dd/MM/yyyy')` | `not_null` | **CONFIRMADA** |
| `DIA_FIM_VIGENC_CONV` | `data_fim_vigencia_convenio` | `string` | `DATE` | NÃO | `TO_DATE(DIA_FIM_VIGENC_CONV, 'dd/MM/yyyy')` | `not_null` | **CONFIRMADA** |
| `DIA_FIM_VIGENC_ORIGINAL_CONV`| `data_fim_vigencia_original` | `string` | `DATE` | NÃO | `TO_DATE(DIA_FIM_VIGENC_ORIGINAL_CONV, 'dd/MM/yyyy')` | `not_null` | **CONFIRMADA** |
| `DIAS_PREST_CONTAS` | `dias_prestacao_contas` | `string` | `INT` | NÃO | `CAST(DIAS_PREST_CONTAS AS INT)` | `not_null`, `>= 0` | **CONFIRMADA** |
| `DIA_LIMITE_PREST_CONTAS`| `data_limite_prestacao_contas` | `string` | `DATE` | NÃO | `TO_DATE(DIA_LIMITE_PREST_CONTAS, 'dd/MM/yyyy')` | `not_null` | **CONFIRMADA** |
| `DATA_SUSPENSIVA` | `data_suspensiva` | `string` | `DATE` | SIM | `TO_DATE(DATA_SUSPENSIVA, 'dd/MM/yyyy')` | 93,16% nulos | **CONFIRMADA** |
| `DATA_RETIRADA_SUSPENSIVA`| `data_retirada_suspensiva` | `string` | `DATE` | SIM | `TO_DATE(DATA_RETIRADA_SUSPENSIVA, 'dd/MM/yyyy')` | 56,29% nulos | **CONFIRMADA** |
| `DIAS_CLAUSULA_SUSPENSIVA`| `dias_clausula_suspensiva` | `string` | `INT` | SIM | `CAST(DIAS_CLAUSULA_SUSPENSIVA AS INT)` | 92,31% nulos | **CONFIRMADA** |
| `SITUACAO_CONTRATACAO` | `situacao_contratacao` | `string` | `STRING` | SIM | `NULLIF(TRIM(SITUACAO_CONTRATACAO), '')` | 22,25% nulos | **CONFIRMADA** |
| `IND_ASSINADO` | `is_assinado` | `string` | `BOOLEAN` | SIM | `CASE WHEN UPPER(TRIM(IND_ASSINADO)) = 'SIM' THEN TRUE WHEN UPPER(TRIM(IND_ASSINADO)) = 'NÃO' THEN FALSE ELSE NULL END` | `accepted_values: [true, false]` | **CONFIRMADA** |
| `MOTIVO_SUSPENSAO` | `motivo_suspensao` | `string` | `STRING` | SIM | `NULLIF(TRIM(MOTIVO_SUSPENSAO), '')` | 93,16% nulos | **CONFIRMADA** |
| `IND_FOTO` | `is_foto` | `string` | `BOOLEAN` | SIM | `CASE WHEN UPPER(TRIM(IND_FOTO)) = 'SIM' THEN TRUE WHEN UPPER(TRIM(IND_FOTO)) = 'NÃO' THEN FALSE ELSE NULL END` | `accepted_values: [true, false]` | **CONFIRMADA** |
| `QTDE_CONVENIOS` | `quantidade_convenios` | `string` | `SMALLINT` | NÃO | `CAST(QTDE_CONVENIOS AS SMALLINT)` | Coluna constante (= 1) | **CONFIRMADA** |
| `QTD_TA` | `quantidade_termos_aditivos` | `string` | `SMALLINT` | SIM | `CAST(QTD_TA AS SMALLINT)` | 48,37% nulos | **CONFIRMADA** |
| `QTD_PRORROGA` | `quantidade_prorrogacoes` | `string` | `SMALLINT` | SIM | `CAST(QTD_PRORROGA AS SMALLINT)` | 69,85% nulos | **CONFIRMADA** |
| `VL_GLOBAL_CONV` | `valor_global_convenio` | `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_GLOBAL_CONV, ',', '.') AS DECIMAL(17,2))` | `not_null`, `>= 0` | **CONFIRMADA** |
| `VL_REPASSE_CONV` | `valor_repasse_convenio` | `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_REPASSE_CONV, ',', '.') AS DECIMAL(17,2))` | `not_null`, `>= 0` | **CONFIRMADA** |
| `VL_CONTRAPARTIDA_CONV` | `valor_contrapartida_convenio` | `string` | `DECIMAL(17,2)` | NÃO | `CAST(REPLACE(VL_CONTRAPARTIDA_CONV, ',', '.') AS DECIMAL(17,2))` | Aceita raros negativos | **CONFIRMADA** |
| `VL_EMPENHADO_CONV` | `valor_empenhado_convenio` | `string` | `DECIMAL(17,2)` | SIM | `CAST(REPLACE(VL_EMPENHADO_CONV, ',', '.') AS DECIMAL(17,2))` | Aceita negativos (estornos) | **CONFIRMADA** |
| `VL_DESEMBOLSADO_CONV` | `valor_desembolsado_convenio` | `string` | `DECIMAL(17,2)` | SIM | `CAST(REPLACE(VL_DESEMBOLSADO_CONV, ',', '.') AS DECIMAL(17,2))` | 34,42% nulos | **CONFIRMADA** |
| `VL_SALDO_REMAN_TESOURO`| `valor_saldo_remanescente_tesouro`| `string`| `DECIMAL(17,2)`| SIM | `CAST(REPLACE(VL_SALDO_REMAN_TESOURO, ',', '.') AS DECIMAL(17,2))`| 49,52% nulos | **CONFIRMADA** |
| `VL_SALDO_REMAN_CONVENENTE`| `valor_saldo_remanescente_convenente`| `string`| `DECIMAL(17,2)`| SIM | `CAST(REPLACE(VL_SALDO_REMAN_CONVENENTE, ',', '.') AS DECIMAL(17,2))`| 73,49% nulos | **CONFIRMADA** |
| `VL_RENDIMENTO_APLICACAO`| `valor_rendimento_aplicacao`| `string`| `DECIMAL(17,2)`| SIM | `CAST(REPLACE(VL_RENDIMENTO_APLICACAO, ',', '.') AS DECIMAL(17,2))`| 97,00% nulos | **CONFIRMADA** |
| `VL_INGRESSO_CONTRAPARTIDA`| `valor_ingresso_contrapartida`| `string`| `DECIMAL(17,2)`| SIM | `CAST(REPLACE(VL_INGRESSO_CONTRAPARTIDA, ',', '.') AS DECIMAL(17,2))`| 48,53% nulos | **CONFIRMADA** |
| `VL_SALDO_CONTA` | `valor_saldo_conta` | `string` | `DECIMAL(17,2)` | SIM | `CAST(REPLACE(VL_SALDO_CONTA, ',', '.') AS DECIMAL(17,2))` | Tratar formato ponto/vírgula | **CONFIRMADA** |
| `VALOR_GLOBAL_ORIGINAL_CONV`| `valor_global_original_convenio`| `string`| `DECIMAL(17,2)`| SIM | `CAST(REPLACE(VALOR_GLOBAL_ORIGINAL_CONV, ',', '.') AS DECIMAL(17,2))`| 70,34% nulos | **CONFIRMADA** |

---

### 3.3. Entidade: `silver.siconv_programa_proposta` (ou `silver_programa_proposta`)

- **Fonte Bronze:** `bronze.siconv_programa_proposta`
- **Grão:** 1 linha por vínculo associativo Programa-Proposta.
- **Chave Primária Lógica:** Par composto `(id_programa, id_proposta)` (100% ÚNICA e NOT NULL comprovada).
- **Volume Esperado:** 1.158.975 linhas.
- **Estratégia de Deduplicação:** **NÃO DEDUPLICAR** (zero duplicatas observadas).

| Coluna Bronze | Coluna Silver Candidata | Tipo Bronze | Tipo Silver Candidato | Nullable | Regra de Transformação | Regra de Qualidade | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ID_PROGRAMA` | `id_programa` | `string` | `BIGINT` | NÃO | `CAST(ID_PROGRAMA AS BIGINT)` | `not_null` | **CONFIRMADA** |
| `ID_PROPOSTA` | `id_proposta` | `string` | `BIGINT` | NÃO | `CAST(ID_PROPOSTA AS BIGINT)` | `not_null` | **CONFIRMADA** |

---

### 3.4. Entidade: `silver.siconv_programa` (ou `silver_programa_elegibilidade`)

- **Fonte Bronze:** `bronze.siconv_programa`
- **Grão:** Critério de elegibilidade e abertura regional/orçamentária do programa governamental.
- **Identidade do Programa Lógico:** `id_programa` (53.018 distintos).
- **Identidade da Linha de Elegibilidade:** Chave composta de 5 colunas `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA, ACAO_ORCAMENTARIA)` (comprovada 100% única em 1.257.350 linhas).
- **Chave Primária Técnica Recomendada:** Surrogate key determinística calculada via SHA-256:
  `id_programa_elegibilidade = sha256(concat_ws('||', coalesce(id_programa, ''), coalesce(modalidade_programa, ''), coalesce(natureza_juridica_programa, ''), coalesce(uf_programa, ''), coalesce(acao_orcamentaria, '')))`.
  *Proibição estrita: Não utilizar MD5 e não utilizar chaves parciais não únicas.*
- **Volume Esperado:** 1.257.350 linhas.
- **Estratégia de Deduplicação:** **NÃO DEDUPLICAR** na tabela de elegibilidade, pois todos os 1.257.350 registros são combinações oficiais distintas.

| Coluna Bronze | Coluna Silver Candidata | Tipo Bronze | Tipo Silver Candidato | Nullable | Regra de Transformação | Regra de Qualidade | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `COD_ORGAO_SUP_PROGRAMA` | `codigo_orgao_superior_programa` | `string` | `STRING` | NÃO | `TRIM(COD_ORGAO_SUP_PROGRAMA)` | `not_null` | **CONFIRMADA** |
| `DESC_ORGAO_SUP_PROGRAMA`| `descricao_orgao_superior_programa`| `string`| `STRING` | NÃO | `TRIM(DESC_ORGAO_SUP_PROGRAMA)` | `not_null` | **CONFIRMADA** |
| `ID_PROGRAMA` | `id_programa` | `string` | `BIGINT` | NÃO | `CAST(ID_PROGRAMA AS BIGINT)` | `not_null` (NÃO é única nesta tabela!) | **CONFIRMADA** |
| `COD_PROGRAMA` | `codigo_programa` | `string` | `STRING` | NÃO | `TRIM(COD_PROGRAMA)` | `not_null` | **CONFIRMADA** |
| `NOME_PROGRAMA` | `nome_programa` | `string` | `STRING` | NÃO | `TRIM(NOME_PROGRAMA)` | `not_null` | **CONFIRMADA** |
| `SIT_PROGRAMA` | `situacao_programa` | `string` | `STRING` | NÃO | `TRIM(SIT_PROGRAMA)` | Domínio fechado: DISPONIBILIZADO, CADASTRADO, INATIVO | **CONFIRMADA** |
| `DATA_DISPONIBILIZACAO` | `data_disponibilizacao` | `string` | `DATE` | SIM | `TO_DATE(DATA_DISPONIBILIZACAO, 'dd/MM/yyyy')` | Nulos: 10,09% | **CONFIRMADA** |
| `ANO_DISPONIBILIZACAO` | `ano_disponibilizacao` | `string` | `SMALLINT` | SIM | `CAST(ANO_DISPONIBILIZACAO AS SMALLINT)` | Nulos: 10,09% | **CONFIRMADA** |
| `DT_PROG_INI_RECEB_PROP` | `data_inicio_recebimento_proposta`| `string`| `DATE` | SIM | `TO_DATE(DT_PROG_INI_RECEB_PROP, 'dd/MM/yyyy')` | Tratar anomalia `0006-03-11` (Regra Candidata) | **CANDIDATA** |
| `DT_PROG_FIM_RECEB_PROP` | `data_fim_recebimento_proposta` | `string` | `DATE` | SIM | `TO_DATE(DT_PROG_FIM_RECEB_PROP, 'dd/MM/yyyy')` | Tratar anomalia `5008-12-15` (Regra Candidata) | **CANDIDATA** |
| `DT_PROG_INI_EMENDA_PAR` | `data_inicio_emenda_parlamentar` | `string` | `DATE` | SIM | `TO_DATE(DT_PROG_INI_EMENDA_PAR, 'dd/MM/yyyy')` | Nulos: 65,88% | **CONFIRMADA** |
| `DT_PROG_FIM_EMENDA_PAR` | `data_fim_emenda_parlamentar` | `string` | `DATE` | SIM | `TO_DATE(DT_PROG_FIM_EMENDA_PAR, 'dd/MM/yyyy')` | Nulos: 65,89% | **CONFIRMADA** |
| `DT_PROG_INI_BENEF_ESP` | `data_inicio_beneficiario_especifico`| `string`| `DATE` | SIM | `TO_DATE(DT_PROG_INI_BENEF_ESP, 'dd/MM/yyyy')`| Nulos: 76,88% | **CONFIRMADA** |
| `DT_PROG_FIM_BENEF_ESP` | `data_fim_beneficiario_especifico` | `string` | `DATE` | SIM | `TO_DATE(DT_PROG_FIM_BENEF_ESP, 'dd/MM/yyyy')` | Nulos: 76,88% | **CONFIRMADA** |
| `MODALIDADE_PROGRAMA` | `modalidade_programa` | `string` | `STRING` | SIM | `NULLIF(TRIM(MODALIDADE_PROGRAMA), '')` | Nulos: 0,05% | **CONFIRMADA** |
| `NATUREZA_JURIDICA_PROGRAMA`| `natureza_juridica_programa` | `string` | `STRING` | NÃO | `TRIM(NATUREZA_JURIDICA_PROGRAMA)` | `not_null` | **CONFIRMADA** |
| `UF_PROGRAMA` | `uf_programa` | `string` | `STRING` | NÃO | `UPPER(TRIM(UF_PROGRAMA))` | `not_null`, 27 UFs | **CONFIRMADA** |
| `ACAO_ORCAMENTARIA` | `acao_orcamentaria` | `string` | `STRING` | SIM | `NULLIF(TRIM(ACAO_ORCAMENTARIA), '')` | 1 nulo | **CONFIRMADA** |
| `NOME_SUBTIPO_PROGRAMA` | `nome_subtipo_programa` | `string` | `STRING` | SIM | `NULLIF(TRIM(NOME_SUBTIPO_PROGRAMA), '')` | Nulos: 99,12% | **CONFIRMADA** |
| `DESCRICAO_SUBTIPO_PROGRAMA`| `descricao_subtipo_programa` | `string` | `STRING` | SIM | `NULLIF(TRIM(DESCRICAO_SUBTIPO_PROGRAMA), '')` | Nulos: 99,12% | **CONFIRMADA** |

---

### 3.5. Entidade: `silver.siconv_programa_cadastral` (ou `silver_programa_cadastral`)

- **Fonte Bronze:** `bronze.siconv_programa` (agrupada por `ID_PROGRAMA`).
- **Status da Entidade:** **CONFIRMADO** (dependência funcional 1:1 estrita comprovada no R3-A.1 para todos os 53.018 programas).
- **Grão:** 1 linha por Programa Governamental cadastrado.
- **Chave Primária Lógica:** `id_programa`.
- **Volume Esperado:** 53.018 linhas.
- **Atributos Cadastrais Determinados (1:1):**
  - `codigo_orgao_superior_programa`
  - `descricao_orgao_superior_programa`
  - `codigo_programa`
  - `nome_programa`
  - `situacao_programa`
  - `data_disponibilizacao`
  - `ano_disponibilizacao`

---

## 4. Testes dbt Candidatos

### 4.1. Testes Seguros (Aptos para Implementação Imediata no R3-B)
- **`silver.siconv_proposta`**:
  - `not_null`: `id_proposta`, `uf_proponente`, `codigo_municipio_ibge`, `data_proposta`, `valor_global_proposta`, `valor_repasse_proposta`.
  - `unique`: `id_proposta`.
  - `accepted_values` em `uf_proponente`: 27 siglas de UF.
  - `accepted_values` em `enviada_mandataria`: `['SIM', 'NÃO', 'NÃO APLICÁVEL']`.
- **`silver.siconv_programa_proposta`**:
  - `not_null`: `id_programa`, `id_proposta`.
  - `dbt_utils.unique_combination_of_columns`: `[id_programa, id_proposta]`.
  - `relationships` de `id_programa` para `silver.siconv_programa.id_programa` (ou tabela de programas desduplicada): 100% de match.
- **`silver.siconv_convenio`**:
  - `not_null`: `numero_convenio`, `id_proposta`, `data_inicio_vigencia_convenio`, `data_fim_vigencia_convenio`, `valor_global_convenio`, `valor_repasse_convenio`.
  - `relationships` de `id_proposta` para `silver.siconv_proposta.id_proposta` (100% de integridade referencial comprovada).
  - `accepted_values` em `is_instrumento_ativo`, `is_opera_obtv`, `is_assinado`, `is_foto`: `[true, false]`.
  - **ATENÇÃO:** O teste `unique` em `numero_convenio` **NÃO é teste obrigatório aprovado para o R3-B**, pois a Silver preservará integralmente as 287.586 observações da Bronze, mantendo os 2 conflitos conhecidos sem descarte destrutivo arbitrário.

### 4.2. Testes Proibidos (NUNCA Aplicar sem Redefinir o Grão)
- **PROIBIDO:** Teste de `unique` sobre `id_programa` na tabela `silver.siconv_programa` (violação comprovada: 53.018 distintos para 1.257.350 linhas).
- **PROIBIDO:** Teste de `unique` sobre `numero_proposta` em `silver.siconv_proposta` (violação comprovada: 1.717 repetições legítimas de numeração operacional).
- **PROIBIDO:** Teste de `relationships` estrito de `silver.siconv_proposta.id_proposta` para `silver.siconv_programa_proposta.id_proposta` (existem 78 propostas legítimas que não têm vínculo com programas cadastrados).
- **PROIBIDO:** Teste de `relationships` estrito de `silver.siconv_programa_proposta.id_proposta` para `silver.siconv_proposta.id_proposta` sem antes tratar os 3 registros órfãos históricos.

---

## 5. Métricas Baseline para Reconciliação R3-B (Bronze $\rightarrow$ Silver)

| Entidade | Linhas Bronze de Entrada | Linhas Silver Esperadas | Critério de Reconciliação | Tolerância Aceitável |
| :--- | :--- | :--- | :--- | :--- |
| **Proposta** | 1.157.619 | **1.157.619** | Zero perdas de linhas. Soma de `valor_global_proposta` idêntica. | 0 linhas / R$ 0,00 |
| **Ponte Programa-Proposta** | 1.158.975 | **1.158.975** | Zero perdas de pares únicos `(id_programa, id_proposta)`. | 0 linhas |
| **Convênio** | 287.586 | **287.586** | Preservação integral das observações (sem descarte das 2 linhas de saldo). | 0 linhas / R$ 0,00 |
| **Programa (Elegibilidade)**| 1.257.350 | **1.257.350** | Zero perdas de critérios de elegibilidade. Chave composta 100% única. | 0 linhas |
| **Programa Cadastral** | 1.257.350 (53.018 IDs) | **53.018** | Entidade 1:1 exata determinada por `id_programa`. | 0 linhas |

### Reconciliação Financeira Estrita:
- **Soma Base de `valor_global_convenio`:** **R$ 356.856.636.504,87** (em precisão exata `DECIMAL(38,2)`).
- **Regra de Tipo:** Transformações individuais em `DECIMAL(17,2)` e reconciliações agregadas em `DECIMAL(38,2)`.
- **Proibição Absoluta:** O uso de `DOUBLE` ou `FLOAT` é estritamente proibido para qualquer reconciliação ou cálculo financeiro no Lakehouse.

---

## 6. R3-A.1 — Fechamento dos Findings da Revisão Humana

1. **Grão de `siconv_programa`:** Comprovada a unicidade estrita de 100% dos 1.257.350 registros na chave composta `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA, ACAO_ORCAMENTARIA)` (zero colisões). Surrogate key padronizada em SHA-256 (MD5 expressamente vedado).
2. **Convênios Conflitantes:** Comprovado que as 2 linhas duplicadas em `949286` e `956078` vieram no mesmo run de ingestão, possuem mesmo SHA-256 de origem, mesmo timestamp técnico e datas oficiais idênticas, divergindo unicamente em `VL_SALDO_CONTA`. Inexistindo critério objetivo de recência, a Silver preservará as 287.586 linhas, e `unique(numero_convenio)` não é teste obrigatório.
3. **Entidade `programa_cadastral`:** Classificada formalmente como **CONFIRMADO**. Apresentou dependência funcional estrita de 100% (0 divergências) em todos os 7 atributos e na tupla completa para todos os 53.018 programas, permitindo criação direta de `silver.siconv_programa_cadastral` sem regras arbitrárias de desempate.
4. **Baseline Financeiro DECIMAL:** Fixado o valor base em **R$ 356.856.636.504,87**. Comprovada matematicamente a explosão da ponte para 287.974 linhas (+R$ 1,50 bilhão) e a expansão para 24.243.823 linhas equivalentes (+R$ 25,60 trilhões / 72,7567x) via cálculo de multiplicidades agregadas sem materialização explosiva.
