# R4-A — Contrato Analítico Candidato da Camada Gold

Este documento formaliza as especificações estruturais, conceituais, relacionais e contratuais das entidades candidatas para a modelagem dimensional da camada **Gold** do Lakehouse Transferegov, atualizado com os refinamentos do marco **R4-A.1**.

---

## 1. Visão Geral da Arquitetura Dimensional Candidata

A arquitetura adota o modelo dimensional constelação (Kimball) com tratamento explícito de relacionamentos muitos-para-muitos, uso de chaves naturais diretas nas dimensões cadastrais e desacoplamento de observações bancárias divergentes:

```text
       [dim_programa]
             │
             │ (1:N)
             ▼
 [bridge_programa_proposta]
             │
             │ (N:1)
             ▼
      [fct_proposta] ◄────── (1 : 0..1) ──────► [fct_convenio]
        │    │    │                                    │
        │    │    └────────────┐                       │ (1:N)
        │    │                 │                       ▼
        │    │                 │           [fct_convenio_saldo_observacao]
        ▼    ▼                 ▼
   [dim_proponente]    [dim_municipio]
        ▲
        │ (role-playing)
        ▼
   [dim_orgao] ◄── (superior / concedente)
        ▲
        │ (role-playing)
        ▼
   [dim_data] ◄── (proposta / assinatura / publicacao / vigencia)
```

---

## 2. Estratégia de Chaves da Gold

O modelo dimensional da Gold adota uma diretriz pragmática de engenharia de dados:

### 2.1. Uso de Chaves Naturais Diretas (Business Keys)
Nas dimensões onde a chave natural de negócio da fonte é universalmente estável, única e não sujeita a versionamento SCD2 no R4 inicial, **a chave natural é adotada diretamente como Primary Key**, sem geração de surrogate keys artificiais (`_sk`):
- `dim_proponente.identificacao_proponente` (CNPJ / CPF do proponente): PK natural.
- `dim_municipio.codigo_municipio_ibge` (Código IBGE de 7 dígitos): PK natural.
- `dim_orgao.codigo_orgao` (Código numérico do órgão): PK natural.
- `dim_programa.id_programa` (Identificador numérico do programa): PK natural.

*Justificativa*: A criação de chaves substitutas artificiais (`proponente_sk`, `municipio_sk`, etc.) adicionaria sobrecarga de tradução e pipelines de lookup sem ganho analítico, dado que o snapshot atual não possui fontes de CDC para SCD2 e as chaves naturais são limpas e estáveis.

### 2.2. Chave Semântica e Sentinelas em `dim_data`
`dim_data` utiliza uma chave inteira inteligente de calendário:
- `dim_data.data_sk`: Inteiro no formato `YYYYMMDD` (ex: `20260916`).
- Chaves Sentinela de Exceção:
  - `-1`: `Data Não Informada / NULL` na fonte.
  - `-2`: `Data Fora da Janela Analítica` (< 1990 ou > 2050).

---

## 3. Contratos das Entidades Fato

### 3.1. `gold.fct_proposta`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_proposta`
- **Grão**: 1 linha por `id_proposta`
- **Chave Primária (PK)**: `id_proposta`
- **Chaves Estrangeiras (FKs)**:
  - `identificacao_proponente` $\to$ `dim_proponente.identificacao_proponente`
  - `codigo_municipio_ibge` $\to$ `dim_municipio.codigo_municipio_ibge`
  - `codigo_orgao_superior` $\to$ `dim_orgao.codigo_orgao` (role: órgão superior)
  - `codigo_orgao` $\to$ `dim_orgao.codigo_orgao` (role: órgão concedente)
  - `data_proposta_sk` $\to$ `dim_data.data_sk`
- **Medidas**:
  - `quantidade_propostas` (INT, valor constante 1)
  - `valor_global_proposta` (DECIMAL(17,2))
  - `valor_repasse_proposta` (DECIMAL(17,2))
  - `valor_contrapartida_proposta` (DECIMAL(17,2))
- **Atributos Degenerados**: `numero_proposta`, `modalidade`, `situacao_proposta`, `objeto_proposta`, `nome_subtipo_proposta`, `descricao_subtipo_proposta`
- **Relacionamentos e Joins**:
  - Com `fct_convenio`: Relação semântica de **$1 : 0..1$** a partir de proposta (nem toda proposta é formalizada em convênio; 24,84% no snapshot R4-A possuem convênio).
  - Com dimensões cadastrais (`dim_proponente`, `dim_municipio`, `dim_orgao`, `dim_data`): Joins $N:1$ preservam o grão da proposta.
  - Com `bridge_programa_proposta`: Join direto **proibido** sem agregação prévia, sob risco de duplicar propostas em $N:N$.
- **Reconciliação Operacional Dinâmica**:
  $$\text{COUNT}(gold.fct\_proposta) = \text{COUNT}(silver.siconv\_proposta)$$
  $$\sum valor\_global\_proposta = \sum silver.siconv\_proposta.valor\_global\_proposta$$
  $$\sum valor\_repasse\_proposta = \sum silver.siconv\_proposta.valor\_repasse\_proposta$$
  $$\sum valor\_contrapartida\_proposta = \sum silver.siconv\_proposta.valor\_contrapartida\_proposta$$
  *(Tolerância: R$ 0,00; tipo DECIMAL).*
- **Baseline Histórico R4-A (Snapshot 16/09/2026)**:
  - Linhas: 1.157.619 | Valor Global: R$ 1.495.209.875.334,42 | Repasse: R$ 1.425.758.135.735,63 | Contrapartida: R$ 69.451.779.598,79.

---

### 3.2. `gold.fct_convenio`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_convenio`
- **Grão**: 1 linha por `numero_convenio` (instrumento jurídico canônico)
- **Chave Primária (PK)**: `numero_convenio`
- **Chaves Estrangeiras (FKs)**:
  - `id_proposta` $\to$ `fct_proposta.id_proposta` (relação $1:1$ partindo do convênio: todo convênio vincula-se a exatamente 1 proposta)
  - `data_assinatura_sk` $\to$ `dim_data.data_sk`
  - `data_publicacao_sk` $\to$ `dim_data.data_sk`
  - `data_inicio_vigencia_sk` $\to$ `dim_data.data_sk`
  - `data_fim_vigencia_sk` $\to$ `dim_data.data_sk`
  - `data_limite_prestacao_contas_sk` $\to$ `dim_data.data_sk`
- **Medidas Canônicas Estáveis**:
  - `quantidade_convenios` (INT, valor constante 1)
  - `quantidade_termos_aditivos` (INT)
  - `quantidade_prorrogacoes` (INT)
  - `valor_global_convenio` (DECIMAL(17,2))
  - `valor_repasse_convenio` (DECIMAL(17,2))
  - `valor_contrapartida_convenio` (DECIMAL(17,2))
  - `valor_empenhado_convenio` (DECIMAL(17,2))
  - `valor_desembolsado_convenio` (DECIMAL(17,2))
  - `valor_saldo_remanescente_tesouro` (DECIMAL(17,2))
  - `valor_saldo_remanescente_convenente` (DECIMAL(17,2))
  - `valor_rendimento_aplicacao` (DECIMAL(17,2))
  - `valor_ingresso_contrapartida` (DECIMAL(17,2))
  - `valor_global_original_convenio` (DECIMAL(17,2))
- **Medida Expressamente Excluída**: `valor_saldo_conta` (divergente entre observações na fonte).
- **Atributos Degenerados**: `situacao_convenio`, `subsituacao_convenio`, `situacao_publicacao`, `situacao_contratacao`, `is_instrumento_ativo`, `is_opera_obtv`, `is_assinado`, `numero_processo`, `unidade_gestora_emitente`
- **Reconciliação Operacional Dinâmica**:
  A validação deve ser efetuada contra a consulta canônica deduplicada sobre a Silver corrente:
  ```sql
  WITH canonical_convenio AS (
      SELECT DISTINCT
          numero_convenio,
          id_proposta,
          data_assinatura_convenio,
          data_publicacao_convenio,
          data_inicio_vigencia_convenio,
          data_fim_vigencia_convenio,
          data_limite_prestacao_contas,
          quantidade_termos_aditivos,
          quantidade_prorrogacoes,
          valor_global_convenio,
          valor_repasse_convenio,
          valor_contrapartida_convenio,
          valor_empenhado_convenio,
          valor_desembolsado_convenio,
          valor_saldo_remanescente_tesouro,
          valor_saldo_remanescente_convenente,
          valor_rendimento_aplicacao,
          valor_ingresso_contrapartida,
          valor_global_original_convenio,
          situacao_convenio,
          subsituacao_convenio,
          situacao_publicacao,
          situacao_contratacao,
          is_instrumento_ativo,
          is_opera_obtv,
          is_assinado
      FROM silver.siconv_convenio
  )
  ```
  Regras:
  $$\text{COUNT}(gold.fct\_convenio) = \text{COUNT}(canonical\_convenio)$$
  $$\sum gold.fct\_convenio.<medida> = \sum canonical\_convenio.<medida>$$
- **Baseline Histórico R4-A (Snapshot 16/09/2026)**:
  - Linhas Canônicas: 287.584
  - Valor Global Canônico: R$ 356.840.744.533,16
  - Repasse: R$ 331.271.766.468,34 | Contrapartida: R$ 23.709.764.114,58
  - Empenhado: R$ 192.064.543.060,79 | Desembolsado: R$ 153.205.012.397,29.

---

### 3.3. `gold.fct_convenio_saldo_observacao`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_convenio`
- **Grão**: 1 linha por observação física (`id_convenio_observacao`)
- **Chave Primária (PK)**: `id_convenio_observacao`
- **Chave Estrangeira (FK)**:
  - `numero_convenio` $\to$ `fct_convenio.numero_convenio`
- **Medida Observacional**:
  - `valor_saldo_conta` (DECIMAL(17,2))
- **Atributos de Auditoria**: `has_source_conflict`, `source_conflict_count`, `__ingested_at_utc`, `__source_file`
- **Reconciliação Dinâmica**:
  $$\text{COUNT}(fct\_convenio\_saldo\_observacao) = \text{COUNT}(silver.siconv\_convenio)$$
  $$\sum valor\_saldo\_conta = \sum silver.siconv\_convenio.valor\_saldo\_conta$$
- **Baseline Histórico R4-A**: 287.586 linhas | Soma de Saldo de Conta: R$ 18.079.072.724,97.

---

### 3.4. `gold.bridge_programa_proposta`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_programa_proposta`
- **Grão**: 1 linha por vínculo `id_programa` $\times$ `id_proposta`
- **Chave Primária Composta (PK)**: `(id_programa, id_proposta)`
- **Chaves Estrangeiras (FKs)**:
  - `id_programa` $\to$ `dim_programa.id_programa`
  - `id_proposta` $\to$ `fct_proposta.id_proposta`
- **Medidas Financeiras**: **NENHUMA**.
- **Reconciliação Dinâmica**:
  $$\text{COUNT}(gold.bridge\_programa\_proposta) = \text{COUNT}(silver.siconv\_programa\_proposta)$$
- **Baseline Histórico R4-A**: 1.158.975 vínculos.

---

## 4. Contratos das Entidades Dimensão

### 4.1. `gold.dim_proponente`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_proposta`
- **Grão**: 1 linha por `identificacao_proponente` (CNPJ / CPF)
- **Chave Primária (PK)**: `identificacao_proponente` (natural key)
- **Atributos Cadastrais**: `nome_proponente`, `codigo_municipio_ibge`, `municipio_proponente`, `uf_proponente`, `cep_proponente`, `endereco_proponente`, `bairro_proponente`, `natureza_juridica`
- **Reconciliação Dinâmica**:
  $$\text{COUNT}(gold.dim\_proponente) = \text{COUNT(DISTINCT } silver.siconv\_proposta.identificacao\_proponente)$$
- **Baseline Histórico R4-A**: 29.325 proponentes.

---

### 4.2. `gold.dim_municipio`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_proposta`
- **Grão**: 1 linha por `codigo_municipio_ibge`
- **Chave Primária (PK)**: `codigo_municipio_ibge` (natural key de 7 dígitos)
- **Atributos**: `nome_municipio`, `sigla_uf`
- **Reconciliação Dinâmica**:
  $$\text{COUNT}(gold.dim\_municipio) = \text{COUNT(DISTINCT } silver.siconv\_proposta.codigo\_municipio\_ibge)$$
- **Baseline Histórico R4-A**: 5.570 municípios observados.

---

### 4.3. `gold.dim_orgao`
- **Status Contratual**: **`CONFIRMADO`** (Dimensão Única Conformada com Role-Playing)
- **Fonte Silver**: `silver.siconv_proposta` e `silver.siconv_programa_cadastral`
- **Grão**: 1 linha por `codigo_orgao`
- **Chave Primária (PK)**: `codigo_orgao` (natural key)
- **Atributos**: `descricao_orgao`, `is_orgao_superior`, `is_orgao_concedente`
- **Reconciliação Dinâmica**:
  O total de linhas deve ser igual à contagem de códigos distintos da união dos 3 papéis na Silver:
  ```sql
  SELECT COUNT(DISTINCT cd) FROM (
      SELECT codigo_orgao_superior as cd FROM silver.siconv_proposta
      UNION
      SELECT codigo_orgao as cd FROM silver.siconv_proposta
      UNION
      SELECT codigo_orgao_superior_programa as cd FROM silver.siconv_programa_cadastral
  )
  ```
- **Baseline Histórico R4-A**: 184 órgãos únicos.

---

### 4.4. `gold.dim_programa`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_programa_cadastral`
- **Grão**: 1 linha por `id_programa`
- **Chave Primária (PK)**: `id_programa` (natural key)
- **Chave Estrangeira (FK)**:
  - `codigo_orgao_superior_programa` $\to$ `dim_orgao.codigo_orgao`
  - `data_disponibilizacao_sk` $\to$ `dim_data.data_sk`
- **Atributos**: `codigo_programa`, `nome_programa`, `situacao_programa`, `ano_disponibilizacao`
- **Reconciliação Dinâmica**:
  $$\text{COUNT}(gold.dim\_programa) = \text{COUNT(DISTINCT } silver.siconv\_programa\_cadastral.id\_programa)$$
- **Baseline Histórico R4-A**: 53.018 programas.

---

### 4.5. `gold.dim_data`
- **Status Contratual**: **`CONFIRMADA EM PRINCÍPIO (COM RESTRIÇÕES)`**
- **Fonte**: Gerador autônomo de calendário diário
- **Grão**: 1 linha por dia civil
- **Chave Primária (PK)**: `data_sk` (INT: `YYYYMMDD`)
- **Janela Analítica Candidata de Governança**: `1990-01-01` a `2050-12-31` (22.280 dias de calendário).
  - *Condição Contratual*: A definição exata da janela analítica deverá ser formalmente ratificada no início do marco R4-B.
- **Registros Sentinela**:
  - `-1`: `Data Não Informada / NULL`
  - `-2`: `Data Fora da Janela Analítica` (<1990 ou >2050)
- **Atributos**: `data`, `ano`, `mes`, `dia`, `trimestre`, `semestre`, `dia_da_semana`, `nome_mes`, `is_fim_de_semana`

---

## 5. Entidades Rejeitadas ou Descartadas

1. **`dim_proponente_perfil`**: Descartada. Proponentes apresentam 100% de estabilidade cadastral no snapshot ativo.
2. **`fct_convenio_alocacao_proporcional`**: Rejeitada. Divisão de valor financeiro por quantidade de programas vinculados ($V/N$) cria números sem base documental na fonte.

---

## 6. R4-A.1 — Fechamento dos Findings da Revisão Humana

- **Semântica de Datas**: Faixa 1990–2050 classificada como janela analítica de governança candidata. Código `-2` renomeado para "Data Fora da Janela Analítica". Status ajustado para `CONFIRMADA EM PRINCÍPIO (COM RESTRIÇÕES)`.
- **Reconciliação Dinâmica**: Contratos de reconciliação ajustados para comparar Silver corrente ↔ Gold corrente via consultas dinâmicas e canonical CTE, desacoplando do snapshot histórico do R4-A.
- **Conformação de Órgão**: Prova completada com 3º par cruzado (`concedente proposta` $\leftrightarrow$ `superior programa`), atestando 0 divergências em 37 códigos compartilhados.
- **Estratégia de Chaves**: Explicitada a adoção de chaves naturais diretas em `dim_proponente`, `dim_municipio`, `dim_orgao` e `dim_programa`.
- **Relação Proposta ↔ Convênio**: Especificada semanticamente como $1 : 0..1$ a partir de proposta, e $1 : 1$ a partir de convênio.
