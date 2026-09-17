# R4-A — Contrato Analítico Candidato da Camada Gold

Este documento formaliza as especificações estruturais, conceituais e contratuais das entidades candidatas para a modelagem dimensional da camada **Gold** do Lakehouse Transferegov.

---

## 1. Visão Geral da Arquitetura Dimensional Candidata

A arquitetura analítica adota o modelo dimensional estrela/constelação (Kimball) com tratamento explícito de relacionamentos muitos-para-muitos e desacoplamento de observações bancárias divergentes:

```text
       [dim_programa]
             │
             │ (1:N)
             ▼
 [bridge_programa_proposta]
             │
             │ (N:1)
             ▼
      [fct_proposta] ◄────── (1:1) ──────► [fct_convenio]
        │    │    │                               │
        │    │    └────────────┐                  │ (1:N)
        │    │                 │                  ▼
        │    │                 │      [fct_convenio_saldo_observacao]
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

## 2. Contratos das Entidades Fato

### 2.1. `gold.fct_proposta`
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
- **Joins Permitidos**:
  - Com `dim_proponente`, `dim_municipio`, `dim_orgao`, `dim_data` (1:1 ou N:1 conservam o grão).
  - Com `fct_convenio` via `id_proposta` (1:1 seguro, relação $0..1$ para $1$).
- **Joins Perigosos**:
  - **JOIN direto com `bridge_programa_proposta` sem agregação**: Multiplica linhas de 1.231 propostas e inflaciona valores financeiros em R$ 4,56 bilhões.
- **Fórmulas de Reconciliação com a Silver**:
  $$\text{COUNT}(fct\_proposta) = \text{COUNT}(silver.siconv\_proposta) = 1.157.619$$
  $$\sum valor\_global\_proposta = \sum silver.valor\_global\_proposta = \text{R\$ } 1.495.209.875.334,42$$
  $$\sum valor\_repasse\_proposta = \sum silver.valor\_repasse\_proposta = \text{R\$ } 1.425.758.135.735,63$$
  $$\sum valor\_contrapartida\_proposta = \sum silver.valor\_contrapartida\_proposta = \text{R\$ } 69.451.779.598,79$$
- **Limitações Analíticas**: Propostas sem convênio não possuem execução orçamentária (empenho/desembolso).

---

### 2.2. `gold.fct_convenio`
- **Status Contratual**: **`CONFIRMADO`** (Gate R4-A comprovado empiricamente)
- **Fonte Silver**: `silver.siconv_convenio`
- **Grão**: 1 linha por `numero_convenio` (instrumento jurídico canônico)
- **Chave Primária (PK)**: `numero_convenio`
- **Chaves Estrangeiras (FKs)**:
  - `id_proposta` $\to$ `fct_proposta.id_proposta` (relação $1:1$ estrita comprovada no snapshot)
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
- **Atributos Degenerados**: `situacao_convenio`, `subsituacao_convenio`, `situacao_publicacao`, `situacao_contratacao`, `is_instrumento_ativo`, `is_opera_obtv`, `is_assinado`, `numero_processo`, `unidade_gestora_emitente`
- **Medida Expressamente Excluída desta Fato**:
  - `valor_saldo_conta` (**proibido** nesta entidade devido à divergência de observações).
- **Joins Permitidos**:
  - Com `fct_proposta` via `id_proposta` (join 1:1 exato).
  - Com `dim_data` via chaves de datas.
  - Com `fct_convenio_saldo_observacao` via `numero_convenio` (relação $1:N$).
- **Joins Perigosos**:
  - Join com `bridge_programa_proposta` via `id_proposta` (inflaciona em R$ 1,50 bilhão se não pré-agregado).
- **Fórmulas de Reconciliação com a Silver**:
  $$\text{COUNT}(fct\_convenio) = \text{COUNT(DISTINCT } numero\_convenio) = 287.584$$
  $$\sum valor\_global\_convenio = \text{R\$ } 356.840.744.533,16$$
  $$\sum valor\_repasse\_convenio = \text{R\$ } 331.271.766.468,34$$
  $$\sum valor\_contrapartida\_convenio = \text{R\$ } 23.709.764.114,58$$
  $$\sum valor\_empenhado\_convenio = \text{R\$ } 192.064.543.060,79$$
  $$\sum valor\_desembolsado\_convenio = \text{R\$ } 153.205.012.397,29$$
  $$\sum valor\_saldo\_remanescente\_tesouro = \text{R\$ } 12.223.173.437,40$$
  $$\sum valor\_saldo\_remanescente\_convenente = \text{R\$ } 1.100.083.974,34$$
  $$\sum valor\_rendimento\_aplicacao = \text{R\$ } 1.859.253.950,24$$
  $$\sum valor\_ingresso\_contrapartida = \text{R\$ } 13.358.461.503,52$$
  $$\sum valor\_global\_original\_convenio = \text{R\$ } 89.451.880.983,68$$
  *(Nota: A reconciliação do valor global difere da soma bruta de 287.586 linhas em exatamente R$ 15.891.971,71, que corresponde à duplicação física dos 2 convênios conflitantes na Silver).*
- **Estratégia de Consolidação no R4-B**:
  ```sql
  SELECT DISTINCT
      numero_convenio,
      id_proposta,
      <32 colunas funcionais estaveis>
  FROM silver.siconv_convenio
  ```

---

### 2.3. `gold.fct_convenio_saldo_observacao`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_convenio`
- **Grão**: 1 linha por observação física (`id_convenio_observacao`)
- **Chave Primária (PK)**: `id_convenio_observacao`
- **Chave Estrangeira (FK)**:
  - `numero_convenio` $\to$ `fct_convenio.numero_convenio`
- **Medida Observacional**:
  - `valor_saldo_conta` (DECIMAL(17,2))
- **Atributos de Auditoria**:
  - `has_source_conflict` (BOOLEAN)
  - `source_conflict_count` (INT)
  - `__ingested_at_utc`, `__source_file`
- **Reconciliação**:
  $$\text{COUNT}(fct\_convenio\_saldo\_observacao) = 287.586$$
  $$\sum valor\_saldo\_conta = \text{R\$ } 18.079.072.724,97$$
- **Limitações**: Medida de natureza instantânea de extrato bancário. Não deve ser somada livremente com medidas orçamentárias de fluxo.

---

### 2.4. `gold.bridge_programa_proposta`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_programa_proposta`
- **Grão**: 1 linha por vínculo `id_programa` $\times$ `id_proposta`
- **Chave Primária Composta (PK)**: `(id_programa, id_proposta)`
- **Chaves Estrangeiras (FKs)**:
  - `id_programa` $\to$ `dim_programa.id_programa`
  - `id_proposta` $\to$ `fct_proposta.id_proposta`
- **Medidas Financeiras**: **NENHUMA** (expressamente proibido carregar valores monetários na ponte).
- **Atributos de Ponderação / Alocação**: **NENHUM** (alocação proporcional foi rejeitada).
- **Reconciliação**:
  $$\text{COUNT}(bridge\_programa\_proposta) = 1.158.975$$
- **Limitações e Regras de Consulta**:
  - Permite responder: "Quais programas apoiam a proposta X?" e "Quais propostas foram apresentadas ao programa Y?".
  - **Proibido**: Usar `SUM(fct_proposta.valor_global_proposta)` agrupando por `id_programa` sem agrupamento ou DISTINCT de proposta.

---

## 3. Contratos das Entidades Dimensão

### 3.1. `gold.dim_proponente`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_proposta`
- **Grão**: 1 linha por `identificacao_proponente` (CNPJ / CPF)
- **Chave Primária (PK)**: `identificacao_proponente`
- **Atributos Cadastrais**:
  - `nome_proponente`
  - `codigo_municipio_ibge`
  - `municipio_proponente`
  - `uf_proponente`
  - `cep_proponente`
  - `endereco_proponente`
  - `bairro_proponente`
  - `natureza_juridica`
- **Reconciliação**:
  $$\text{COUNT}(dim\_proponente) = 29.325$$
- **Tipo de SCD**: Tipo 1 no R4 inicial.

---

### 3.2. `gold.dim_municipio`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_proposta`
- **Grão**: 1 linha por `codigo_municipio_ibge`
- **Chave Primária (PK)**: `codigo_municipio_ibge` (7 dígitos)
- **Atributos**:
  - `nome_municipio`
  - `sigla_uf`
- **Reconciliação**:
  $$\text{COUNT}(dim\_municipio) = 5.570$$

---

### 3.3. `gold.dim_orgao`
- **Status Contratual**: **`CONFIRMADO`** (Dimensão Única Conformada)
- **Fonte Silver**: `silver.siconv_proposta` e `silver.siconv_programa_cadastral`
- **Grão**: 1 linha por `codigo_orgao`
- **Chave Primária (PK)**: `codigo_orgao`
- **Atributos**:
  - `descricao_orgao`
  - `is_orgao_superior` (BOOLEAN, indica se atua como superior)
  - `is_orgao_concedente` (BOOLEAN, indica se atua como concedente)
- **Reconciliação**:
  $$\text{COUNT}(dim\_orgao) = 184\text{ órgãos únicos}$$
  *(182 em proposta + 2 exclusivos de programas cadastrais sem proposta no snapshot).*

---

### 3.4. `gold.dim_programa`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte Silver**: `silver.siconv_programa_cadastral`
- **Grão**: 1 linha por `id_programa`
- **Chave Primária (PK)**: `id_programa`
- **Chave Estrangeira (FK)**:
  - `codigo_orgao_superior_programa` $\to$ `dim_orgao.codigo_orgao`
  - `data_disponibilizacao_sk` $\to$ `dim_data.data_sk`
- **Atributos**: `codigo_programa`, `nome_programa`, `situacao_programa`, `ano_disponibilizacao`
- **Reconciliação**:
  $$\text{COUNT}(dim\_programa) = 53.018$$

---

### 3.5. `gold.dim_data`
- **Status Contratual**: **`CONFIRMADO`**
- **Fonte**: Gerada programaticamente (dimensão calendário autônoma)
- **Grão**: 1 linha por dia civil
- **Chave Primária (PK)**: `data_sk` (INT: `YYYYMMDD`)
- **Domínio Válido**: `1990-01-01` a `2050-12-31` (22.280 linhas)
- **Registros Sentinela Obrigatórios**:
  - `-1`: `Data Não Informada / NULL`
  - `-2`: `Data Inválida / Fora do Domínio Analítico (<1990 ou >2050)`
- **Atributos**: `data`, `ano`, `mes`, `dia`, `trimestre`, `semestre`, `dia_da_semana`, `nome_mes`, `is_fim_de_semana`

---

## 4. Entidades Não Aprovadas ou Rejeitadas

### 4.1. `dim_proponente_perfil`
- **Status**: **`NÃO RECOMENDADO / DESCARTADO NESTE MARCO`**
- **Motivo**: O profiling comprovou que 100% dos proponentes possuem cadastro idêntico em todas as propostas no snapshot ativo. Criar uma entidade intermediária de perfil acrescentaria complexidade relacional sem ganho analítico concreto.

### 4.2. `fct_convenio_alocacao_proporcional`
- **Status**: **`NÃO APROVADO / EXPRESSAMENTE REJEITADO`**
- **Motivo**: Dividir o valor financeiro da proposta pelo número de programas vinculados ($V / N$) inventa quotas contábeis fictícias sem respaldo regulatório ou contratual na fonte.
