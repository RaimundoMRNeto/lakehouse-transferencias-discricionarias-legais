# R4-A — Diagnóstico Analítico, Profiling Dirigido e Evidências Empíricas da Camada Gold

## 1. Missão e Princípios de Governança da Camada Gold

O marco **R4-A** tem como objetivo estabelecer as bases analíticas, empíricas e contratuais da futura camada Gold do Lakehouse de Transferências Discricionárias e Legais da União (Transferegov).

O princípio regente da camada Gold é:
> **"A camada Silver preserva e qualifica os dados da fonte original. A camada Gold organiza esses dados para consumo analítico eficiente e intuitivo, sem alterar significativamente os significados de negócio, sem multiplicar medidas por expansão cartesiana de relacionamentos e sem ocultar ambiguidades operacionais."**

Uma consulta SQL tecnicamente possível não implica que seja analiticamente válida. O papel deste diagnóstico é investigar os limites empíricos dos grãos, dependências funcionais, integridade referencial, cardinalidade e comportamento temporal das entidades Silver para fundamentar as decisões de engenharia e modelagem dimensional.

### 1.1. Natureza Estritamente Read-Only
Esta etapa foi executada sob estrita restrição de leitura:
- Nenhuma tabela, view ou banco de dados Gold foi criado.
- Nenhum dado foi gravado em `s3a://gold/`.
- Nenhuma alteração foi efetuada nas camadas Bronze e Silver ou nos metadados de ingestão.
- Todo o profiling foi conduzido dinamicamente via script modular dedicado (`spark/profile_r4a.py`).

---

## 2. Snapshot de Referência da Camada Silver

O diagnóstico operou exclusivamente sobre as 5 entidades da camada Silver geradas a partir do snapshot ativo de referência da camada Bronze.

- **Data/Hora de Execução do Profiling R4-A**: `2026-09-17T14:11:32.347868+00:00`
- **Catálogo de Bancos no Hive/Spark**: `['bronze', 'default', 'silver']`
- **Existência do Schema `gold` no Catálogo**: `False` (banco inexistente, confirmando conformidade)

### Tabela 2.1 — Metadados de Snapshot das Entidades Silver Auditadas

| Entidade Silver | Linhas | Ingestion Runs | Run ID Ativo | SHA-256 da Fonte | Timestamp Ingestão (UTC) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `silver.siconv_proposta` | 1.157.619 | 1 | `run_20260916T182321` | `a60687d706c4c17b95cf400485a9c48b2ec2ebd1bfd07f9463fca4b40e6d3e27` | `2026-09-16T18:30:39.993116` |
| `silver.siconv_programa_cadastral` | 53.018 | 1 | `run_20260916T182321` | `617c9b75de5aa5c03e97fb90c6f34b175e3dabbd532e715c2889e19b1607f45c` | `2026-09-16T18:25:45.596562` |
| `silver.siconv_programa_elegibilidade` | 1.257.350 | 1 | `run_20260916T182321` | `617c9b75de5aa5c03e97fb90c6f34b175e3dabbd532e715c2889e19b1607f45c` | `2026-09-16T18:25:45.596562` |
| `silver.siconv_programa_proposta` | 1.158.975 | 1 | `run_20260916T182321` | `f8723df70f84c0489f7e3db2302106e8ff0ef7015d36e28e8a33c83923077006` | `2026-09-16T18:27:44.257753` |
| `silver.siconv_convenio` | 287.586 | 1 | `run_20260916T182321` | `4ba907602d1f6eb47399b5911565b53ce849bf34086835ff80744ce7e3d2a6ab` | `2026-09-16T18:33:25.679585` |

---

## 3. Finding A — Dimensão Proponente

### 3.1. Fato Observado
A fonte primária de proponentes no snapshot é a tabela `silver.siconv_proposta` (1.157.619 propostas).
- **Proponentes distintos (`identificacao_proponente`)**: **29.325**
- **Valores NULL de `identificacao_proponente`**: **0** (100% de preenchimento)

Foi avaliada a dependência funcional (FD) estrita de `identificacao_proponente` sobre os atributos cadastrais presentes em cada proposta, tratando NULL como um estado observacional explícito via expressão:
$$\text{Distinct Vals} = \text{COUNT(DISTINCT attr)} + \max(\mathbb{I}(\text{attr IS NULL}))$$

#### Tabela 3.1 — Dependência Funcional dos Atributos de Proponente
| Atributo Analisado | Total Proponentes | Exatamente 1 Valor | > 1 Valor | % Variação | Máx. Distintos | NULLs na Tabela |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `nome_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `codigo_municipio_ibge` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `municipio_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `uf_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `cep_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `endereco_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `bairro_proponente` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |
| `natureza_juridica` | 29.325 | 29.325 | 0 | 0,0000% | 1 | 0 |

#### Teste da Tupla Cadastral Completa
Foi testada a tupla `(nome, ibge, município, UF, CEP, endereço, bairro, natureza jurídica)`:
- Proponentes com exatamente 1 tupla cadastral: **29.325 (100,00%)**
- Proponentes com mais de 1 tupla cadastral: **0 (0,00%)**
- Máximo de tuplas por proponente: **1**

### 3.2. Interpretação
No snapshot corrente, a base Transferegov apresenta dados cadastrais do proponente desnormalizados na tabela de propostas de forma completamente estável. Não há discrepância de grafia, mudança de endereço, alteração de CEP ou duplicidade cadastral vinculada ao mesmo CNPJ/CPF (`identificacao_proponente`).
Cada `identificacao_proponente` atua como chave candidata natural para uma dimensão com grão de uma linha por proponente.

### 3.3. Decisão Candidata
- **Classificação**: **`CONFIRMADA`**
- **Entidade Recomendada**: `gold.dim_proponente`
- **Grão**: 1 linha por `identificacao_proponente`
- **Nota Metodológica**: Embora os dados do snapshot atual sustentem a estabilidade 1:1, a modelagem futura deve ter ciência de que o snapshot reflete o estado atual dos proponentes no sistema. Não é necessário implementar nesta fase a entidade complexa `dim_proponente_perfil`.

---

## 4. Finding B — Dimensão Município

### 4.1. Fato Observado
Foi avaliada a dependência funcional em `silver.siconv_proposta` a partir de `codigo_municipio_ibge`:
- $\text{codigo\_municipio\_ibge} \to \text{municipio\_proponente}$
- $\text{codigo\_municipio\_ibge} \to \text{uf\_proponente}$
- $\text{codigo\_municipio\_ibge} \to (\text{municipio\_proponente}, \text{uf\_proponente})$

#### Resultados Empíricos:
- **Códigos IBGE distintos observados**: **5.570** (cobertura total dos municípios brasileiros cadastrados)
- **Códigos com `codigo_municipio_ibge IS NULL`**: **0**
- **Códigos com exatamente 1 nome de município**: **5.570 (100,00%)**
- **Códigos com múltiplos nomes de município**: **0 (0,00%)** (máximo: 1)
- **Códigos com exatamente 1 UF**: **5.570 (100,00%)**
- **Códigos com múltiplas UFs**: **0 (0,00%)** (máximo: 1)
- **Códigos com exatamente 1 tupla `(município, UF)`**: **5.570 (100,00%)**
- **Códigos com múltiplas tuplas**: **0 (0,00%)** (máximo: 1)

### 4.2. Interpretação
Não há anomalias de codificação, colisões de códigos IBGE entre municípios diferentes, nem inconsistências federativas de UF. A relação funcional é estritamente $1:1$ de `codigo_municipio_ibge` para `(municipio_proponente, uf_proponente)`.

### 4.3. Decisão Candidata
- **Classificação**: **`CONFIRMADA`**
- **Entidade Recomendada**: `gold.dim_municipio`
- **Grão**: 1 linha por `codigo_municipio_ibge` (7 dígitos)
- **Chave Primária**: `codigo_municipio_ibge`
- **Atributos Canônicos**: `nome_municipio`, `sigla_uf`

---

## 5. Finding C — Dimensão Órgão e Conformação de Papéis

### 5.1. Fato Observado
O modelo de dados original do SICONV/Transferegov contém referências a órgãos em múltiplos papéis administrativos:
1. **Órgão Superior da Proposta**: `codigo_orgao_superior` / `descricao_orgao_superior` em `silver.siconv_proposta`
2. **Órgão Concedente (Subordinado/Executor) da Proposta**: `codigo_orgao` / `descricao_orgao` em `silver.siconv_proposta`
3. **Órgão Superior do Programa**: `codigo_orgao_superior_programa` / `descricao_orgao_superior_programa` em `silver.siconv_programa_cadastral`

#### Teste de Dependência Funcional Local:
- `codigo_orgao_superior -> descricao_orgao_superior`: 37 códigos distintos, todos com exatamente 1 descrição (0 conflitos).
- `codigo_orgao -> descricao_orgao`: 182 códigos distintos, todos com exatamente 1 descrição (0 conflitos).
- `codigo_orgao_superior_programa -> descricao_orgao_superior_programa`: 39 códigos distintos, todos com exatamente 1 descrição (0 conflitos).

#### Análise Intersetorial de Universos de Códigos:
- Códigos em Órgão Superior (Proposta): **37**
- Códigos em Órgão Concedente (Proposta): **182**
- Códigos em Órgão Superior (Programa): **39**
- Interseção Órgão Superior $\cap$ Órgão Concedente (Proposta): **37** (100% dos superiores constam como concedentes)
- Interseção Órgão Superior (Proposta) $\cap$ Órgão Superior (Programa): **37** (os 37 de proposta estão contidos nos 39 de programa; programa possui 2 órgãos adicionais que nunca emitiram propostas no snapshot)
- **Divergência de Descrições para o mesmo Código**: **0 ocorrências** (quando um código aparece como superior e concedente, ou em programa e proposta, sua descrição textual é rigorosamente idêntica).

### 5.2. Interpretação
Os códigos de órgãos do governo federal utilizam o mesmo plano de codificação SIORG (Sistema de Informações Organizacionais do Governo Federal). Não existe colisão semântica entre os papéis: um código representa a mesma entidade administrativa quer ela atue como ministério supervisor (órgão superior) ou como unidade setorial concedente (órgão concedente).

### 5.3. Decisão Candidata
- **Classificação**: **`CONFIRMADA`**
- **Arquitetura Recomendada**: **Dimensão Única Conformada (`gold.dim_orgao`) com Role-Playing**.
- **Grão**: 1 linha por `codigo_orgao`.
- **Papéis no Modelo Dimensional**:
  - `dim_orgao` desempenha o papel de `orgao_superior` e de `orgao_concedente` nas fatos via foreign keys role-playing (`id_orgao_superior_sk`, `id_orgao_concedente_sk`).
  - Não há necessidade de criar tabelas físicas separadas (`dim_orgao_superior` e `dim_orgao_concedente`), evitando duplicação desnecessária de armazenamento e catálogo.

---

## 6. Finding D — Consolidação Analítica de Convênio

Este é o gate analítico mais crítico do R4-A.

### 6.1. Fato Observado
Na camada `silver.siconv_convenio`:
- **Total de linhas físicas (observações)**: **287.586**
- **Total de números de convênio distintos (`numero_convenio`)**: **287.584**
- **Chaves com multiplicidade > 1**: Exatamente **2** números de convênio (`949286` e `956078`), totalizando 4 linhas (2 linhas cada).

Foi executada auditoria exaustiva e NULL-aware sobre **todas as 34 colunas analíticas e de negócio** da tabela Silver para verificar a estabilidade de cada coluna entre as observações duplicadas.

#### Tabela 6.1 — Estabilidade de Colunas em `silver.siconv_convenio`
| Grupo de Atributos | Colunas Auditadas | Classificação | Variação Observada |
| :--- | :--- | :---: | :---: |
| **Identificadores e Chaves** | `numero_convenio`, `id_proposta`, `numero_processo`, `unidade_gestora_emitente` | **ESTÁVEL POR CONVÊNIO** | 0 convênios (0,00%) |
| **Status e Flags de Operação** | `situacao_convenio`, `subsituacao_convenio`, `situacao_publicacao`, `situacao_contratacao`, `is_instrumento_ativo`, `is_opera_obtv`, `is_assinado`, `is_foto`, `motivo_suspensao` | **ESTÁVEL POR CONVÊNIO** | 0 convênios (0,00%) |
| **Datas de Ciclo de Vida** | `data_assinatura_convenio`, `data_publicacao_convenio`, `data_inicio_vigencia_convenio`, `data_fim_vigencia_convenio`, `data_fim_vigencia_original`, `data_limite_prestacao_contas`, `data_suspensiva`, `data_retirada_suspensiva`, `dia_assinatura`, `mes_assinatura`, `ano_assinatura`, `dias_prestacao_contas`, `dias_clausula_suspensiva` | **ESTÁVEL POR CONVÊNIO** | 0 convênios (0,00%) |
| **Contadores de Alterações** | `quantidade_convenios`, `quantidade_termos_aditivos`, `quantidade_prorrogacoes` | **ESTÁVEL POR CONVÊNIO** | 0 convênios (0,00%) |
| **Medidas Financeiras Canônicas** | `valor_global_convenio`, `valor_repasse_convenio`, `valor_contrapartida_convenio`, `valor_empenhado_convenio`, `valor_desembolsado_convenio`, `valor_saldo_remanescente_tesouro`, `valor_saldo_remanescente_convenente`, `valor_rendimento_aplicacao`, `valor_ingresso_contrapartida`, `valor_global_original_convenio` | **ESTÁVEL POR CONVÊNIO** | 0 convênios (0,00%) |
| **Saldo Bancário Instantâneo** | `valor_saldo_conta` | **VARIÁVEL POR OBSERVAÇÃO** | **2 convênios (100% dos conflitos)** |

#### Detalhe dos Conflitos em `valor_saldo_conta`:
- Convênio `949286`: Linha 1 = R$ 9.915,04 \| Linha 2 = R$ 9.918,59 (diferença de R$ 3,55).
- Convênio `956078`: Linha 1 = R$ 3.164.224,05 \| Linha 2 = R$ 3.165.629,92 (diferença de R$ 1.405,87).
- Todas as demais 33 colunas analíticas são rigorosamente idênticas entre as linhas 1 e 2 de ambos os instrumentos.

### 6.2. Interpretação
A duplicidade observada em `siconv_convenio` na fonte original (e preservada na Silver com rastreabilidade intacta) decorre exclusivamente de leituras bancárias instantâneas conflitantes (`valor_saldo_conta`), provavelmente geradas por microatualizações de extrato durante o processo de extração operacional da base Transferegov.
Todos os parâmetros jurídicos, orçamentários, contratuais e financeiros oficiais do convênio são **100% estáveis por `numero_convenio`**.

### 6.3. Decisão Candidata e Gate da Fato Convênio
- **Gate para `gold.fct_convenio`**: **`APROVADO / CONFIRMADO`**
- **Arquitetura de Duas Camadas para Convênios**:
  1. `gold.fct_convenio`: Grão analítico consolidado de **1 linha por `numero_convenio`** (287.584 linhas). Conterá todos os atributos e medidas canônicas estáveis, **excluindo expressamente** a coluna ambígua `valor_saldo_conta`.
  2. `gold.fct_convenio_saldo_observacao`: Entidade observacional de grão fino com **1 linha por `id_convenio_observacao`** (287.586 linhas), preservando integralmente todas as leituras de saldo de conta, metadados de auditoria e flags de conflito (`has_source_conflict`).
- **Estratégia Futura de Consolidação**: Como os 33 atributos são estáveis, a materialização da fato canônica poderá utilizar `SELECT DISTINCT numero_convenio, <colunas_estaveis>`, sem recorrer a funções heurísticas perigosas (`ROW_NUMBER`, `FIRST`, `MAX` arbitrário) para ocultar divergências.

---

## 7. Finding E — Relação N:N Programa × Proposta e Risco Financeiro

### 7.1. Fato Observado
A cardinalidade entre Programas e Propostas é comprovadamente muitos-para-muitos ($N:N$):
- Propostas sem nenhum programa vinculado (`prog_cnt = 0`): **78 propostas** (órfãs na ponte)
- Propostas com exatamente 1 programa vinculado (`prog_cnt = 1`): **1.156.310 propostas**
- Propostas com mais de 1 programa vinculado (`prog_cnt > 1`): **1.231 propostas**
- Máximo de programas associados a uma única proposta: **16 programas**

#### Tabela 7.1 — Distribuição Empírica de Programas por Proposta
| Qtd. Programas (`prog_cnt`) | Qtd. Propostas | % Propostas | Valor Único Propostas (R$) | Valor se Inflado Direto (R$) | Fator |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 0 | 78 | 0,0067% | 15.422.243.023,50 | 0,00 | 0,00x |
| 1 | 1.156.310 | 99,8870% | 1.476.359.806.633,98 | 1.476.359.806.633,98 | 1,00x |
| 2 | 1.106 | 0,0955% | 2.692.790.489,46 | 5.385.580.978,92 | 2,00x |
| 3 | 94 | 0,0081% | 575.034.977,32 | 1.725.104.931,96 | 3,00x |
| 4 | 16 | 0,0014% | 42.776.655,91 | 171.106.623,64 | 4,00x |
| 5 | 7 | 0,0006% | 22.381.484,25 | 111.907.421,25 | 5,00x |
| 6 | 4 | 0,0003% | 89.172.070,00 | 535.032.420,00 | 6,00x |
| 7 | 1 | 0,0001% | 600.000,00 | 4.200.000,00 | 7,00x |
| 8 | 1 | 0,0001% | 2.040.000,00 | 16.320.000,00 | 8,00x |
| 14 | 1 | 0,0001% | 1.400.000,00 | 19.600.000,00 | 14,00x |
| 16 | 1 | 0,0001% | 1.630.000,00 | 26.080.000,00 | 16,00x |
| **Total c/ Programa (>=1)** | **1.157.541** | **99,9933%** | **1.479.787.632.310,92** | **1.484.354.739.009,75** | **1,003086x** |

#### Impacto Financeiro em Convênios Vinculados:
Todos os convênios estão vinculados a pelo menos 1 programa via proposta:
- Convênios com 1 programa: **287.239** (R$ 355.656.130.101,98)
- Convênios com 2 programas: **307** (R$ 882.635.315,91 único vs R$ 1.765.270.631,82 inflado)
- Convênios com 3 programas: **33** (R$ 288.293.459,21 único vs R$ 864.880.377,63 inflado)
- Convênios com 4 programas: **5** (R$ 13.685.656,06 único vs R$ 54.742.624,24 inflado)
- **Total Único Conveniado com Programa**: R$ 356.840.744.533,16
- **Total Inflado por Join Direto na Ponte**: R$ 358.341.023.735,67
- **Diferença Artificial de Inflação**: **+R$ 1.500.279.202,51 (+R$ 1,50 bilhão)**

### 7.2. Interpretação
Um join direto entre a fato de propostas (ou convênios) e a ponte de programas multiplica as linhas das entidades associadas a múltiplos programas.
Se um analista executar `SUM(valor_global_proposta)` agrupado por programa e depois somar o resultado de todos os programas, o total será **R$ 4.567.106.698,83 (R$ 4,56 bilhões) maior** que o valor real total das propostas!
No caso de convênios, a distorção introduz **R$ 1,50 bilhão de duplicação financeira**.

Isso **NÃO é um defeito de software nem erro no SQL**: é uma consequência matemática intrínseca da cardinalidade $N:N$.

### 7.3. Contrato Analítico por Programa
1. **Métricas Permitidas**:
   - `COUNT(DISTINCT id_proposta)` por programa: representa fidedignamente a quantidade de propostas atendidas pelo programa.
2. **Métricas de Atenção**:
   - `COUNT(DISTINCT id_convenio)` por programa: representa instrumentos associados, mas alerta obrigatório de não-aditividade na soma de programas.
3. **Métricas Proibidas / Bloqueadas como Aditivas**:
   - `SUM(valor_global_proposta)` ou `SUM(valor_global_convenio)` segmentadas diretamente por programa.
4. **Política de Alocação Proporcional**:
   - **`NÃO APROVADA`**. Não dividir artificialmente os valores das propostas ($V / N$) nesta etapa, pois o sistema de origem não define quotas de recursos entre os programas vinculados.

---

## 8. Finding F — Dimensão Data e Profiling Temporal

### 8.1. Fato Observado
Foram mapeados e auditados todos os 19 campos temporais (`DATE`) da camada Silver.

#### Tabela 8.1 — Profiling Temporal das Entidades Silver
| Entidade / Campo | Total Linhas | NULLs (%) | Data Mínima | Data Máxima | Anos Dist. | < 1990 | > 2050 | Exemplos Outliers |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`proposta.data_proposta`** | 1.157.619 | 0 (0,0%) | 2008-01-01 | 2026-09-15 | 19 | 0 | 0 | *Nenhum* |
| `proposta.data_inicio_vigencia_proposta` | 1.157.619 | 3 (0,0%) | 0011-06-20 | 2028-07-03 | 36 | 10 | 0 | `0011-06-20`, `0020-05-30`, `1111-01-01` |
| `proposta.data_fim_vigencia_proposta` | 1.157.619 | 3 (0,0%) | 0001-01-01 | 3012-05-30 | 37 | 1 | 3 | `0001-01-01`, `2122-07-20`, `3012-05-30` |
| `convenio.data_assinatura_convenio` | 287.586 | 50.024 (17,4%) | 2008-09-15 | 2026-09-15 | 19 | 0 | 0 | *Nenhum* |
| `convenio.data_publicacao_convenio` | 287.586 | 50.426 (17,5%) | 2001-01-11 | 2026-09-15 | 21 | 0 | 0 | *Nenhum* |
| `convenio.data_inicio_vigencia_convenio` | 287.586 | 0 (0,0%) | 1900-04-15 | 2027-11-26 | 23 | 1 | 0 | `1900-04-15` |
| `convenio.data_fim_vigencia_convenio` | 287.586 | 0 (0,0%) | 2008-10-12 | 2035-12-19 | 26 | 0 | 0 | *Nenhum* |
| `convenio.data_fim_vigencia_original` | 287.586 | 0 (0,0%) | 2008-10-12 | 2035-12-19 | 26 | 0 | 0 | *Nenhum* |
| `convenio.data_limite_prestacao_contas` | 287.586 | 0 (0,0%) | 2008-10-31 | 2036-01-18 | 27 | 0 | 0 | *Nenhum* |
| `convenio.data_suspensiva` | 287.586 | 267.907 (93,2%)| 0010-03-30 | 2029-12-31 | 22 | 4 | 0 | `0010-03-30`, `0011-09-26` |
| `convenio.data_retirada_suspensiva` | 287.586 | 161.887 (56,3%)| 2010-04-20 | 2026-09-15 | 17 | 0 | 0 | *Nenhum* |
| `programa_cad.data_disponibilizacao` | 53.018 | 3.138 (5,9%) | 2008-06-18 | 2026-09-15 | 19 | 0 | 0 | *Nenhum* |
| `elegibilidade.data_disponibilizacao` | 1.257.350 | 126.909 (10,1%)| 2008-06-18 | 2026-09-15 | 19 | 0 | 0 | *Nenhum* |
| `elegibilidade.data_inicio_recebimento` | 1.257.350 | 669.409 (53,2%)| 0006-03-11 | 2026-09-14 | 25 | 117 | 0 | `0006-03-11`, `0008-08-06`, `0209-06-11` |
| `elegibilidade.data_fim_recebimento` | 1.257.350 | 669.409 (53,2%)| 2007-12-31 | 5008-12-15 | 21 | 0 | 1 | `5008-12-15` |
| `elegibilidade.data_inicio_emenda` | 1.257.350 | 828.377 (65,9%)| 2009-01-01 | 2026-09-14 | 18 | 0 | 0 | *Nenhum* |
| `elegibilidade.data_fim_emenda` | 1.257.350 | 828.433 (65,9%)| 2010-04-30 | 2026-12-31 | 17 | 0 | 0 | *Nenhum* |
| `elegibilidade.data_inicio_beneficiario` | 1.257.350 | 966.696 (76,9%)| 2008-06-27 | 2026-09-16 | 19 | 0 | 0 | *Nenhum* |
| `elegibilidade.data_fim_beneficiario` | 1.257.350 | 966.696 (76,9%)| 2009-12-31 | 2026-12-31 | 18 | 0 | 0 | *Nenhum* |

### 8.2. Interpretação
As datas nucleares de transação (`data_proposta`, `data_assinatura_convenio`, `data_publicacao_convenio`) operam exclusivamente dentro do intervalo civil contemporâneo válido do SICONV (2001 a 2026).
No entanto, campos de vigência e janelas de programas sofrem de erros pontuais de digitação humana no sistema de origem, gerando anos anômalos como `0001`, `0011`, `1900` ou `5008`.

### 8.3. Contrato Analítico para `gold.dim_data`
- **Classificação**: **`CONFIRMADA`**
- **Grão**: 1 linha por dia de calendário.
- **Domínio Analítico Aprovado**: `1990-01-01` a `2050-12-31` (~22.280 registros). Não gerar uma dimensão de 5.000 anos para acolher erros pontuais da fonte.
- **Chave Primária**: `data_sk` em formato inteiro inteligente `YYYYMMDD` (ex: `20260916`).
- **Tratamento de Exceções (Chaves Sentinela)**:
  - `-1`: Data não informada / NULL na Silver.
  - `-2`: Data fora do domínio analítico válido (< 1990 ou > 2050).

---

## 9. Finding G — Matriz de Medidas e Regras de Aditividade

A classificação de aditividade determina como ferramentas de BI (Superset) e usuários SQL podem agregar cada métrica sem corromper as análises.

### 9.1. Distinção Crucial: Snapshot Corrente vs. Séries Temporais
O Lakehouse opera atualmente com **snapshot corrente qualificado**. Portanto, as medidas de convênio (ex: `valor_empenhado_convenio`) são **aditivas entre convênios no snapshot atual**, mas serão **semi-aditivas ao longo do tempo** caso snapshots históricos venham a ser acumulados no futuro.

### Tabela 9.1 — Matriz de Aditividade de Medidas

| Entidade | Medida de Negócio | Tipo de Dado | Aditividade por Entidade | Aditividade Temporal (Snapshot) | Aditividade عبر Programa (Bridge) | Regra de NULL | Classificação Contratual |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Proposta** | `quantidade_propostas` (constante 1) | INT | Aditiva | Aditiva | Não Aditiva | `0` se não houver | **ADITIVA** |
| **Proposta** | `valor_global_proposta` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Proposta** | `valor_repasse_proposta` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Proposta** | `valor_contrapartida_proposta` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `quantidade_convenios` (constante 1) | INT | Aditiva | Aditiva | Não Aditiva | `0` se não houver | **ADITIVA** |
| **Convênio** | `valor_global_convenio` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `valor_repasse_convenio` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `valor_contrapartida_convenio` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `valor_empenhado_convenio` | DECIMAL(17,2) | Aditiva | Semi-aditiva (fluxo acumulado) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `valor_desembolsado_convenio` | DECIMAL(17,2) | Aditiva | Semi-aditiva (fluxo acumulado) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `valor_saldo_remanescente_tesouro` | DECIMAL(17,2) | Aditiva | Semi-aditiva (saldo pontual) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `valor_saldo_remanescente_convenente`| DECIMAL(17,2) | Aditiva | Semi-aditiva (saldo pontual) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `valor_rendimento_aplicacao` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `valor_ingresso_contrapartida` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `valor_global_original_convenio` | DECIMAL(17,2) | Aditiva | Aditiva | Não Aditiva | COALESCE(val, 0) | **ADITIVA** |
| **Convênio** | `quantidade_termos_aditivos` | INT | Aditiva | Semi-aditiva (acumulado) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `quantidade_prorrogacoes` | INT | Aditiva | Semi-aditiva (acumulado) | Não Aditiva | COALESCE(val, 0) | **ADITIVA NO SNAPSHOT** |
| **Convênio** | `valor_saldo_conta` (observação) | DECIMAL(17,2) | Não Aditiva | Não Aditiva | Não Aditiva | Manter original | **NÃO ADITIVA / OBSERVACIONAL** |

---

## 10. Finding H — Linha de Base das Métricas e Indicadores Chave

Os valores abaixo foram calculados dinamicamente sobre o snapshot da Silver:

### 10.1. Métricas Base Canônicas
- **Quantidade de Propostas**: **1.157.619**
- **Valor Global Total das Propostas**: **R$ 1.495.209.875.334,42 (R$ 1,495 trilhão)**
- **Valor de Repasse das Propostas**: **R$ 1.425.758.135.735,63 (R$ 1,425 trilhão)**
- **Valor de Contrapartida das Propostas**: **R$ 69.451.779.598,79 (R$ 69,45 bilhões)**
- **Quantidade de Convênios Canônicos Distintos**: **287.584**
- **Valor Global dos Convênios (Canônico)**: **R$ 356.840.744.533,16 (R$ 356,84 bilhões)**
- **Valor de Repasse dos Convênios**: **R$ 331.271.766.468,34 (R$ 331,27 bilhões)**
- **Valor de Contrapartida dos Convênios**: **R$ 23.709.764.114,58 (R$ 23,71 bilhões)**
- **Valor Empenhado Total**: **R$ 192.064.543.060,79 (R$ 192,06 bilhões)**
- **Valor Desembolsado Total**: **R$ 153.205.012.397,29 (R$ 153,20 bilhões)**
- **Saldo Remanescente Tesouro**: **R$ 12.223.173.437,40**
- **Saldo Remanescente Convenente**: **R$ 1.100.083.974,34**
- **Rendimento de Aplicação Financeira**: **R$ 1.859.253.950,24**
- **Ingresso de Contrapartida**: **R$ 13.358.461.503,52**
- **Valor Global Original Conveniado**: **R$ 89.451.880.983,68**

### 10.2. Indicadores Derivados Aprovados
- **Taxa de Conveniação**:
  $$\text{Taxa} = \frac{\text{Propostas Conveniadas Distintas}}{\text{Total de Propostas Distintas}} = \frac{287.584}{1.157.619} = \mathbf{24,8427\%}$$
- **Ticket Médio por Proposta Apresentada**:
  $$\text{TM}_{\text{prop}} = \frac{\text{R\$ } 1.495.209.875.334,42}{1.157.619} = \mathbf{R\$\ 1.291.625,18}$$
- **Ticket Médio por Convênio Formalizado**:
  $$\text{TM}_{\text{conv}} = \frac{\text{R\$ } 356.840.744.533,16}{287.584} = \mathbf{R\$\ 1.240.822,66}$$
- **Percentual de Empenho sobre Repasse Conveniado**:
  $$\%_{\text{emp}} = \frac{\text{R\$ } 192.064.543.060,79}{\text{R\$ } 331.271.766.468,34} \times 100 = \mathbf{57,9779\%}$$
- **Percentual de Desembolso sobre Repasse Conveniado**:
  $$\%_{\text{des}} = \frac{\text{R\$ } 153.205.012.397,29}{\text{R\$ } 331.271.766.468,34} \times 100 = \mathbf{46,2475\%}$$

---

## 11. Finding I — Perguntas Analíticas do Primeiro Dashboard

Classificação de prontidão analítica para o Superset:

| Pergunta Analítica Priorizada | Classificação de Prontidão | Entidades Requeridas | Observações e Cuidados |
| :--- | :---: | :--- | :--- |
| **Quantas propostas foram apresentadas por ano?** | **SEGURA COM O CONTRATO ATUAL** | `fct_proposta`, `dim_data` | Agrupamento direto por `ano_proposta` ou `dim_data.ano`. |
| **Qual o valor global das propostas por UF?** | **SEGURA COM O CONTRATO ATUAL** | `fct_proposta`, `dim_municipio` / `dim_proponente` | Aditivo e consistente. |
| **Quantas propostas foram formalizadas em convênios?** | **SEGURA COM O CONTRATO ATUAL** | `fct_proposta`, `fct_convenio` | Indicador de conveniação via join 1:1 seguro por `id_proposta`. |
| **Qual o valor total conveniado por ano de assinatura?** | **SEGURA COM O CONTRATO ATUAL** | `fct_convenio`, `dim_data` | Uso de `data_assinatura_convenio`. |
| **Qual o valor empenhado e desembolsado por ministério?** | **SEGURA COM O CONTRATO ATUAL** | `fct_convenio`, `fct_proposta`, `dim_orgao` | Agrupamento por órgão superior concedente via role-playing. |
| **Quais municípios mais captam recursos federais?** | **SEGURA COM O CONTRATO ATUAL** | `fct_convenio`, `fct_proposta`, `dim_municipio` | Baseado no município do proponente beneficiário. |
| **Qual a distribuição de convênios por situação cadastral?** | **SEGURA COM O CONTRATO ATUAL** | `fct_convenio` | Dimensão degenerada `situacao_convenio` na própria fato. |
| **Quantas propostas estão vinculadas a cada programa?** | **SEGURA COM O CONTRATO ATUAL** | `dim_programa`, `bridge_programa_proposta` | Métrica permitida: `COUNT(DISTINCT id_proposta)`. |
| **Qual o valor financeiro alocado a cada programa?** | **EXIGE REGRA ANALÍTICA FUTURA** | `bridge_programa_proposta`, `fct_proposta` | **BLOQUEADO COMO SOMA DIRETA** devido ao fator de inflação N:N. |
| **Qual a evolução histórica do saldo bancário de convênios?** | **EXIGE REGRA ANALÍTICA FUTURA** | `fct_convenio_saldo_observacao` | Requer tratamento de séries temporais de extratos bancários. |

---

## 12. Finding J — Histórico, Snapshot e SCD

1. **Snapshot Corrente**: As tabelas Bronze e Silver representam o estado corrente das bases operacionais do Transferegov. Não constituem um log completo de mudanças (CDC).
2. **SCD Type 2**: Não é factível nem metodologicamente honesto criar dimensões com versionamento histórico completo (SCD Tipo 2) a partir de um snapshot corrente único.
3. **Estabilidade Cadastral**: O profiling comprovou que 100% dos proponentes possuem cadastro idêntico em todas as propostas no snapshot atual. Desta forma, a dimensão `dim_proponente` será modelada como SCD Tipo 1 (sobrescrevendo atributos cadastrais caso novas versões venham a ser ingeridas).

---

## 13. Finding K — Catálogo e Estrutura Física Candidata da Gold

- **Schema**: `gold`
- **Location**: `s3a://gold/warehouse`
- **Formato**: `Delta Lake`
- **Estratégia de Carga no R4 Inicial**: `Full Refresh / Replace` (o particionamento e estratégia incremental serão abordados após validação da estrutura dimensional completa).
- **Catálogo Hive/Spark**:
  - Consulta `SHOW DATABASES` atestou que `gold` ainda não existe.
  - A criação do schema será efetuada no marco R4-B via bootstrap seguro e idempotente.
