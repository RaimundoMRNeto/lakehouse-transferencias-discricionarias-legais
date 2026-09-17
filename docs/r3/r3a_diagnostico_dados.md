# R3-A — Relatório de Diagnóstico, Profiling e Descoberta de Dados (Camada Bronze)

**Data da Análise:** 2026-09-16 22:50 UTC
**Ambiente:** Lakehouse Transferegov (Docker / MinIO / Apache Spark 3.4.3 / Spark Thrift Server / Airflow / dbt)
**Branch:** `feat/r3-silver-qualidade-modelagem`
**Escopo:** Diagnóstico empírico read-only dos quatro datasets analíticos da camada Bronze.

---

## 1. Identificação do Snapshot Analisado

O profiling e todas as análises empíricas deste relatório foram executados diretamente sobre o snapshot ativo registrado e validado no Lakehouse:

| Metadado de Controle | Valor Observado | Evidência / Origem |
| :--- | :--- | :--- |
| **Última Execução Bronze Global SUCCESS** | `run_20260916T185220` | `bronze.ingestion_runs` (`status = 'SUCCESS'`) |
| **Última Execução Registrada (No-Change)** | `run_20260916T185859` | `bronze.ingestion_runs` (`status = 'NO_CHANGE'`, estado idêntico mantido) |
| **Data de Carga da Fonte Oficial (`data_carga`)** | `16/09/2026 06:32:11` | `source_data_carga_raw_final` em `ingestion_runs` |
| **SHA256 do Arquivo de Controle** | `266de077d72977104b4e157b2b3cfe1b6d1eb65846f1e7cd92bbfdb376898aad` | `control_zip_sha256` (`data_carga_siconv.csv.zip`) |
| **Total de Execuções Registradas** | `4` | `SELECT COUNT(*) FROM bronze.ingestion_runs` |
| **Total de Registros de Manifesto** | `12` | `SELECT COUNT(*) FROM bronze.ingestion_manifest` |
| **Data/Hora da Execução do Profiling** | `2026-09-16 22:37:27 a 22:51:17 UTC` | Duração de execução: 827,10s via Spark Thrift Server |

### Linhas Registradas por Dataset no Snapshot

| Dataset Bronze | Tabela Delta | Delta Version | Linhas Lógicas / Delta | Status |
| :--- | :--- | :--- | :--- | :--- |
| `siconv_programa` | `bronze.siconv_programa` | 0 | **1.257.350** | `SUCCESS` |
| `siconv_programa_proposta` | `bronze.siconv_programa_proposta` | 0 | **1.158.975** | `SUCCESS` |
| `siconv_proposta` | `bronze.siconv_proposta` | 0 | **1.157.619** | `SUCCESS` |
| `siconv_convenio` | `bronze.siconv_convenio` | 0 | **287.586** | `SUCCESS` |

---

## 2. Inventário Estrutural das Quatro Tabelas

Na camada Bronze, todos os campos provenientes dos arquivos CSV de origem foram ingeridos estritamente como tipo físico `string` (UTF-8), preservando fidelidade textual absoluta aos dados oficiais. Foram adicionadas apenas 4 colunas técnicas de governança e linhagem (`__ingested_at_utc`, `__ingestion_run_id`, `__source_file`, `__source_sha256`).

| Dataset Bronze | Total Colunas | Colunas Oficiais | Colunas Técnicas | Tipo Físico Bronze Predominante |
| :--- | :--- | :--- | :--- | :--- |
| `bronze.siconv_programa` | 24 | 20 | 4 | `string` (todas) |
| `bronze.siconv_programa_proposta` | 6 | 2 | 4 | `string` (todas) |
| `bronze.siconv_proposta` | 40 | 36 | 4 | `string` (todas) |
| `bronze.siconv_convenio` | 44 | 40 | 4 | `string` (todas) |

---

## 3. Descoberta Empírica do Grão e Unicidade de Chaves

A diretriz basilar do R3-A é: **NÃO PRESUMIR O MODELO DOS DADOS**. A investigação empírica revelou características cruciais sobre os grãos de cada dataset.

### 3.1. `siconv_programa`
- **Hipótese ingênua descartada:** `ID_PROGRAMA` NÃO é chave primária nem representa uma linha por programa.
- **Evidência Empírica:**
  - Total de linhas na tabela: **1.257.350**.
  - Total de valores distintos em `ID_PROGRAMA`: **53.018**.
  - Multiplicidade: Cada `ID_PROGRAMA` se repete em média 23,7 vezes, alcançando um máximo de **405 ocorrências** para um único `ID_PROGRAMA` (ex.: programas `45749`, `43096`, `43239`, `43108`, `14077`).
  - Inspeção dos atributos que variam para um mesmo `ID_PROGRAMA`: Os registros explodem pelas regras de habilitação/elegibilidade do programa por **Modalidade** (`MODALIDADE_PROGRAMA`), **Natureza Jurídica** (`NATUREZA_JURIDICA_PROGRAMA`), **UF** (`UF_PROGRAMA`) e **Ação Orçamentária** (`ACAO_ORCAMENTARIA`).
  - Exemplo: Programa `45749` aceita 3 modalidades, 5 naturezas jurídicas e 27 UFs ($3 \times 5 \times 27 = 405$ combinações).
  - Teste de Unicidade Composta: A chave composta `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA, ACAO_ORCAMENTARIA)` possui exatamente **1.257.350** valores distintos para 1.257.350 linhas (100% de unicidade com tratamento de nulos/GROUP BY, **zero colisões**, multiplicidade máxima 1). A divergência de 601 registros observada em contagens ingênuas com `COUNT(DISTINCT)` decorria exclusivamente do descarte de linhas com valores nulos em `MODALIDADE_PROGRAMA` (600 nulos) e `ACAO_ORCAMENTARIA` (1 nulo). Além disso, a chave com 4 colunas `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA)` também é estritamente única (zero colisões).
  - Teste de Linhas Fisicamente Idênticas: O `COUNT(DISTINCT)` sobre TODAS as 20 colunas oficiais retornou exatamente **1.257.350**. Ou seja, **não existe duplicidade física completa**. Cada linha é um registro legítimo de elegibilidade de abertura de programa.
- **Grão Comprovado:** Critério de elegibilidade e abertura regional/orçamentária do programa governamental.
- **Recomendação de Modelagem:** Na Silver, manter o dataset íntegro de elegibilidade (chave natural composta comprovada, gerando surrogate key SHA-256) e criar a dimensão confirmada `silver.siconv_programa_cadastral` (grão 1:1 com `ID_PROGRAMA`, 53.018 linhas). **Nunca deduplicar com `DISTINCT ID_PROGRAMA` arbitrariamente descartando as UFs e modalidades.**

### 3.2. `siconv_programa_proposta`
- **Evidência Empírica:**
  - Total de linhas: **1.158.975**.
  - Total de `ID_PROGRAMA` distintos na ponte: **36.321**.
  - Total de `ID_PROPOSTA` distintos na ponte: **1.157.544**.
  - Total de pares distintos `(ID_PROGRAMA, ID_PROPOSTA)`: **1.158.975**.
  - Nulos em `ID_PROGRAMA` ou `ID_PROPOSTA`: **0**.
- **Grão Comprovado:** Associação entre Programa e Proposta (tabela associativa/ponte).
- **Chave Comprovada:** Chave primária composta `(ID_PROGRAMA, ID_PROPOSTA)` é **100% ÚNICA** (zero duplicidades).
- **Cardinalidade Empírica:** Relação **N:N (Many-to-Many)**.
  - 1.156.313 propostas ligadas a exatamente 1 programa.
  - **1.231 propostas** ligadas a múltiplos programas (até 16 programas por proposta, gerando 1.431 vínculos adicionais).
  - Programas associados a de 1 até 25.419 propostas.

### 3.3. `siconv_proposta`
- **Evidência Empírica:**
  - Total de linhas: **1.157.619**.
  - Total de valores distintos em `ID_PROPOSTA`: **1.157.619** (100% único).
  - Total de nulos em `ID_PROPOSTA`: **0**.
  - Total de valores distintos em `NR_PROPOSTA`: **1.155.902** (**1.717 repetições**). `NR_PROPOSTA` NÃO é chave primária (é uma numeração gerada pelo proponente/órgão que pode colidir entre proponentes distintos ou exercícios fiscais).
- **Grão Comprovado:** Uma proposta de trabalho submetida no sistema.
- **Chave Comprovada:** `ID_PROPOSTA` é chave primária **única e não nula**.

### 3.4. `siconv_convenio`
- **Investigação de Nomes:** A tabela NÃO possui coluna `ID_CONVENIO`. A chave primária oficial é `NR_CONVENIO`.
- **Evidência Empírica:**
  - Total de linhas: **287.586**.
  - Total de `NR_CONVENIO` distintos: **287.584**.
  - Total de `ID_PROPOSTA` distintos: **287.584**.
  - Nulos em `NR_CONVENIO`: **0**.
  - Nulos em `ID_PROPOSTA`: **0**.
  - Duplicidade Observada: Exatamente **2 convênios** possuem 2 linhas registradas no snapshot bruto:
    - Convênio `949286` (Proposta `1900208`, assinado em 28/12/2023): Linhas idênticas em 39 das 40 colunas, divergindo apenas em `VL_SALDO_CONTA` (`9918.59` vs `9915.04`).
    - Convênio `956078` (Proposta `1961101`, assinado em 22/05/2024): Linhas idênticas em 39 das 40 colunas, divergindo apenas em `VL_SALDO_CONTA` (`3165629.92` vs `3164224.05`).
  - Associação Proposta ↔ Convênio: **Zero** propostas possuem múltiplos `NR_CONVENIO` distintos (`HAVING COUNT(DISTINCT NR_CONVENIO) > 1` = 0).
- **Grão Comprovado:** Instrumento de repasse/convênio formalizado.
- **Chave Comprovada:** `NR_CONVENIO` é a chave natural do convênio; a relação com `ID_PROPOSTA` é estritamente **1:0..1** (1 proposta pode gerar 0 ou 1 convênio). As duas duplicidades pontuais decorrem de leituras concorrentes/snapshots de saldo bancário na extração original do Transferegov.

---

## 4. Matriz de Integridade Referencial Global

Cruzamento exaustivo entre as chaves das 4 tabelas no snapshot ativo:

| Relação | Chave de Ligação | Registros Match | Sem Match Esquerda | Sem Match Direita | Cardinalidade Observada | Diagnóstico |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`programa_proposta` $\rightarrow$ `programa`** | `ID_PROGRAMA` | **36.321** (100,0%) | 0 (0,0%) | 16.697 (31,49%) | **N:1** | Integridade referencial íntegra da ponte para o programa. Os 16.697 programas sem match são chamadas/programas sem propostas submetidas. |
| **`programa_proposta` $\rightarrow$ `proposta`** | `ID_PROPOSTA` | **1.157.541** (99,9997%) | **3** (0,0003%) | **78** (0,0067%) | **N:1** | Quase perfeita. Existem exatamente 3 propostas órfãs na ponte (`321453`, `1427146`, `296629`) e 78 propostas cadastradas sem vínculo na ponte de programas. |
| **`convenio` $\rightarrow$ `proposta`** | `ID_PROPOSTA` | **287.584** (100,0%) | 0 (0,0%) | 870.035 (75,16%) | **1:1** (ou 1:0..1) | Integridade referencial de 100%. Todo convênio possui proposta cadastrada. 870.035 propostas não viraram convênio (75,16%). |

---

## 5. Simulação e Prova de Risco de Multiplicação em JOINs

> [!CAUTION]
> **GATE CRÍTICO:** Realizar JOIN achatado ingênuo entre as tabelas da Bronze provoca uma **catástrofe analítica e computacional**, expandindo os dados em até **84 vezes** e inflando os valores financeiros em **72 vezes**!

### 5.1. Simulação Empírica de Expansão de Linhas

Partindo da base de instrumentos de `bronze.siconv_convenio` (287.586 linhas):

| Etapa do JOIN | Linhas Resultantes | Fator de Multiplicação | Motivo Técnico da Variação |
| :--- | :--- | :--- | :--- |
| **1. Base `siconv_convenio`** | **287.586** | 1,0000x | Base original de instrumentos |
| **2. INNER JOIN `siconv_proposta`** | **287.586** | 1,0000x | Cada convênio aponta para exatamente 1 proposta única |
| **3. INNER JOIN `siconv_programa_proposta`** | **287.974** | **1,0013x** | Pequena expansão (+388 linhas) decorrente das propostas que participam de mais de um programa na ponte N:N |
| **4. INNER JOIN `siconv_programa` (Raw Bronze)** | **24.243.823** | **84,3011x** | **EXPANSÃO EXPLOSIVA:** Como `siconv_programa` tem até 405 linhas por `ID_PROGRAMA` (elegibilidade por UF/modalidade), cada proposta é multiplicada dezenas de vezes! |

### 5.2. Simulação Empírica de Risco Financeiro (Dupla Contagem)

Comparação do montante do campo `VL_GLOBAL_CONV` (Valor Global do Convênio):

| Cenário de Consulta | Linhas | Soma de `VL_GLOBAL_CONV` (R$) | Fator de Distorção |
| :--- | :--- | :--- | :--- |
| **Soma Real na Tabela de Origem (`siconv_convenio`)** | 287.586 | **R$ 356.856.636.504,87** | **1,000x (Base Real: ~356,8 bilhões)** |
| **Soma após JOIN com `programa_proposta`** | 287.974 | **R$ 358.356.915.707,38** | **+ R$ 1,50 bilhão** (+0,42% de inflação indevida) |
| **Soma após JOIN com `siconv_programa` (Raw Bronze)** | 24.243.823 | **R$ 25.963.722.686.298,16** | **72,7567x (R$ 25,9 TRILHÕES / +7.175,67%)** |

**Conclusão Inegociável:**
- Valores financeiros têm como grão estrito a **Proposta** e o **Convênio**.
- Jamais deve ser construída uma tabela Silver "desnormalizada e achatada" que junte Programa com Convênio na mesma linha sem preservar a granularidade correta ou sem separar fatos de dimensões na Gold.

---

## 6. Diagnóstico de Qualidade, Nulos e Tipagem Semântica por Tabela

### 6.1. Tabela: `bronze.siconv_programa` (1.257.350 linhas)

| Coluna Bronze | Nulos (Qtd / %) | Vazios | Distintos | Tam Min-Max | Tipo Candidato Silver | Diagnóstico de Qualidade & Transformação |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `COD_ORGAO_SUP_PROGRAMA` | 0 (0,0%) | 0 | 39 | 5-5 | `STRING` | Código de órgão superior. Preservar STRING. |
| `DESC_ORGAO_SUP_PROGRAMA` | 0 (0,0%) | 0 | 38 | 15-77 | `STRING` | Descrição do ministério/órgão superior. Limpar espaços extras. |
| `ID_PROGRAMA` | 0 (0,0%) | 0 | 53.018 | 4-5 | `BIGINT` | Identificador numérico do programa. |
| `COD_PROGRAMA` | 0 (0,0%) | 0 | 52.968 | 10-13 | `STRING` | Código de divulgação do programa. Preservar STRING. |
| `NOME_PROGRAMA` | 0 (0,0%) | 0 | 22.108 | 1-255 | `STRING` | Nome do programa. |
| `SIT_PROGRAMA` | 0 (0,0%) | 0 | 3 | 7-15 | `STRING` | Domínio fechado: DISPONIBILIZADO (1.006.898), CADASTRADO (128.034), INATIVO (122.418). |
| `DATA_DISPONIBILIZACAO` | 126.909 (10,09%) | 0 | 3.939 | 10-10 | `DATE` | 100% parseável no formato `dd/MM/yyyy`. Intervalo: 2008-06-18 a 2026-09-15. |
| `ANO_DISPONIBILIZACAO` | 126.909 (10,09%) | 0 | 19 | 4-4 | `SMALLINT` | Ano (2008 a 2026). Nulos coincidem exatamente com data nula. |
| `DT_PROG_INI_RECEB_PROP` | 669.409 (53,24%) | 0 | 2.562 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Contém anomalia de data aberrante: `0006-03-11`. |
| `DT_PROG_FIM_RECEB_PROP` | 669.409 (53,24%) | 0 | 2.221 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Contém anomalia de data aberrante: `5008-12-15`. |
| `DT_PROG_INI_EMENDA_PAR` | 828.377 (65,88%) | 0 | 2.075 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Intervalo: 2009-01-01 a 2026-09-14. |
| `DT_PROG_FIM_EMENDA_PAR` | 828.433 (65,89%) | 0 | 1.477 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Intervalo: 2010-04-30 a 2026-12-31. |
| `DT_PROG_INI_BENEF_ESP` | 966.696 (76,88%) | 0 | 2.541 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Intervalo: 2008-06-27 a 2026-09-16. |
| `DT_PROG_FIM_BENEF_ESP` | 966.696 (76,88%) | 0 | 1.962 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Intervalo: 2009-12-31 a 2026-12-31. |
| `MODALIDADE_PROGRAMA` | 600 (0,05%) | 0 | 8 | 8-31 | `STRING` | CONVENIO (810k), CONTRATO DE REPASSE (291k), etc. |
| `NATUREZA_JURIDICA_PROGRAMA` | 0 (0,0%) | 0 | 7 | 17-53 | `STRING` | Adm Municipal, Estadual, OSC, Consórcio, etc. |
| `UF_PROGRAMA` | 0 (0,0%) | 0 | 27 | 2-2 | `STRING` | Sigla da UF (todas as 27 UFs do Brasil presentes). |
| `ACAO_ORCAMENTARIA` | 1 (0,0001%) | 0 | 9.583 | 8-8 | `STRING` | Código da ação orçamentária no SIOP (ex: `6492XXXX`). |
| `NOME_SUBTIPO_PROGRAMA` | 1.246.295 (99,12%) | 0 | 8 | 3-23 | `STRING` | Alta taxa de nulo (só preenchido para PAC, ECTI, etc.). |
| `DESCRICAO_SUBTIPO_PROGRAMA` | 1.246.295 (99,12%) | 0 | 8 | 7-220 | `STRING` | Descrição legal do subtipo. |

### 6.2. Tabela: `bronze.siconv_programa_proposta` (1.158.975 linhas)

| Coluna Bronze | Nulos (Qtd / %) | Vazios | Distintos | Tam Min-Max | Tipo Candidato Silver | Diagnóstico de Qualidade & Transformação |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ID_PROGRAMA` | 0 (0,0%) | 0 | 36.321 | 4-5 | `BIGINT` | Chave estrangeira para Programa. Zero nulos. |
| `ID_PROPOSTA` | 0 (0,0%) | 0 | 1.157.544 | 4-7 | `BIGINT` | Chave estrangeira para Proposta. Zero nulos. |

### 6.3. Tabela: `bronze.siconv_proposta` (1.157.619 linhas)

| Coluna Bronze | Nulos (Qtd / %) | Vazios | Distintos | Tam Min-Max | Tipo Candidato Silver | Diagnóstico de Qualidade & Transformação |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ID_PROPOSTA` | 0 (0,0%) | 0 | 1.157.619 | 4-7 | `BIGINT` | **Chave Primária Única**. Zero nulos. |
| `UF_PROPONENTE` | 0 (0,0%) | 0 | 27 | 2-2 | `STRING` | Sigla de UF (27 estados, sem nulos ou inválidos). |
| `MUNIC_PROPONENTE` | 0 (0,0%) | 0 | 5.297 | 3-32 | `STRING` | Nome do município do proponente. |
| `COD_MUNIC_IBGE` | 0 (0,0%) | 0 | 5.570 | 7-7 | `STRING` | Código IBGE de 7 dígitos. Manter STRING. |
| `COD_ORGAO_SUP` | 0 (0,0%) | 0 | 37 | 5-5 | `STRING` | Código SIAFI do órgão superior (5 dígitos). |
| `DESC_ORGAO_SUP` | 0 (0,0%) | 0 | 36 | 17-77 | `STRING` | Descrição do órgão superior. |
| `NATUREZA_JURIDICA` | 0 (0,0%) | 0 | 5 | 17-53 | `STRING` | Adm Municipal (1.010.666), OSC (92.301), etc. |
| `NR_PROPOSTA` | 0 (0,0%) | 0 | 1.155.902 | 6-11 | `STRING` | Numeração de proposta (1.717 repetições). |
| `DIA_PROP` | 0 (0,0%) | 0 | 31 | 1-2 | `SMALLINT` | Dia do envio (1 a 31). |
| `MES_PROP` | 0 (0,0%) | 0 | 12 | 1-2 | `SMALLINT` | Mês do envio (1 a 12). |
| `ANO_PROP` | 0 (0,0%) | 0 | 19 | 4-4 | `SMALLINT` | Ano do envio (2008 a 2026). |
| `DIA_PROPOSTA` | 0 (0,0%) | 0 | 6.186 | 10-10 | `DATE` | 100% parseável em `dd/MM/yyyy` (2008 a 2026). |
| `COD_ORGAO` | 0 (0,0%) | 0 | 182 | 5-5 | `STRING` | Código SIAFI do órgão concedente. |
| `DESC_ORGAO` | 0 (0,0%) | 0 | 181 | 3-77 | `STRING` | Descrição do órgão concedente. |
| `MODALIDADE` | 0 (0,0%) | 0 | 7 | 8-31 | `STRING` | CONTRATO DE REPASSE (540k), CONVENIO (499k), etc. |
| `IDENTIF_PROPONENTE` | 0 (0,0%) | 0 | 29.325 | 14-14 | `STRING` | CNPJ/CPF: **39,20% começam com '0'**. Manter STRING. |
| `NM_PROPONENTE` | 0 (0,0%) | 0 | 27.436 | 4-150 | `STRING` | Razão social / Nome do proponente. |
| `CEP_PROPONENTE` | 0 (0,0%) | 0 | 18.221 | 8-8 | `STRING` | CEP: **1,58% começam com '0'**. Manter STRING. |
| `ENDERECO_PROPONENTE` | 0 (0,0%) | 0 | 29.075 | 42-241 | `STRING` | Endereço completo. |
| `BAIRRO_PROPONENTE` | 0 (0,0%) | 0 | 8.005 | 1-50 | `STRING` | Bairro. |
| `NM_BANCO` | 0 (0,0%) | 0 | 5 | 13-30 | `STRING` | Caixa (830k), BB (323k), BNB (3.3k), BASA (223), Não Informado (85). |
| `SITUACAO_CONTA` | 0 (0,0%) | 0 | 9 | 7-27 | `STRING` | Cadastrada (888k), Regularizada (240k), etc. |
| `SITUACAO_PROJETO_BASICO` | 159.802 (13,80%) | 0 | 14 | 8-41 | `STRING` | Não Cadastrado (702k), Em Análise (121k), etc. |
| `SIT_PROPOSTA` | 294 (0,03%) | 0 | 18 | 31-72 | `STRING` | Situação de tramitação da proposta. |
| `DIA_INIC_VIGENCIA_PROPOSTA`| 3 (0,0003%) | 0 | 6.830 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Contém data anômala `0011-06-20`. |
| `DIA_FIM_VIGENCIA_PROPOSTA` | 3 (0,0003%) | 0 | 8.138 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Contém data anômala `0001-01-01` e `3012-05-30`. |
| `OBJETO_PROPOSTA` | 67 (0,006%) | 0 | 788.971 | 1-5000 | `STRING` | Objeto textual. Contém 16 registros sentinela `'-'`. |
| `ITEM_INVESTIMENTO` | 720.031 (62,20%) | 0 | 7 | 12-65 | `STRING` | Obras (210k), Equipamentos (151k), Custeio (46k). |
| `ENVIADA_MANDATARIA` | 0 (0,0%) | 0 | 3 | 3-13 | `STRING` | NÃO APLICÁVEL (607k), NÃO (404k), SIM (145k). |
| `NOME_SUBTIPO_PROPOSTA` | 1.147.783 (99,15%) | 0 | 7 | 3-23 | `STRING` | Subtipo PAC/MCMV. |
| `DESCRICAO_SUBTIPO_PROPOSTA`| 1.147.783 (99,15%) | 0 | 7 | 7-220 | `STRING` | Descrição legal do subtipo. |
| `VL_GLOBAL_PROP` | 0 (0,0%) | 0 | 256.710 | 1-13 | `DECIMAL(17,2)`| 100% parseável (233.101 usam vírgula). Max: 12,8 bi. |
| `VL_REPASSE_PROP` | 0 (0,0%) | 0 | 155.461 | 1-13 | `DECIMAL(17,2)`| 100% parseável (128.357 usam vírgula). Max: 10,5 bi. |
| `VL_CONTRAPARTIDA_PROP` | 0 (0,0%) | 0 | 191.597 | 1-15 | `DECIMAL(17,2)`| 100% parseável. Contém **2 valores negativos** (min: -78.764,95). |
| `CD_AGENCIA` | 0 (0,0%) | 0 | 6.662 | 1-27 | `STRING` | Código da agência: **28,63% começam com '0'**. 90 sentinelas `'-'`. |
| `CD_CONTA` | 889.162 (76,81%) | 0 | 128.160 | 2-13 | `STRING` | Conta corrente: **45,11% começam com '0'**. Manter STRING. |

### 6.4. Tabela: `bronze.siconv_convenio` (287.586 linhas)

| Coluna Bronze | Nulos (Qtd / %) | Vazios | Distintos | Tam Min-Max | Tipo Candidato Silver | Diagnóstico de Qualidade & Transformação |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NR_CONVENIO` | 0 (0,0%) | 0 | 287.584 | 6-6 | `STRING` | Número do convênio (6 dígitos). 2 duplicatas pontuais. |
| `ID_PROPOSTA` | 0 (0,0%) | 0 | 287.584 | 4-7 | `BIGINT` | Chave estrangeira para proposta. |
| `DIA` | 50.024 (17,39%) | 0 | 31 | 1-2 | `SMALLINT` | Dia de assinatura. Nulo para não assinados. |
| `MES` | 50.024 (17,39%) | 0 | 12 | 1-2 | `SMALLINT` | Mês de assinatura. Nulo para não assinados. |
| `ANO` | 50.024 (17,39%) | 0 | 19 | 4-4 | `SMALLINT` | Ano de assinatura (2008 a 2026). Nulo para não assinados. |
| `DIA_ASSIN_CONV` | 50.024 (17,39%) | 0 | 4.218 | 10-10 | `DATE` | 100% parseável em `dd/MM/yyyy`. Nulo para não assinados. |
| `SIT_CONVENIO` | 17.986 (6,25%) | 0 | 20 | 8-61 | `STRING` | Prestação Concluída (92k), Em Execução (46k), Aprovada (42k), etc. |
| `SUBSITUACAO_CONV` | 285.995 (99,45%) | 0 | 9 | 13-35 | `STRING` | Quase totalmente nulo. |
| `SITUACAO_PUBLICACAO` | 0 (0,0%) | 0 | 2 | 9-19 | `STRING` | Publicado (287.574) ou Transferido para IN (12). |
| `INSTRUMENTO_ATIVO` | 0 (0,0%) | 0 | 2 | 3-3 | `BOOLEAN` | NÃO (174.178) vs SIM (113.408). |
| `IND_OPERA_OBTV` | 0 (0,0%) | 0 | 2 | 3-3 | `BOOLEAN` | SIM (208.122) vs NÃO (79.464). |
| `NR_PROCESSO` | 1 (0,0003%) | 0 | 287.398 | 1-31 | `STRING` | Número do processo administrativo. |
| `UG_EMITENTE` | 34.658 (12,05%) | 0 | 594 | 6-6 | `STRING` | Unidade Gestora emitente. 25 começam com zero. |
| `DIA_PUBL_CONV` | 50.426 (17,53%) | 0 | 4.277 | 10-10 | `DATE` | 100% parseável em `dd/MM/yyyy`. Intervalo: 2001 a 2026. |
| `DIA_INIC_VIGENC_CONV` | 0 (0,0%) | 0 | 5.369 | 10-10 | `DATE` | 100% parseável. Contém ano aberrante `1900-04-15`. |
| `DIA_FIM_VIGENC_CONV` | 0 (0,0%) | 0 | 7.887 | 10-10 | `DATE` | 100% parseável. Vigências até 2035. |
| `DIA_FIM_VIGENC_ORIGINAL_CONV` | 0 (0,0%) | 0 | 7.807 | 10-10 | `DATE` | 100% parseável. |
| `DIAS_PREST_CONTAS` | 0 (0,0%) | 0 | 772 | 1-4 | `INT` | Prazo em dias para prestação de contas. |
| `DIA_LIMITE_PREST_CONTAS` | 0 (0,0%) | 0 | 7.981 | 10-10 | `DATE` | 100% parseável. |
| `DATA_SUSPENSIVA` | 267.907 (93,16%) | 0 | 2.802 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Contém ano aberrante `0010-03-30`. |
| `DATA_RETIRADA_SUSPENSIVA` | 161.887 (56,29%) | 0 | 4.453 | 10-10 | `DATE` | Parseável em `dd/MM/yyyy`. Intervalo: 2010 a 2026. |
| `DIAS_CLAUSULA_SUSPENSIVA` | 265.478 (92,31%) | 0 | 1.027 | 1-4 | `INT` | Dias de cláusula suspensiva. |
| `SITUACAO_CONTRATACAO` | 63.999 (22,25%) | 0 | 4 | 6-42 | `STRING` | Normal (201k), Cláusula Suspensiva (19k), Liminar (2.4k). |
| `IND_ASSINADO` | 0 (0,0%) | 0 | 2 | 3-3 | `BOOLEAN` | SIM (237.562) vs NÃO (50.024). Explica os nulos em data de assinatura! |
| `MOTIVO_SUSPENSAO` | 267.910 (93,16%) | 0 | 779 | 1-286 | `STRING` | Texto com justificativa de suspensão. |
| `IND_FOTO` | 0 (0,0%) | 0 | 2 | 3-3 | `BOOLEAN` | NÃO (269.480) vs SIM (18.106). |
| `QTDE_CONVENIOS` | 0 (0,0%) | 0 | 1 | 1-1 | `INT` | **Coluna Constante**: 100% dos valores são `'1'`. |
| `QTD_TA` | 139.116 (48,37%) | 0 | 42 | 1-2 | `SMALLINT` | Quantidade de Termos Aditivos (0 a 42). |
| `QTD_PRORROGA` | 200.874 (69,85%) | 0 | 25 | 1-2 | `SMALLINT` | Quantidade de prorrogações de ofício (1 a 25). |
| `VL_GLOBAL_CONV` | 0 (0,0%) | 0 | 141.347 | 1-13 | `DECIMAL(17,2)`| 100% parseável. 113.800 vírgulas. Max: 1,51 bi. |
| `VL_REPASSE_CONV` | 0 (0,0%) | 0 | 58.111 | 1-13 | `DECIMAL(17,2)`| 100% parseável. 48.296 vírgulas. Max: 1,51 bi. |
| `VL_CONTRAPARTIDA_CONV` | 0 (0,0%) | 0 | 109.770 | 1-15 | `DECIMAL(17,2)`| 100% parseável. Contém **2 valores negativos** (-78.764,95). |
| `VL_EMPENHADO_CONV` | 13.352 (4,64%) | 0 | 52.219 | 1-20 | `DECIMAL(17,2)`| 100% parseável. Contém **31 valores negativos** (estorno de empenho, min: -5.118.910,33). |
| `VL_DESEMBOLSADO_CONV` | 98.973 (34,42%) | 0 | 54.451 | 1-12 | `DECIMAL(17,2)`| 100% parseável. 44.289 vírgulas. Max: 625 mi. |
| `VL_SALDO_REMAN_TESOURO` | 142.409 (49,52%) | 0 | 142.279 | 1-11 | `DECIMAL(17,2)`| 100% parseável. 143.117 vírgulas. Max: 116 mi. |
| `VL_SALDO_REMAN_CONVENENTE` | 211.353 (73,49%) | 0 | 59.661 | 1-11 | `DECIMAL(17,2)`| 100% parseável. 74.679 vírgulas. Max: 13,1 mi. |
| `VL_RENDIMENTO_APLICACAO` | 278.964 (97,00%) | 0 | 7.912 | 1-11 | `DECIMAL(17,2)`| 100% parseável. 5.888 vírgulas. Max: 95,7 mi. |
| `VL_INGRESSO_CONTRAPARTIDA` | 139.579 (48,53%) | 0 | 94.042 | 1-12 | `DECIMAL(17,2)`| 100% parseável. 85.902 vírgulas. Max: 196 mi. |
| `VL_SALDO_CONTA` | 56.478 (19,64%) | 0 | 33.552 | 1-12 | `DECIMAL(17,2)`| **Atenção:** 34.901 valores usam PONTO decimal (não vírgula!). Parseável com ponto. |
| `VALOR_GLOBAL_ORIGINAL_CONV`| 202.279 (70,34%) | 0 | 30.962 | 5-12 | `DECIMAL(17,2)`| 100% parseável. 21.790 vírgulas. Max: 565 mi. |

---

## 7. Anomalias de Dados Relevantes e Descobertas Críticas

1. **Datas Aberrantes (Sentinelas Cronológicas):**
   - Foram encontradas datas com anos inválidos como `0001-01-01`, `0006-03-11`, `0010-03-30`, `0011-06-20`, `1900-04-15` e datas futuras como `3012-05-30` e `5008-12-15`.
   - Essas datas decorrem de preenchimentos manuais sentinela ou erros de digitação nos sistemas legados do SICONV. Elas são sintaticamente válidas para o parser `dd/MM/yyyy`, mas semanticamente impossíveis.
2. **Formatação Mista de Valores Decimais:**
   - 99% das colunas monetárias usam vírgula como separador decimal (padrão brasileiro `1234,56`).
   - Porém, a coluna `VL_SALDO_CONTA` em `siconv_convenio` vem com **ponto decimal** (`65574.68`), pois decorre de extrato bancário automático.
   - Regra segura para o R3-B: Utilizar uma macro/função que substitui vírgula por ponto de forma idempotente: `CAST(REPLACE(col, ',', '.') AS DECIMAL(17, 2))`.
3. **Valores Negativos Legítimos em Execução Orçamentária:**
   - Foram detectados 31 valores negativos em `VL_EMPENHADO_CONV` (menor valor: `-R$ 5.118.910,33`) e 2 em contrapartida.
   - No orçamento público brasileiro, empenhos negativos são **estornos de empenho** (cancelamentos ou anulações parciais de dotações). Portanto, **não devem ser descartados ou tratados como erro**, pois fazem parte da aritmética de liquidação.
4. **Preservação de Zeros à Esquerda em Documentos e Códigos:**
   - `IDENTIF_PROPONENTE` (CNPJ/CPF): 39,20% começam com zero.
   - `CD_AGENCIA`: 28,63% começam com zero.
   - `CD_CONTA`: 45,11% começam com zero.
   - `CEP_PROPONENTE`: 1,58% começam com zero.
   - Converter esses campos para números inteiros ou bigint causaria corrupção irreparável de dados cadastrais. **Devem permanecer como STRING.**
5. **Sentinelas e Caracteres de Preenchimento:**
   - `OBJETO_PROPOSTA`: 16 registros com `'-'`.
   - `CD_AGENCIA`: 90 registros com `'-'`.
   - `NM_BANCO`: 85 registros com `'NÃO INFORMADO'`.
   - `ENVIADA_MANDATARIA`: 607.528 registros com `'NÃO APLICÁVEL'`.
   - No R3-B, converter strings sentinelas vazias ou de preenchimento (`'-'`) para `NULL` quando o campo for identificador ou descritivo opcional.

---

## 8. Gate Crítico de Decisão — Respostas Explícitas

Respondendo estritamente às 10 perguntas do Gate Crítico da Missão:

1. **Qual é o grão comprovado de cada uma das quatro tabelas?**
   - `siconv_programa`: Critério de elegibilidade e abertura regional/orçamentária do programa (composto por Programa $\times$ Modalidade $\times$ Natureza Jurídica $\times$ UF $\times$ Ação Orçamentária).
   - `siconv_programa_proposta`: Vínculo associativo entre programa e proposta (grão: par `ID_PROGRAMA` e `ID_PROPOSTA`).
   - `siconv_proposta`: Proposta de trabalho submetida (grão: `ID_PROPOSTA`).
   - `siconv_convenio`: Instrumento de convênio/repasse formalizado (grão lógico pretendido: `NR_CONVENIO`).

2. **Quais chaves são realmente únicas?**
   - `ID_PROPOSTA` em `siconv_proposta` (1.157.619 distintos em 1.157.619 linhas — 100% única).
   - `(ID_PROGRAMA, ID_PROPOSTA)` em `siconv_programa_proposta` (1.158.975 pares distintos em 1.158.975 linhas — 100% única).

3. **Quais chaves NÃO são únicas?**
   - `ID_PROGRAMA` em `siconv_programa` (53.018 distintos em 1.257.350 linhas — repete até 405 vezes).
   - `NR_PROPOSTA` em `siconv_proposta` (1.155.902 distintos em 1.157.619 linhas — 1.717 colisões).
   - `NR_CONVENIO` em `siconv_convenio` bruto (287.584 distintos em 287.586 linhas — 2 convênios com 2 linhas devido à variação de saldo de conta).
   - `ID_PROPOSTA` em `siconv_convenio` bruto (287.584 distintos em 287.586 linhas).

4. **`programa_proposta` é realmente uma ponte N:N?**
   - **SIM, COMPROVADO.** Existem 1.231 propostas associadas a múltiplos programas (até 16 programas por proposta) e cada programa possui de 1 a 25.419 propostas associadas.

5. **Uma proposta pode estar associada a mais de um programa?**
   - **SIM, COMPROVADO.** Observado empiricamente em 1.231 propostas na ponte `siconv_programa_proposta`.

6. **Uma proposta pode estar associada a mais de um convênio?**
   - **NÃO, COMPROVADO NOS DADOS.** Nenhuma proposta em `siconv_convenio` possui mais de um `NR_CONVENIO` distinto. A cardinalidade proposta ↔ convênio é estritamente 1:0..1.

7. **Quais joins multiplicam linhas?**
   - Join com `siconv_programa` (Raw Bronze): Multiplica catastrophicamente por até **84,3 vezes** (de 287 mil para 24,2 milhões de linhas).
   - Join com `siconv_programa_proposta`: Multiplica suavemente por **1,0013 vezes** (+388 linhas nos convênios, +1.431 linhas nas propostas).

8. **Quais joins podem multiplicar valores financeiros?**
   - Qualquer join direto que cruze `siconv_convenio` ou `siconv_proposta` com `siconv_programa_proposta` ou `siconv_programa` sem agrupamento prévio. Na simulação empírica, a soma de convênios salta de R$ 356 bilhões para **R$ 25,9 trilhões** se unida diretamente à tabela de programas.

9. **Quais campos podem ser tipados com segurança?**
   - Datas no padrão `dd/MM/yyyy` (100% de sucesso no parsing em todas as tabelas).
   - Campos de valor monetário tratados com substituição de `,` por `.` para `DECIMAL(17, 2)`.
   - Campos de ano e dia para `SMALLINT`.
   - Indicadores 'SIM'/'NÃO' para `BOOLEAN`.
   - Documentos (CNPJ/CPF, CEP, Agência, Conta, IBGE) DEVEM ser tipados como `STRING` para não destruir zeros à esquerda.

10. **Quais decisões foram fechadas no R3-A.1?**
    - Tratamento definitivo dos 2 convênios conflitantes em `siconv_convenio`: preservação integral das 287.586 observações (sem deduplicação por falta de critério objetivo de recência).
    - Unicidade de `siconv_programa`: comprovada 100% única na chave de 5 colunas (e também de 4 colunas). Chave surrogate recomendada via SHA-256.
    - Entidade `programa_cadastral`: CONFIRMADA com dependência funcional 1:1 estrita para todos os 7 atributos em todos os 53.018 programas.
    - Baseline financeiro: fixado estritamente em DECIMAL com cálculo de multiplicidades agregadas (sem materialização explosiva).

---

## 9. R3-A.1 — Fechamento dos Findings da Revisão Humana

Em 17/09/2026, foi executada a investigação controlada e incremental (R3-A.1) para fechar formalmente as quatro ambiguidades remanescentes do R3-A antes do início do R3-B.

### 9.1. Finding 1 — Identidade e Grão Final de `siconv_programa`

- **Investigação:** Testou-se a presença de colisões na chave candidata de 5 colunas `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA, ACAO_ORCAMENTARIA)` e na chave de 4 colunas (sem `ACAO_ORCAMENTARIA`).
- **Resultados Empíricos Observados:**
  - Grupos de colisão (5 colunas): **0**
  - Linhas envolvidas em colisão: **0**
  - Multiplicidade máxima: **1**
  - Unicidade comprovada: **1.257.350 combinações distintas em 1.257.350 linhas (100% de unicidade)**.
  - Teste de minimalidade: A chave com 4 colunas `(ID_PROGRAMA, MODALIDADE_PROGRAMA, NATUREZA_JURIDICA_PROGRAMA, UF_PROGRAMA)` também apresentou **0 grupos de colisão** e unicidade estrita (1.257.350 distintos), demonstrando que a `ACAO_ORCAMENTARIA` possui relação 1:1 com a quádrupla programa/modalidade/natureza/UF.
  - Esclarecimento técnico: A divergência de 601 registros registrada no profiling inicial ocorreu porque o `COUNT(DISTINCT a, b, c, d, e)` padrão do Spark descarta qualquer linha contendo pelo menos um valor `NULL`. Como `MODALIDADE_PROGRAMA` possui 600 nulos e `ACAO_ORCAMENTARIA` possui 1 nulo ($600 + 1 = 601$), tais registros foram excluídos da contagem de distintos agregada simples, simulando falsa colisão. Com agrupamento e tratamento de nulos (`GROUP BY` ou `CONCAT_WS` com `COALESCE`), a unicidade é absoluta.
- **Decisão e Contrato:**
  - **Identidade do Programa Lógico:** `id_programa` (53.018 distintos).
  - **Identidade da Linha de Elegibilidade:** Chave natural composta pelas 5 colunas de elegibilidade.
  - **Identidade Técnica Recomendada:** `id_programa_elegibilidade = sha256(concat_ws('||', coalesce(id_programa, ''), coalesce(modalidade_programa, ''), coalesce(natureza_juridica_programa, ''), coalesce(uf_programa, ''), coalesce(acao_orcamentaria, '')))`.
  - **Proibição Estrita:** Não utilizar hash baseado em chaves comprovadamente não únicas e não utilizar MD5.

### 9.2. Finding 2 — Tratamento Não Destrutivo dos Conflitos de `siconv_convenio`

- **Investigação:** Inspeção exaustiva de todas as 40 colunas de negócio e 4 colunas técnicas nos dois instrumentos conflitantes:
  - Convênio `949286` (linhas com saldo `9915.04` vs `9918.59`).
  - Convênio `956078` (linhas com saldo `3164224.05` vs `3165629.92`).
- **Resultados Empíricos Observados:**
  - Mesmo `__ingestion_run_id`: **SIM** (`run_20260916T182321` em ambos).
  - Mesmo `__source_sha256`: **SIM** (`4ba90760...` em ambos).
  - Mesmo `__ingested_at_utc`: **SIM** (`2026-09-16T18:33:25.679585+00:00` em ambos).
  - Mesmo `__source_file`: **SIM** (`siconv_convenio.zip` em ambos).
  - Diferença em colunas temporais de negócio (`DIA_ASSIN_CONV`, `DIA_PUBL_CONV`, etc.): **NENHUMA** (datas 100% idênticas).
  - Coluna divergente: **EXCLUSIVAMENTE `VL_SALDO_CONTA`**. Todas as outras 43 colunas são rigorosamente idênticas.
- **Decisão e Contrato:**
  - **Critério Temporal Objetivo:** **NÃO EXISTE CRITÉRIO OBJETIVO DE RECÊNCIA**. As duas linhas de cada convênio foram extraídas e disponibilizadas simultaneamente no mesmo arquivo oficial pelo Transferegov.
  - **Deduplicação Comprovada:** **NÃO DEDUPLICAR**. É expressamente proibido arbitrar desempate escolhendo maior saldo, menor saldo ou aplicando `ROW_NUMBER()` aleatório.
  - **Volume Silver Recomendado:** **287.586 linhas** (preservando integralmente as observações da Bronze).
  - **Teste de Unicidade:** O teste `unique(numero_convenio)` **NÃO é teste obrigatório aprovado para o R3-B**.
  - **Hipótese:** Oscilação de concorrência ou snapshots assíncronos na extração bancária do órgão concedente.

### 9.3. Finding 3 — Baseline Financeiro Exato com DECIMAL

- **Investigação:** Substituição da materialização explosiva de joins (proibida devido ao consumo excessivo de recursos e risco de OOM) pelo cálculo exato por meio de **multiplicidades agregadas**, utilizando precisão fixa `DECIMAL(38, 2)`:
  - Base: `COUNT(*)` e `SUM(DECIMAL)` em `siconv_convenio`.
  - Ponte (`siconv_programa_proposta`): Multiplicidade de programas por proposta ($\sum \text{qtd\_programas}$).
  - Programa (`siconv_programa`): Multiplicidade física por programa cruzada com a ponte ($\sum \text{qtd\_linhas\_programa}$).
- **Resultados Empíricos Observados:**
  - **Soma Base (`siconv_convenio`):**
    - Linhas base: **287.586**
    - Soma DECIMAL: **R$ 356.856.636.504,87**
    - Soma DOUBLE: R$ 356.856.636.504,87 (coincidente nesta granularidade)
  - **Efeito da Ponte (`siconv_programa_proposta`):**
    - Linhas equivalentes após join: **287.974** (+388 linhas, confirmação exata do baseline anterior)
    - Soma DECIMAL: **R$ 358.356.915.707,38**
    - Diferença absoluta: **+ R$ 1.500.279.202,51** (+0,420415% de distorção)
  - **Efeito de `siconv_programa` (sem gerar fisicamente as ~24M linhas):**
    - Linhas equivalentes após join: **24.243.823** (**confirmação exata da cardinalidade do R3-A por multiplicidade agregada em 9,78 segundos!**)
    - Soma DECIMAL: **R$ 25.963.722.686.298,16**
    - Diferença absoluta: **+ R$ 25.606.866.049.793,29**
    - Fator multiplicativo: **72,7567x**
  - **Comparação com DOUBLE anterior:** O valor em DOUBLE anteriormente calculado na revisão preliminar apresentava pequenas oscilações de arredondamento IEEE-754 na casa dos centavos (`.239,21` vs `.298,16`). O baseline oficial fica estritamente definido pela precisão exata de **DECIMAL(38, 2)**.
- **Decisão e Contrato:**
  - Transformação individual: `DECIMAL(17, 2)` (ou tipo de domínio adequado).
  - Agregações e métricas de reconciliação: `DECIMAL(38, 2)` de precisão ampliada.
  - Tipos `DOUBLE` e `FLOAT` ficam **estritamente proibidos** para reconciliação financeira.

### 9.4. Finding 4 — Dependência Funcional e Confirmação de `programa_cadastral`

- **Investigação:** Avaliação da dependência funcional dos atributos cadastrais de programa em relação a `ID_PROGRAMA` por meio de uma única agregação distribuída:
  - `COD_ORGAO_SUP_PROGRAMA`
  - `DESC_ORGAO_SUP_PROGRAMA`
  - `COD_PROGRAMA`
  - `NOME_PROGRAMA`
  - `SIT_PROGRAMA`
  - `DATA_DISPONIBILIZACAO`
  - `ANO_DISPONIBILIZACAO`
  - Assinatura da tupla cadastral completa.
- **Resultados Empíricos Observados (53.018 `ID_PROGRAMA` analisados):**

| Atributo Cadastral | IDs com > 1 Valor | Percentual | Máximo de Valores Distintos |
| :--- | :--- | :--- | :--- |
| `COD_ORGAO_SUP_PROGRAMA` | **0** | 0,0000% | 1 |
| `DESC_ORGAO_SUP_PROGRAMA` | **0** | 0,0000% | 1 |
| `COD_PROGRAMA` | **0** | 0,0000% | 1 |
| `NOME_PROGRAMA` | **0** | 0,0000% | 1 |
| `SIT_PROGRAMA` | **0** | 0,0000% | 1 |
| `DATA_DISPONIBILIZACAO` | **0** | 0,0000% | 1 |
| `ANO_DISPONIBILIZACAO` | **0** | 0,0000% | 1 |
| **Tupla Cadastral Completa** | **0** | **0,0000%** | **1** |

- **Decisão e Contrato:**
  - **Dependência Funcional 1:1 Estrita:** **COMPROVADA**. Cada `ID_PROGRAMA` determina exatamente uma única tupla cadastral em 100% dos casos.
  - **Classificação da Entidade:** **CONFIRMADO**. A criação da entidade `silver.siconv_programa_cadastral` (grão: 1 linha por `id_programa`, 53.018 linhas) é relacionalmente limpa, não requer funções arbitrárias de desempate (`FIRST`, `MAX`, `MIN`) e elimina redundâncias nos modelos subsequentes.

### 9.5. Ajustes Menores de Governança

1. **Serviços Docker:** O cluster ativo é composto por **9 serviços** no `docker compose ps`: `airflow`, `airflow-db`, `mc`, `minio`, `spark-master`, `spark-thrift-server`, `spark-worker-1`, `spark-worker-2` e `superset`.
2. **Distinção de Cardinalidades de Programa:**
   - Na relação lógica da ponte `siconv_programa_proposta` $\rightarrow$ Programa Cadastral: **N:1** (cada proposta aponta para programas lógicos únicos).
   - Na junção direta da ponte com as linhas físicas de `siconv_programa`: **Multiplicativo catastrófico** (devido às regras de elegibilidade por UF/modalidade).
3. **Tratamento de Datas Fora do Domínio Plausível:** O corte para anos `< 1990` ou `> 2050` permanece categorizado como **REGRA CANDIDATA** (não aplicar filtros destrutivos sem aprovação de negócio).
4. **Transformação Segura de Booleanos:** Implementar via:
   ```sql
   CASE
     WHEN UPPER(TRIM(valor)) = 'SIM' THEN TRUE
     WHEN UPPER(TRIM(valor)) = 'NÃO' THEN FALSE
     ELSE NULL
   END
   ```
   Apenas para colunas cujo domínio tenha sido empiricamente auditado, nunca utilizando `ELSE FALSE`.
