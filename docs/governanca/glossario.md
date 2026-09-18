# Glossário de Conceitos e Terminologia

## 1. Visão Geral

Este glossário define de forma precisa, objetiva e contextual os termos técnicos, dimensionais, operacionais e de negócio empregados no **Lakehouse de Transferências Discricionárias e Legais da União**. As definições refletem fielmente as estruturas encontradas nas bases oficiais do Transferegov e na arquitetura implementada.

---

## 2. Termos de Negócio e Domínio Público

- **Programa**: Política pública ou ação orçamentária governamental criada por um órgão superior da União, sob a qual são disponibilizados recursos para proponentes apresentarem propostas de trabalho. Identificado logicamente por `id_programa`.
- **Elegibilidade de Programa**: Conjunto de critérios e restrições operacionais e regionais que delimitam quem pode submeter propostas a um determinado programa (definido pela combinação de modalidade, natureza jurídica do proponente, UF de atendimento e ação orçamentária).
- **Proposta**: Projeto ou pleito de trabalho submetido por um proponente no Transferegov solicitando recursos financeiros da União para a execução de um objeto específico. Identificada univocamente por `id_proposta`.
- **Convênio / Instrumento**: Instrumento jurídico formal celebrado entre a União (órgão concedente) e o proponente aprovado (convenente), formalizando a transferência voluntária ou discricionária e as obrigações mútuas. Identificado canonicamente por `numero_convenio`.
- **Proponente**: Entidade federativa (estado, município), consórcio público ou organização da sociedade civil que apresenta uma proposta de trabalho e atua como eventual convenente na execução dos recursos. Identificado por `identificacao_proponente` (CNPJ ou CPF).
- **Órgão Superior**: Ministério ou órgão de cúpula da Administração Pública Federal responsável pela formulação da política pública e diretrizes do programa orçamentário.
- **Órgão Concedente**: Entidade, autarquia ou unidade administrativa diretamente responsável pela celebração do convênio, análise técnica, empenho, liberação de parcelas e fiscalização da execução.

---

## 3. Conceitos de Engenharia e Modelagem Dimensional

- **Grão (Granularidade)**: Nível atômico de detalhe de uma tabela de dados; a definição exata do que constitui uma única linha em uma tabela (ex.: 1 linha por proposta em `fct_proposta`).
- **Chave Natural (Business Key)**: Identificador original gerado pelo sistema de origem (Transferegov) que possui significado direto no domínio do negócio (ex.: `id_proposta`, `numero_convenio`, `codigo_municipio_ibge`).
- **Chave Técnica (Surrogate Key)**: Chave artificial ou determinística gerada pelo processo de engenharia de dados (geralmente via hash SHA-256) para garantir unicidade estrita quando a fonte original não oferece chave natural atômica (ex.: `id_programa_elegibilidade`, `id_convenio_observacao`).
- **Tabela Fato**: Tabela dimensional contendo eventos de negócio, quantidades, datas e medidas numéricas quantitativas passíveis de agregação analítica (ex.: `fct_proposta`, `fct_convenio`).
- **Tabela Dimensão**: Tabela contendo atributos descritivos, contextuais e hierárquicos que qualificam os eventos das tabelas fato (ex.: `dim_proponente`, `dim_municipio`, `dim_orgao`, `dim_data`).
- **Bridge (Tabela Ponte)**: Tabela associativa relacional utilizada em modelagem dimensional para resolver e representar relacionamentos muitos-para-muitos (N:N) entre fatos e dimensões (ex.: `bridge_programa_proposta`).
- **Aditividade**: Propriedade de uma medida numérica que permite sua soma consistente através de qualquer dimensão associada. Medidas puramente aditivas podem ser somadas livremente; medidas semi-aditivas ou não aditivas exigem restrições de agregação.
- **Medida Observacional**: Registro numérico de um estado externo coletado em um ponto no tempo que não representa um fluxo contábil canônico (ex.: `valor_saldo_conta`). Por estar sujeito a concorrências e assincronias de captura, é categorizado como não aditivo.

---

## 4. Conceitos Operacionais e Camadas do Lakehouse

- **Snapshot**: Fotografia física completa do estado de uma base de dados em determinado instante de tempo, refletindo o conteúdo exato disponibilizado pelo publicador naquele momento.
- **RAW**: Camada inicial de armazenamento de objetos no MinIO, onde os pacotes compactados (.zip) são guardados de forma imutável, exatamente como foram baixados, com verificação de integridade SHA-256.
- **Bronze**: Camada de tabelas Delta Lake contendo os dados brutos extraídos do RAW, onde todos os campos da fonte são mantidos como texto (`StringType`) e são anexados metadados técnicos de auditoria.
- **Silver**: Camada de tabelas Delta Lake padronizadas, com tipagem estrita de datas e valores monetários, remoção de inconsistências estruturais, desacoplamento cadastral e identificação de conflitos.
- **Gold Core**: Camada de tabelas Delta Lake organizadas segundo a modelagem dimensional estrela estendida, contendo fatos, dimensões conformadas e tabelas ponte de relacionamento.
- **Semantic View**: View dbt lógica (`vw_superset_proposta_convenio`) que formaliza as regras de negócio de apresentação para Business Intelligence, preservando propostas não conveniadas e blindando o grão analítico.
- **Serving Mart**: Tabela Delta Lake materializada fisicamente a partir da Semantic View (`mart_superset_proposta_convenio`), reduzindo o custo de joins e recalculações no consumo do Apache Superset.
- **NO_CHANGE**: Estado de conclusão inteligente e idempotente da ingestão, sinalizando que a fonte de dados externa não sofreu alterações desde a última execução e dispensando o reprocessamento redundante da base.
- **ingestion_run_id**: Identificador determinístico e seguro de correlação de uma execução de ingestão (por exemplo, `e2e_<timestamp>`). É persistido na auditoria Bronze e preservado por linha até a Silver; camadas analíticas posteriores utilizam linhagem de modelo/execução.
