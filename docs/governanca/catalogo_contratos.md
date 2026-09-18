# Catálogo Técnico e Contratos de Dados

## 1. Introdução

Este catálogo formaliza as especificações técnicas, grãos garantidos, chaves primárias e regras contratuais de negócio para cada ativo de dados do Lakehouse. As definições aqui registradas representam contratos estruturais invariantes da arquitetura, desvinculados de volumetrias pontuais de snapshots específicos.

---

## 2. Contratos Obrigatórios de Grão e Chaves

A integridade do Lakehouse repousa na estrita aderência aos seguintes contratos de grão por entidade:

| Entidade / Conceito | Grão Contratual Garantido | Chave Canônica de Identificação | Restrição Estrutural |
| :--- | :--- | :--- | :--- |
| **Proposta de Trabalho** | 1 linha por proposta submetida | `id_proposta` (numérico bigint) | 100% única e NOT NULL em todas as camadas downstream. |
| **Programa Cadastral** | 1 linha por programa governamental | `id_programa` (numérico bigint) | 100% única na dimensão; dependência funcional 1:1 comprovada com nome e código. |
| **Critério de Elegibilidade** | 1 linha por critério regional/orçamentário | `id_programa_elegibilidade` (SHA-256) | Superchave composta pelos 5 atributos de elegibilidade. |
| **Vínculo Programa-Proposta** | 1 linha por associação Programa × Proposta | Par composto (`id_programa`, `id_proposta`) | Cardinalidade N:N; não admite duplicação do par de relacionamento. |
| **Convênio Canônico** | 1 linha por instrumento formalizado | `numero_convenio` (numérico/string) | Relação estrita 1:1 partindo do convênio para a proposta originária. |
| **Saldo Observacional** | 1 linha por observação física no snapshot | `id_convenio_observacao` (SHA-256) | Preserva divergências e concorrências de observações da fonte oficial. |
| **Serving Mart Executivo** | 1 linha por proposta de trabalho | `id_proposta` (numérico bigint) | Preserva propostas sem convênio via LEFT JOIN; grão idêntico ao de proposta. |

---

## 3. Contratos Críticos de Aditividade Financeira

Para evitar distorções analíticas e conclusões financeiras equivocadas nos relatórios e painéis executivos, os seguintes contratos de aditividade são estritamente mandatórios:

```mermaid
flowchart TD
    subgraph Valido ["Caminhos Analíticos Válidos"]
        P[fct_proposta] -->|Soma Aditiva| MP[Métricas de Propostas\nvalor_global, valor_repasse]
        C[fct_convenio] -->|Soma Aditiva| MC[Métricas de Convênio\nvalor_global, empenhado, desembolsado]
        B[bridge_programa_proposta] -->|COUNT DISTINCT| QP[Contagem de Propostas e Convênios por Programa]
    end

    subgraph Proibido ["Caminhos Analíticos Inválidos (Proibidos)"]
        X[bridge_programa_proposta] -.->|SOMA NÃO ADITIVA (Dupla Contagem)| EX[Soma de valores de convênio por programa]
        Y[fct_convenio_saldo_observacao] -.->|SOMA NÃO ADITIVA (Inconsistente)| EY[Soma de valor_saldo_conta]
    end

    style Proibido fill:#ffebee,stroke:#c62828,stroke-width:2px;
    style Valido fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
```

1. **Relação N:N entre Programas e Propostas (`bridge_programa_proposta`)**:
   - Um mesmo programa pode ter dezenas de propostas, e uma mesma proposta pode se beneficiar de mais de um programa governamental.
   - **Contrato**: É terminantemente proibido somar valores financeiros (`valor_global_proposta`, `valor_repasse_convenio`, etc.) agrupando por `id_programa` através da bridge sem prévia consolidação. A bridge serve exclusivamente para **contagem distinta (`COUNT(DISTINCT ...)`)** de propostas e instrumentos vinculados.
2. **Medida Observacional de Saldo (`valor_saldo_conta`)**:
   - A coluna `valor_saldo_conta` reflete a posição bancária pontual reportada pela fonte oficial, sujeita a divergências de recência e observações concorrentes legítimas.
   - **Contrato**: `valor_saldo_conta` é uma medida **estritamente observacional e não aditiva**. Ela reside exclusivamente em `fct_convenio_saldo_observacao` e está **deliberadamente excluída** do Serving Mart e dos painéis analíticos do Superset.

---

## 4. Matriz do Catálogo de Ativos

A tabela a seguir consolida todos os ativos gerenciados no Lakehouse, indicando camada, grão, chave primária, origem, materialização física, aditividade e consumidores autorizados:

| Camada | Ativo de Dados | Grão Contratual | Chave Primária / Identificador | Origem | Materialização | Aditividade | Consumidor | Restrição Principal |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `siconv_proposta` | 1 linha / proposta | `ID_PROPOSTA` | Dump oficial Siconv | Delta Lake (tabela) | Não aplicável (bruto) | Staging dbt | Campos texto bruto; metadados técnicos obrigatórios. |
| **Bronze** | `siconv_programa_proposta` | 1 linha / vínculo N:N | (`ID_PROGRAMA`, `ID_PROPOSTA`) | Dump oficial Siconv | Delta Lake (tabela) | Não aplicável (bruto) | Staging dbt | Pode conter propostas órfãs da fonte. |
| **Bronze** | `siconv_programa` | 1 linha / elegibilidade | Superchave 5 colunas | Dump oficial Siconv | Delta Lake (tabela) | Não aplicável (bruto) | Staging dbt | `ID_PROGRAMA` não é chave primária física bruta. |
| **Bronze** | `siconv_convenio` | 1 observação do instrumento | Chave de observação | Dump oficial Siconv | Delta Lake (tabela) | Não aplicável (bruto) | Staging dbt | Preserva observações concorrentes da fonte. |
| **Bronze** | `ingestion_runs` | 1 linha / execução E2E | `run_id` (UUID) | Airflow / Ingestão | Delta Lake (tabela) | Totalmente aditivo | Governança / Auditoria | Histórico append-only de execuções. |
| **Bronze** | `ingestion_manifest` | 1 linha / dataset / run | (`run_id`, `dataset_name`) | Ingestão Spark | Delta Lake (tabela) | Totalmente aditivo | Governança / Auditoria | Registro individual de SHA-256 e contagens de linhas. |
| **Staging** | `stg_siconv_proposta` | 1 linha / proposta | `id_proposta` | `bronze.siconv_proposta` | Ephemeral (CTE) | Não aplicável | Silver | Ponto único de casting, parsing de datas e decimais. |
| **Staging** | `stg_siconv_programa_proposta` | 1 linha / vínculo N:N | (`id_programa`, `id_proposta`) | `bronze.siconv_programa_proposta` | Ephemeral (CTE) | Não aplicável | Silver | Tipagem numérica de chaves. |
| **Staging** | `stg_siconv_programa` | 1 linha / elegibilidade | `id_programa_elegibilidade` | `bronze.siconv_programa` | Ephemeral (CTE) | Não aplicável | Silver | Geração de hash SHA-256 para superchave de elegibilidade. |
| **Staging** | `stg_siconv_convenio` | 1 observação do instrumento | `id_convenio_observacao` | `bronze.siconv_convenio` | Ephemeral (CTE) | Não aplicável | Silver | Identificação e flag de conflito de observações. |
| **Silver** | `siconv_proposta` | 1 linha / proposta | `id_proposta` | `stg_siconv_proposta` | Delta Lake (tabela) | Aditiva em propostas | Gold Core | `numero_proposta` não é único operacionalmente. |
| **Silver** | `siconv_programa_cadastral` | 1 linha / programa | `id_programa` | `stg_siconv_programa` | Delta Lake (tabela) | Não aplicável (cadastral) | Gold Core | Entidade cadastral 1:1 sem duplicações de elegibilidade. |
| **Silver** | `siconv_programa_elegibilidade`| 1 linha / critério | `id_programa_elegibilidade` | `stg_siconv_programa` | Delta Lake (tabela) | Não aplicável | Gold Core | Detalhamento regional/orçamentário do programa. |
| **Silver** | `siconv_programa_proposta` | 1 linha / vínculo N:N | (`id_programa`, `id_proposta`) | `stg_siconv_programa_proposta` | Delta Lake (tabela) | Somente contagem distinta | Gold Core | Tabela associativa relacional. |
| **Silver** | `siconv_convenio` | 1 observação de convênio | `id_convenio_observacao` | `stg_siconv_convenio` | Delta Lake (tabela) | Observacional | Gold Core | Preserva histórico completo de observações. |
| **Gold** | `dim_data` | 1 linha / dia civil | `data_sk` (YYYYMMDD) | Script de calendário | Delta Lake (tabela) | Não aplicável (dimensão) | Fatos Gold / BI | Janela fixa 1990–2050; sentinelas -1 e -2. |
| **Gold** | `dim_proponente` | 1 linha / proponente | `identificacao_proponente` | `siconv_proposta` | Delta Lake (tabela) | Não aplicável (dimensão) | Fatos Gold / BI | Pode conter CNPJ ou CPF; grão preservado sem MAX/MIN. |
| **Gold** | `dim_municipio` | 1 linha / município IBGE | `codigo_municipio_ibge` | `siconv_proposta` | Delta Lake (tabela) | Não aplicável (dimensão) | Fatos Gold / BI | Dimensão geográfica derivada dos registros oficiais. |
| **Gold** | `dim_orgao` | 1 linha / órgão | `codigo_orgao` | Propostas e Programas | Delta Lake (tabela) | Não aplicável (dimensão) | Fatos Gold / BI | Dimensão institucional conformada (superior e concedente). |
| **Gold** | `dim_programa` | 1 linha / programa | `id_programa` | `siconv_programa_cadastral` | Delta Lake (tabela) | Não aplicável (dimensão) | Fatos Gold / BI | Não usar `codigo_programa` como chave primária. |
| **Gold** | `fct_proposta` | 1 linha / proposta | `id_proposta` | `siconv_proposta` | Delta Lake (tabela) | Perfeitamente aditiva | Semantic / Serving / BI | Não ligar diretamente com bridge sem agregação prévia. |
| **Gold** | `bridge_programa_proposta` | 1 linha / par associativo | (`id_programa`, `id_proposta`) | `siconv_programa_proposta` | Delta Lake (tabela) | **Não aditiva financeiramente** | Semantic / BI | Estritamente restrita a contagens distintas. |
| **Gold** | `fct_convenio` | 1 linha / instrumento canônico | `numero_convenio` | `siconv_convenio` | Delta Lake (tabela) | Perfeitamente aditiva | Semantic / Serving / BI | Relação 1:1 estrita com proposta; não contém `valor_saldo_conta`. |
| **Gold** | `fct_convenio_saldo_observacao`| 1 linha / observação física | `id_convenio_observacao` | `siconv_convenio` | Delta Lake (tabela) | **Observacional (não aditiva)**| Auditoria Técnica | Isola discrepâncias de saldo e registros concorrentes. |
| **Semantic**| `vw_superset_proposta_convenio`| 1 linha / proposta | `id_proposta` | Fatos e Dimensões Gold | View (lógica) | Aditiva no grão proposta | Serving Mart / BI | Propostas sem convênio via LEFT JOIN; sem bridge e sem saldo. |
| **Serving** | `mart_superset_proposta_convenio`| 1 linha / proposta | `id_proposta` | `vw_superset_proposta_convenio` | Delta Lake (tabela) | Aditiva no grão proposta | Apache Superset / BI | Materialização de alta performance para o dashboard oficial. |
