# Diagnóstico e Resolução de Performance — R5-MVP.2

## 1. Contexto e Finding Físico

Durante a validação interativa no Apache Superset conectado ao Spark Thrift Server, foi observado o seguinte comportamento:
- **KPIs (Big Numbers)**: Carregavam normalmente;
- **Gráficos Analíticos (4 gráficos de barras) e Tabela**: Apresentavam erro de timeout:
  ```text
  Timeout error: Queries are set to timeout after 60 seconds
  ```

Este documento relata o diagnóstico minucioso da causa raiz, a prova matemática formal que viabilizou a simplificação das métricas, a estratégia arquitetural de serving mart físico em Delta Lake e os resultados quantitativos de benchmark comprovando a eliminação definitiva dos timeouts.

---

## 2. Diagnóstico da Causa Raiz

### 2.1 Causa Raiz Primária: Carga Concorrente e Reexecução de Joins da View Semântica
A view semântica `gold.vw_superset_proposta_convenio` realiza 7 joins (`LEFT JOIN` em `fct_proposta`, `fct_convenio`, `dim_proponente`, `dim_municipio`, `dim_orgao` [2x] e `dim_data` [2x]).

Quando o usuário abre o Dashboard Executivo no Superset:
1. O navegador dispara simultaneamente até **12 requisições para os slices** (7 KPIs + 4 gráficos + 1 tabela detalhada) mais as consultas de metadados para os **8 filtros nativos**.
2. No ambiente local conteinerizado, o cluster Apache Spark possui **2 workers com 2 cores cada**, totalizando **4 slots de execução paralela**.
3. Como a view não estava pré-materializada, **cada uma das consultas concorrentes** disparava um plano de execução completo no Spark contendo os 7 joins dimensionais sobre 1.157.619 propostas e 287.584 convênios.
4. Embora uma consulta individual na view levasse entre 5 s e 9 s quando executada isoladamente, o enfileiramento concorrente de ~20 planos pesados saturava os 4 cores do cluster. As consultas do final da fila acumulavam tempo de espera superior aos 60 segundos do timeout do Superset (`SUPERSET_WEBSERVER_TIMEOUT = 60`).

### 2.2 Causa Raiz Secundária: Custo Computacional de `COUNT(DISTINCT)`
As métricas iniciais utilizavam `COUNT(DISTINCT id_proposta)` e `COUNT(DISTINCT numero_convenio)`:
- No Spark SQL, um `COUNT(DISTINCT)` exige particionamento em shuffle (*hash partitioning*) e agregação em duas etapas (*two-phase aggregate*) para garantir unicidade em cluster distribuído.
- Isso adicionava sobrecarga desnecessária de I/O e CPU em cada gráfico agrupado por ano, UF ou órgão.

---

## 3. Demonstração Matemática da Equivalência de Métricas

Para simplificar com segurança as métricas de contagem sem alterar a semântica de negócio, foi analisada a estrutura dimensional da modelagem Gold estabelecida no R4:

### 3.1 Prova de Equivalência para `id_proposta`
1. Na modelagem dimensional Gold, a tabela fato `gold.fct_proposta` tem grão estrito de **1 linha por proposta**. A coluna `id_proposta` é sua chave primária lógica comprovada pelo teste dbt `unique_fct_proposta_id_proposta`.
2. Todos os joins na view semântica são realizados contra dimensões (`dim_*`) com chaves únicas (relação N:1) ou contra `gold.fct_convenio` cuja relação comprovada no R4 é de **1:0..1** (uma proposta relaciona-se com no máximo 1 convênio formalizado).
3. Portanto, não há multiplicação de linhas (fan-out) na junção:
   $$\forall \text{ linha da view}, \quad \text{id\_proposta} \text{ é único e não nulo}.$$
   Consequentemente:
   $$\operatorname{COUNT}(\text{DISTINCT } \text{id\_proposta}) \equiv \operatorname{COUNT}(*) \equiv 1.157.619$$

### 3.2 Prova de Equivalência para `numero_convenio`
1. No R4, a tabela `gold.fct_convenio` possui cardinalidade estrita de 287.584 convênios únicos (`unique_fct_convenio_numero_convenio`).
2. Cada convênio liga-se a exatamente uma proposta (`id_proposta`).
3. Nas propostas não conveniadas, o `LEFT JOIN` produz `numero_convenio = NULL`.
4. Pela semântica SQL padrão (ANSI e Spark SQL), `COUNT(coluna)` ignora valores `NULL`:
   $$\operatorname{COUNT}(\text{DISTINCT } \text{numero\_convenio}) \equiv \operatorname{COUNT}(\text{numero\_convenio}) \equiv 287.584$$

### 3.3 Prova de Equivalência da Taxa de Formalização
A métrica de Propostas com convênio (%) simplifica-se de:
$$\frac{\operatorname{COUNT}(\text{DISTINCT CASE WHEN } \text{numero\_convenio IS NOT NULL THEN } \text{id\_proposta END})}{\operatorname{COUNT}(\text{DISTINCT } \text{id\_proposta})} \times 100$$
para:
$$\frac{\operatorname{COUNT}(\text{numero\_convenio})}{\operatorname{COUNT}(*)} \times 100$$

### 3.4 Validação Automatizada de Paridade
O script `superset/prove_metric_equivalence.py` testou a equivalência em toda a base e em múltiplos agrupamentos dimensionais:
- **Baseline Nacional**:
  - `COUNT(*)` = 1.157.619 | `COUNT(DISTINCT id_proposta)` = 1.157.619 (Diferença: **0**)
  - `COUNT(numero_convenio)` = 287.584 | `COUNT(DISTINCT numero_convenio)` = 287.584 (Diferença: **0**)
  - Taxa de formalização: **24,8427177%** vs **24,8427177%** (Diferença: **0,0000000%**)
- **Agrupado por Ano da Proposta**: 19 anos testados, **0 divergências**.
- **Agrupado por Ano de Assinatura**: 19 anos testados, **0 divergências**.
- **Agrupado por UF**: 27 estados testados, **0 divergências**.
- **Agrupado por Órgão Concedente**: Todos os órgãos testados, **0 divergências**.

---

## 4. Arquitetura da Solução: Serving Mart Físico Delta Lake

Para resolver a raiz primária do problema (sobrecarga de 7 joins simultâneos concorrentes), adotou-se o padrão arquitetural canônico de Lakehouse:
- **Contrato Lógico**: A view semântica `gold.vw_superset_proposta_convenio` permanece como a definição central e padronizada das regras de negócio, joins e filtros de integridade;
- **Camada Física de Serving (BI Mart)**: Criação do modelo dbt `gold.mart_superset_proposta_convenio` materializado como tabela física **Delta Lake** (`materialized='table'`, `file_format='delta'`, `schema='gold'`), que armazena fisicamente o resultado da view;
- **Consumo no Superset**: O dataset do Superset foi apontado diretamente para a tabela física `gold.mart_superset_proposta_convenio`.

### 4.1 Linhagem e Governança
```text
[fct_proposta] + [fct_convenio] + [dim_*]
               ↓
[gold.vw_superset_proposta_convenio]  (View semântica / Contrato lógico dbt)
               ↓
[gold.mart_superset_proposta_convenio] (Serving Mart físico / Delta Lake pré-calculado)
               ↓
[Apache Superset Dataset & Dashboard]  (Consultas colunares puras sem joins em runtime)
```

---

## 5. Comparativo de Performance (Benchmarks)

Foram executadas 3 baterias de medição em condições estritamente idênticas no Spark Thrift Server:
1. **Cenário A**: View semântica original com `COUNT(DISTINCT)`;
2. **Cenário B**: View semântica com métricas simplificadas (`COUNT(*)`, `COUNT(col)`);
3. **Cenário C**: Mart físico Delta Lake com métricas simplificadas (Solução Adotada).

### 5.1 Tabela Comparativa de Latência Individual (Q1 a Q6)

| Consulta / Gráfico | Cenário A: View Original (s) | Cenário B: View Otimizada (s) | Cenário C: Mart Físico Delta (s) | Speedup vs Baseline (C vs A) |
|:---|:---:|:---:|:---:|:---:|
| **Q1 — KPI Propostas** | 5,09 s | 4,92 s | **0,52 s** | **9,8x mais rápido** |
| **Q2 — KPI Convênios** | 6,88 s | 5,34 s | **0,68 s** | **10,1x mais rápido** |
| **Q3 — Propostas por ano** | 9,63 s | 5,60 s | **0,80 s** | **12,0x mais rápido** |
| **Q4 — Convênios por ano** | 9,38 s | 5,23 s | **0,84 s** | **11,2x mais rápido** |
| **Q5 — Propostas por UF** | 8,42 s | 5,34 s | **0,80 s** | **10,5x mais rápido** |
| **Q6 — Convênios por órgão (Top 10)** | 9,31 s | 5,51 s | **0,99 s** | **9,4x mais rápido** |

> **Resultado**: Todas as consultas que antes levavam entre 5 e 10 segundos agora executam em **menos de 1 segundo (sub-segundo)**.

---

## 6. Teste de Carga e Concorrência Real (12 Slices Simultâneos)

Para reproduzir a carga exata do navegador do usuário, o script `superset/benchmark_concurrency.py` disparou simultaneamente todos os 12 componentes analíticos do dashboard (7 KPIs + 4 gráficos + 1 tabela de detalhamento) através de uma pool de threads com 6 conexões paralelas.

### 6.1 Resultados do Teste Concorrente no Mart Físico
- **KPI 1 — Propostas**: 3,45 s
- **KPI 2 — Convênios**: 3,71 s
- **KPI 3 — Taxa de Formalização**: 3,62 s
- **KPI 4 — Valor Global Proposto**: 3,64 s
- **KPI 5 — Valor Global Conveniado**: 3,67 s
- **KPI 6 — Valor Empenhado**: 3,73 s
- **KPI 7 — Valor Desembolsado**: 5,56 s
- **Gráfico 1 — Propostas por ano**: 5,55 s
- **Gráfico 2 — Convênios por ano**: 5,53 s
- **Gráfico 3 — Propostas por UF**: 5,54 s
- **Gráfico 4 — Convênios por órgão concedente**: 5,32 s
- **Tabela — Detalhamento contratual**: 4,99 s

```text
TEMPO TOTAL PARA CARREGAR TODOS OS 12 COMPONENTES CONCORRENTES: 9,36 s
MAIOR TEMPO INDIVIDUAL:                                        5,56 s
MENOR TEMPO INDIVIDUAL:                                        3,45 s
MÉDIA POR COMPONENTE:                                          4,53 s
LIMITE DO TIMEOUT NO SUPERSET:                                60,00 s
MARGEM DE SEGURANÇA:                                         > 54,00 s (10x de folga)
```

---

## 7. Reconciliação Financeira e Integridade (Tolerância Zero: R$ 0,00)

A tabela física `gold.mart_superset_proposta_convenio` foi submetida a testes rigorosos de reconciliação dbt contra a camada Gold e a view semântica. A paridade foi absoluta em todas as métricas:

| Indicador | Camada Gold | View Semântica | Mart Físico Delta | Divergência |
|:---|:---:|:---:|:---:|:---:|
| **Total de Linhas / Propostas** | 1.157.619 | 1.157.619 | 1.157.619 | **0** |
| **Convênios Formalizados** | 287.584 | 287.584 | 287.584 | **0** |
| **Valor Global Proposto (R$)** | 1.495.209.875.334,42 | 1.495.209.875.334,42 | 1.495.209.875.334,42 | **R$ 0,00** |
| **Valor Global Conveniado (R$)** | 356.840.744.533,16 | 356.840.744.533,16 | 356.840.744.533,16 | **R$ 0,00** |
| **Valor Repasse Conveniado (R$)** | 331.271.766.468,34 | 331.271.766.468,34 | 331.271.766.468,34 | **R$ 0,00** |
| **Valor Empenhado (R$)** | 192.064.543.060,79 | 192.064.543.060,79 | 192.064.543.060,79 | **R$ 0,00** |
| **Valor Desembolsado (R$)** | 153.205.012.397,29 | 153.205.012.397,29 | 153.205.012.397,29 | **R$ 0,00** |

---

## 8. Testes dbt e Governança da Solução

### 8.1 Novos Testes dbt Adicionados para o Serving Mart
1. `not_null_mart_superset_proposta_convenio_id_proposta`: PK `id_proposta` não nula.
2. `unique_mart_superset_proposta_convenio_id_proposta`: Unicidade estrita de `id_proposta`.
3. `r5_row_count_mart`: Reconciliação do volume exato de 1.157.619 linhas.
4. `r5_distinct_convenios_mart`: Reconciliação do volume exato de 287.584 convênios.
5. `r5_reconcile_proposta_financials_mart`: Reconciliação de valores de proposta (tolerância R$ 0,00).
6. `r5_reconcile_convenio_financials_mart`: Reconciliação de valores de convênio (tolerância R$ 0,00).

### 8.2 Execução de Testes
- **Build e Idempotência**: Executado duas vezes com sucesso (`PASS=7 WARN=0 ERROR=0`).
- **Suíte Completa Singular do Lakehouse**: 35 testes singulares aprovados (`PASS=35 WARN=0 ERROR=0`).
- **Validação com Filtros Superset**: 5 cenários com filtros executados em `superset/validate_r5_filters.py` com tempos médios de ~1 s e zero erros SQL.
