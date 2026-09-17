# R4-A — Catálogo Candidato de Métricas e Indicadores da Camada Gold

Este catálogo define formalmente a semântica, fórmulas de cálculo, dimensões válidas, restrições e comportamento de agregação das métricas candidatas para consumo na camada Gold e no Apache Superset, incorporando as correções conceituais do marco **R4-A.1**.

---

## 1. Princípios de Governança de Métricas e Aditividade

1. **Separação de Eixos Temporais**:
   - **Tempo de Negócio**: Datas do ciclo de vida dos instrumentos (proposta, assinatura, vigência) que particionam registros **dentro do mesmo snapshot**. Medidas de fluxo acumulado são aditivas ao longo desse eixo se cada instrumento pertencer a exatamente um intervalo.
   - **Tempo de Snapshot**: Execuções de carga do Lakehouse ao longo do tempo. O projeto opera em **snapshot corrente único**. Em um modelo futuro de múltiplos snapshots, medidas acumuladas de fluxo ou saldos pontuais tornam-se **semi-aditivas ou não aditivas** ao longo do tempo de snapshot.
2. **Proibição de Soma em Cardinalidade N:N**: Medidas financeiras originadas de fatos de grão Proposta ou Convênio **não podem ser somadas através da dimensão Programa**, pois a associação $N:N$ duplica valores na agregação.
3. **Desacoplamento de Saldo Bancário**: O saldo de conta bancária não possui agregação canônica no tempo nem entre instrumentos, pertencendo à categoria de métricas observacionais pontuais.

---

## 2. Matriz Exaustiva de Aditividade das Medidas da Gold

| ID | Nome Técnico | Entidade / Fato | Mesmo Snapshot (Entre Entidades) | Tempo de Negócio (Partição Intra-Snapshot) | Entre Snapshots Históricos | Via Programa N:N (Bridge) | Classificação Contratual |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **M-001** | `quantidade_propostas` | `fct_proposta` | **ADITIVA** | **ADITIVA** | Não Aditiva (repetição) | Não Aditiva (exige DISTINCT) | **ADITIVA NO SNAPSHOT** |
| **M-002** | `valor_global_proposta` | `fct_proposta` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona +R$ 4,56 bi) | **ADITIVA NO SNAPSHOT** |
| **M-003** | `valor_repasse_proposta` | `fct_proposta` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-004** | `valor_contrapartida_proposta` | `fct_proposta` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-005** | `quantidade_convenios` | `fct_convenio` | **ADITIVA** | **ADITIVA** | Não Aditiva (repetição) | Não Aditiva (exige DISTINCT) | **ADITIVA NO SNAPSHOT** |
| **M-006** | `valor_global_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona +R$ 1,50 bi) | **ADITIVA NO SNAPSHOT** |
| **M-007** | `valor_repasse_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-008** | `valor_contrapartida_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | Não Aditiva (acumula) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-009** | `valor_empenhado_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (fluxo acumulado) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-010** | `valor_desembolsado_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (fluxo acumulado) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-011** | `valor_saldo_remanescente_tesouro` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (saldo pontual) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-012** | `valor_saldo_remanescente_convenente`| `fct_convenio`| **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (saldo pontual) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-013** | `valor_rendimento_aplicacao` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (ganho financeiro acumulado) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-014** | `valor_ingresso_contrapartida`| `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (fluxo financeiro acumulado) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-015** | `valor_global_original_convenio` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (valor estático pactuado) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-016** | `quantidade_termos_aditivos` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (contador cumulativo) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-017** | `quantidade_prorrogacoes` | `fct_convenio` | **ADITIVA** | **ADITIVA** | **SEMI-ADITIVA** (contador cumulativo) | **NÃO ADITIVA** (inflaciona em N:N) | **ADITIVA NO SNAPSHOT** |
| **M-018** | `valor_saldo_conta` (observação) | `fct_convenio_saldo` | **NÃO ADITIVA** | **NÃO ADITIVA** | **NÃO ADITIVA** (saldo bancário instantâneo) | **NÃO ADITIVA** | **OBSERVACIONAL / NÃO ADITIVA** |

---

## 3. Indicadores Analíticos Derivados

| ID | Nome do Indicador | Nome Técnico | Fórmula / Lógica | Aditividade | Interpretação de Negócio e Limitações | Status |
| :---: | :--- | :--- | :--- | :---: | :--- | :---: |
| **I-001** | Taxa de Conveniação Global | `taxa_conveniacao` | $\frac{\text{COUNT(DISTINCT } c.id\_proposta)}{\text{COUNT(DISTINCT } p.id\_proposta)} \times 100$ | Não Aditiva | **Proporção das propostas presentes no snapshot que possuem instrumento formalizado** (24,84% no snapshot R4-A).<br>*Limitação*: Não constitui taxa causal de sucesso nem indicador de desempenho temporal, pois propostas de diferentes coortes e anos possuem tempos distintos de tramitação e maturação até a formalização. | **CONFIRMADA** |
| **I-002** | Ticket Médio da Proposta | `ticket_medio_proposta` | $\frac{\sum valor\_global\_proposta}{\text{COUNT}(id\_proposta)}$ | Não Aditiva | Porte financeiro médio dos planos de trabalho cadastrados (R$ 1,29 milhão no baseline R4-A). Deve ser sempre recalculada pela razão de agregados. | **CONFIRMADA** |
| **I-003** | Ticket Médio do Convênio | `ticket_medio_convenio` | $\frac{\sum valor\_global\_convenio}{\text{COUNT}(numero\_convenio)}$ | Não Aditiva | Porte financeiro médio dos instrumentos conveniados formalizados (R$ 1,24 milhão no baseline R4-A). Deve ser recalculada dinamicamente. | **CONFIRMADA** |
| **I-004** | Percentual de Empenho sobre Repasse | `percentual_empenhado` | $\frac{\sum valor\_empenhado\_convenio}{\sum valor\_repasse\_convenio} \times 100$ | Não Aditiva | Grau de garantia orçamentária do repasse federal pactuado (57,98% no baseline R4-A). Nunca somar percentuais pré-calculados. | **CONFIRMADA** |
| **I-005** | Percentual de Desembolso sobre Repasse | `percentual_desembolsado`| $\frac{\sum valor\_desembolsado\_convenio}{\sum valor\_repasse\_convenio} \times 100$ | Não Aditiva | Ritmo de liberação financeira efetiva dos recursos da União (46,25% no baseline R4-A). Recalcular por numerador e denominador somados. | **CONFIRMADA** |
| **I-006** | Alocação Proporcional por Programa | `valor_proposta_alocado` | $\frac{valor\_global\_proposta}{N}$ | Não Aditiva | Divisão artificial de recursos entre programas vinculados. **Rejeitada**. | **NÃO APROVADA** |

---

## 4. Métricas de Associação a Programas (Cardinalidade N:N)

| ID | Métrica por Programa | Fórmula Segura | Grão Válido | Semântica Correta | Risco e Limitação Crítica | Status |
| :---: | :--- | :--- | :---: | :--- | :--- | :---: |
| **P-001** | Propostas Vinculadas ao Programa | `COUNT(DISTINCT id_proposta)` | Programa | Quantidade de propostas distintas que se candidataram a este programa específico. | Permitida e segura. A soma desta métrica entre vários programas não é aditiva (uma proposta pode constar em mais de um programa). | **CONFIRMADA** |
| **P-002** | Convênios Vinculados ao Programa | `COUNT(DISTINCT numero_convenio)` | Programa | Quantidade de convênios formalizados cujas propostas estavam vinculadas ao programa. | Permitida com aviso de cautela no dashboard: a soma por programa excede o total de convênios devido à cardinalidade $N:N$. | **CONFIRMADA COM RESTRIÇÕES** |
| **P-003** | Soma de Valores de Proposta por Programa | `SUM(valor_global_proposta)` após join com bridge | Programa | **ILEGÍTIMA COMO SOMA GLOBAL** | **BLOQUEADA**. Gera R$ 4,56 bilhões de inflação duplicada em agregados de múltiplos programas. | **BLOQUEADA COMO ADITIVA** |

---

## 5. Métricas de Qualidade e Governança de Dados

| ID | Nome Técnico | Definição | Fórmula Dinâmica | Linha de Base no Snapshot R4-A | Uso Recomendado |
| :---: | :--- | :--- | :--- | :---: | :--- |
| **Q-001** | `qtd_convenios_conflito_saldo` | Quantidade de números de convênio com leituras bancárias divergentes no snapshot. | `COUNT(DISTINCT numero_convenio) WHERE has_source_conflict = TRUE` | **2 convênios** (`949286`, `956078`) | Painel de auditoria do Lakehouse; indicador de divergência bancária da fonte. |
| **Q-002** | `qtd_propostas_orfas_programa` | Propostas que não possuem nenhum programa associado na tabela de ligação. | `COUNT(*) FROM fct_proposta p LEFT JOIN bridge b ON p.id_proposta = b.id_proposta WHERE b.id_proposta IS NULL` | **78 propostas** (R$ 15,42 bi) | Monitoramento de consistência de cadastramento no Transferegov. |
| **Q-003** | `qtd_datas_fora_janela` | Quantidade de datas que caem fora da janela analítica candidata ([1990, 2050]) e recebem chave `-2`. | `COUNT(*) WHERE data_sk = -2` | **132 ocorrências** (10 proposta, 5 convênio, 117 elegibilidade) | Monitoramento de anomalias de calendário da fonte. |

---

## 6. R4-A.1 — Fechamento dos Findings da Revisão Humana

- **Aditividade Temporal Reformulada**: A matriz de aditividade agora decompõe explicitamente o comportamento de cada medida entre **mesmo snapshot**, **tempo de negócio** e **entre snapshots históricos**, sanando a ambiguidade anterior.
- **Exemplo Canônico de Desembolso**: O indicador `valor_desembolsado_convenio` foi explicitado como aditivo entre instrumentos no snapshot e por tempo de negócio, mas não aditivo através da ponte de programas nem entre snapshots históricos acumulados.
- **Rendimento, Ingresso e Original Qualificados**: Formalizado que essas grandezas são aditivas no snapshot corrente, mas sua soma ao longo de séries temporais históricas requer modelagem própria de fluxos incrementais.
- **Taxa de Conveniação**: Semântica ajustada para "proporção das propostas presentes no snapshot que possuem instrumento formalizado", afastando interpretação de taxa causal de sucesso.
