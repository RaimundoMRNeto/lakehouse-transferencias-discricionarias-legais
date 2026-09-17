# R4-A — Catálogo Candidato de Métricas e Indicadores da Camada Gold

Este catálogo define formalmente a semântica, fórmulas de cálculo, dimensões válidas, restrições e comportamento de agregação das métricas candidatas para consumo na camada Gold e no Apache Superset.

---

## 1. Princípios de Governança de Métricas

1. **Aditividade Segura**: Uma métrica só pode ser somada ao longo de dimensões onde cada unidade elementar pertença a exatamente uma partição da dimensão.
2. **Proibição de Soma em Cardinalidade N:N**: Medidas monetárias originadas de fatos de grão Proposta ou Convênio **não podem ser somadas através da dimensão Programa**, pois a associação N:N duplica valores.
3. **Desacoplamento de Saldo Bancário**: O saldo de conta bancária não possui agregação canônica no tempo nem entre instrumentos, pertencendo à categoria de métricas observacionais pontuais.

---

## 2. Catálogo Geral de Métricas

| ID da Métrica | Nome de Negócio | Nome Técnico | Fato Origem | Fórmula de Cálculo | Grão Válido | Aditividade | Dimensões Seguras | Dimensões Restritas / Perigosas | Tratamento NULL/Zero | Status |
| :---: | :--- | :--- | :---: | :--- | :---: | :---: | :--- | :--- | :--- | :---: |
| **M-001** | Quantidade de Propostas | `quantidade_propostas` | `fct_proposta` | `COUNT(id_proposta)` | Proposta | Totalmente Aditiva | Tempo, Proponente, Município, UF, Órgão, Modalidade | **Programa** (exige COUNT DISTINCT) | Zero se vazio | **CONFIRMADA** |
| **M-002** | Valor Global das Propostas | `valor_global_propostas` | `fct_proposta` | `SUM(valor_global_proposta)` | Proposta | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Modalidade | **Programa** (inflaciona +R$ 4,56 bi) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-003** | Valor de Repasse das Propostas | `valor_repasse_propostas` | `fct_proposta` | `SUM(valor_repasse_proposta)` | Proposta | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Modalidade | **Programa** (inflaciona em N:N) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-004** | Valor de Contrapartida das Propostas | `valor_contrapartida_propostas` | `fct_proposta` | `SUM(valor_contrapartida_proposta)` | Proposta | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Modalidade | **Programa** (inflaciona em N:N) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-005** | Quantidade de Convênios | `quantidade_convenios` | `fct_convenio` | `COUNT(numero_convenio)` | Convênio | Totalmente Aditiva | Tempo, Proponente, Município, UF, Órgão, Situação | **Programa** (exige COUNT DISTINCT) | Zero se vazio | **CONFIRMADA** |
| **M-006** | Valor Global Conveniado | `valor_global_convenios` | `fct_convenio` | `SUM(valor_global_convenio)` | Convênio | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Situação | **Programa** (inflaciona +R$ 1,50 bi) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-007** | Valor de Repasse Conveniado | `valor_repasse_convenios` | `fct_convenio` | `SUM(valor_repasse_convenio)` | Convênio | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Situação | **Programa** (inflaciona em N:N) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-008** | Valor de Contrapartida Conveniado | `valor_contrapartida_convenios` | `fct_convenio` | `SUM(valor_contrapartida_convenio)` | Convênio | Aditiva (Exceto Programa) | Tempo, Proponente, Município, UF, Órgão, Situação | **Programa** (inflaciona em N:N) | COALESCE(val, 0) | **CONFIRMADA** |
| **M-009** | Valor Empenhado | `valor_empenhado` | `fct_convenio` | `SUM(valor_empenhado_convenio)` | Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão, Situação | **Tempo** (é saldo acumulado), **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-010** | Valor Desembolsado | `valor_desembolsado` | `fct_convenio` | `SUM(valor_desembolsado_convenio)` | Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão, Situação | **Tempo** (é saldo acumulado), **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-011** | Saldo Remanescente Tesouro | `saldo_remanescente_tesouro` | `fct_convenio` | `SUM(valor_saldo_remanescente_tesouro)`| Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão | **Tempo** (é posição pontual), **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-012** | Saldo Remanescente Convenente | `saldo_remanescente_convenente` | `fct_convenio` | `SUM(valor_saldo_remanescente_convenente)` | Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão | **Tempo** (é posição pontual), **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-013** | Rendimento de Aplicação | `rendimento_aplicacao` | `fct_convenio` | `SUM(valor_rendimento_aplicacao)` | Convênio | Aditiva | Tempo, Proponente, Município, UF, Órgão | **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-014** | Ingresso de Contrapartida | `ingresso_contrapartida` | `fct_convenio` | `SUM(valor_ingresso_contrapartida)` | Convênio | Aditiva | Tempo, Proponente, Município, UF, Órgão | **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-015** | Valor Global Original | `valor_global_original` | `fct_convenio` | `SUM(valor_global_original_convenio)` | Convênio | Aditiva | Tempo, Proponente, Município, UF, Órgão | **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-016** | Quantidade de Termos Aditivos | `quantidade_aditivos` | `fct_convenio` | `SUM(quantidade_termos_aditivos)` | Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão | **Tempo**, **Programa** | COALESCE(val, 0) | **CONFIRMADA** |
| **M-017** | Quantidade de Prorrogações | `quantidade_prorrogacoes` | `fct_convenio` | `SUM(quantidade_prorrogacoes)` | Convênio | Semi-Aditiva (Tempo) | Proponente, Município, UF, Órgão | **Tempo**, **Programa** | COALESCE(val, 0) | **CONFIRMADA** |

---

## 3. Indicadores Analíticos Derivados

| ID | Nome do Indicador | Nome Técnico | Numerador | Denominador | Fórmula / Lógica | Aditividade | Interpretação de Negócio | Status |
| :---: | :--- | :--- | :--- | :--- | :--- | :---: | :--- | :---: |
| **I-001** | Taxa de Conveniação Global | `taxa_conveniacao` | Propostas distintas com convênio | Total de propostas distintas | $\frac{\text{COUNT(DISTINCT } c.id\_proposta)}{\text{COUNT(DISTINCT } p.id\_proposta)} \times 100$ | Não Aditiva | Percentual de projetos apresentados que lograram celebração de instrumento jurídico federal. | **CONFIRMADA** (24,84%) |
| **I-002** | Ticket Médio da Proposta | `ticket_medio_proposta` | $\sum valor\_global\_proposta$ | $\text{COUNT}(id\_proposta)$ | $\frac{\sum valor\_global\_proposta}{\text{COUNT}(id\_proposta)}$ | Não Aditiva | Porte financeiro médio dos planos de trabalho cadastrados. | **CONFIRMADA** (R$ 1,29 mi) |
| **I-003** | Ticket Médio do Convênio | `ticket_medio_convenio` | $\sum valor\_global\_convenio$ | $\text{COUNT}(numero\_convenio)$ | $\frac{\sum valor\_global\_convenio}{\text{COUNT}(numero\_convenio)}$ | Não Aditiva | Porte financeiro médio dos instrumentos conveniados formalizados. | **CONFIRMADA** (R$ 1,24 mi) |
| **I-004** | Percentual de Empenho sobre Repasse | `percentual_empenhado` | $\sum valor\_empenhado$ | $\sum valor\_repasse$ | $\frac{\sum valor\_empenhado\_convenio}{\sum valor\_repasse\_convenio} \times 100$ | Não Aditiva | Grau de garantia orçamentária do repasse federal pactuado. | **CONFIRMADA** (57,98%) |
| **I-005** | Percentual de Desembolso sobre Repasse | `percentual_desembolsado`| $\sum valor\_desembolsado$ | $\sum valor\_repasse$ | $\frac{\sum valor\_desembolsado\_convenio}{\sum valor\_repasse\_convenio} \times 100$ | Não Aditiva | Ritmo de liberação financeira efetiva dos recursos da União. | **CONFIRMADA** (46,25%) |
| **I-006** | Alocação Proporcional por Programa | `valor_proposta_alocado` | $valor\_global\_proposta$ | Qtd. Programas Vinculados ($N$) | $\frac{valor\_global\_proposta}{N}$ | Não Aditiva | Partição fictícia de recursos entre programas vinculados. | **NÃO APROVADA / REJEITADA** |

---

## 4. Métricas de Associação a Programas (Cardinalidade N:N)

| ID | Métrica por Programa | Fórmula Segura | Grão Válido | Semântica Correta | Risco e Limitação Crítica | Status |
| :---: | :--- | :--- | :---: | :--- | :--- | :---: |
| **P-001** | Propostas Vinculadas ao Programa | `COUNT(DISTINCT id_proposta)` | Programa | Quantidade de propostas distintas que se candidataram a este programa específico. | Permitida e segura. A soma desta métrica entre vários programas não é aditiva (uma proposta pode constar em mais de um programa). | **CONFIRMADA** |
| **P-002** | Convênios Vinculados ao Programa | `COUNT(DISTINCT numero_convenio)` | Programa | Quantidade de convênios formalizados cujas propostas estavam vinculadas ao programa. | Permitida com aviso de cautela no dashboard: não soma 287.584 se agrupada por programa. | **CONFIRMADA COM RESTRIÇÕES** |
| **P-003** | Soma de Valores de Proposta por Programa | `SUM(valor_global_proposta)` após join com bridge | Programa | **ILEGÍTIMA COMO SOMA GLOBAL** | **BLOQUEADA**. Gera R$ 4,56 bilhões de inflação duplicada em agregados de múltiplos programas. | **BLOQUEADA COMO ADITIVA** |

---

## 5. Métricas de Qualidade e Governança de Dados

Estas métricas técnicas serão expostas para monitoramento de integridade e auditoria:

| ID | Nome Técnico | Definição | Fórmula | Linha de Base no Snapshot | Uso Recomendado |
| :---: | :--- | :--- | :--- | :---: | :--- |
| **Q-001** | `qtd_convenios_conflito_saldo` | Quantidade de números de convênio que apresentam leituras bancárias divergentes no snapshot. | `COUNT(DISTINCT numero_convenio) WHERE has_source_conflict = TRUE` | **2 convênios** (`949286`, `956078`) | Painel de auditoria do Lakehouse; indicador de divergência bancária da fonte. |
| **Q-002** | `qtd_propostas_orfas_programa` | Propostas que não possuem nenhum programa associado na tabela de ligação. | `COUNT(*) FROM fct_proposta p LEFT JOIN bridge b WHERE b.id_proposta IS NULL` | **78 propostas** (R$ 15,42 bi) | Monitoramento de consistência de cadastramento no Transferegov. |
| **Q-003** | `qtd_datas_fora_dominio` | Quantidade de datas com anos anômalos (<1990 ou >2050) tratadas com chave sentinela `-2`. | `COUNT(*) WHERE data_sk = -2` | **132 ocorrências** (10 em proposta, 5 em convênio, 117 em elegibilidade) | Monitoramento de digitação inválida na origem. |
