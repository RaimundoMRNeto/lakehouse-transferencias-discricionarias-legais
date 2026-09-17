# Relatório de Entrega — R5-MVP: View Semântica + Dashboard Executivo no Apache Superset

## 1. Objetivo da Etapa R5-MVP
A etapa **R5-MVP** entrega a camada analítica de consumo e visualização executiva do Lakehouse de Transferências Discricionárias e Legais da União (Siconv / Transferegov). O objetivo primordial é prover um produto analítico ponta a ponta, demonstrável, reprodutível, semanticamente íntegro e aderente à governança estabelecida nos ciclos R1 a R4.

Esta entrega contempla:
1. **1 View Semântica dbt**: `gold.vw_superset_proposta_convenio`
2. **1 Dataset no Apache Superset**: `Transferências — Propostas e Convênios` (`gold.vw_superset_proposta_convenio`)
3. **1 Dashboard Executivo no Apache Superset**: `Transferências Discricionárias e Legais — Visão Geral`
4. **7 Métricas Oficiais**: Propostas, Convênios formalizados, Propostas com convênio (%), Valor global proposto, Valor global conveniado, Valor empenhado, Valor desembolsado (+ 2 opcionais de eficiência financeira)
5. **4 Gráficos Analíticos**: Propostas por ano, Convênios formalizados por ano, Propostas por UF, Convênios por órgão concedente (Top 10)
6. **1 Tabela Detalhada**: Detalhamento com paginação de 13 atributos contratuais
7. **8 Filtros Nativos**: Ano da proposta, Ano da assinatura, UF, Município, Órgão concedente, Modalidade, Situação da proposta, Situação do convênio
8. **5 Testes de Reconciliação Contratual dbt**: Testes singulares cobrindo volume, unicidade, cardinalidade e reconciliação financeira a nível de centavos
9. **Export dos Assets Superset**: 16 arquivos YAML versionados em `superset/assets/r5_mvp/`
10. **Scripts de Reprodução e Validação**: `superset/setup_r5_assets.py` e `superset/validate_r5_filters.py`

---

## 2. Arquitetura Analítica de Ponta a Ponta

Para assegurar integridade analítica e governança centralizada, nenhuma lógica de transformação crítica fica dispersa na ferramenta de BI. O Superset atua estritamente como camada de visualização sobre o Lakehouse:

```text
Transferegov (Dados Abertos / Siconv)
     ↓
Bronze (Raw Ingestion / Parquet Auditável)
     ↓
Silver (Typed & Cleaned / Delta Lake)
     ↓
Gold (Star Schema Dimensional / Delta Lake)
     ↓
gold.vw_superset_proposta_convenio (dbt Semantic View)
     ↓
Apache Superset (PyHive / Spark Thrift Server)
     ↓
Dashboard Executivo (KPIs + Gráficos + Filtros Nativos + Tabela)
```

---

## 3. View Semântica (`gold.vw_superset_proposta_convenio`)

### 3.1 Grão Contratual
- **Grão**: Exatamente **1 linha por proposta** (`id_proposta`).
- **PK Lógica**: `id_proposta`.
- **Relacionamento Proposta ↔ Convênio**: Como comprovado no R4, uma proposta relaciona-se com **0..1 convênio**. Portanto, a junção entre `fct_proposta` e `fct_convenio` é realizada obrigatoriamente via `LEFT JOIN`, preservando a integralidade das propostas ainda não formalizadas.

### 3.2 Joins Permitidos e Seguros (Sem Multiplicação de Grão)
Todos os joins utilizam relacionamentos comprovadamente N:1 ou 1:0..1:
- `fct_proposta` (p)
- `LEFT JOIN dim_proponente` (prop) `ON p.identificacao_proponente = prop.identificacao_proponente` (N:1)
- `LEFT JOIN dim_municipio` (mun) `ON p.codigo_municipio_ibge = mun.codigo_municipio_ibge` (N:1)
- `LEFT JOIN dim_orgao` (org_sup) `ON p.codigo_orgao_superior = org_sup.codigo_orgao` (N:1, role: superior)
- `LEFT JOIN dim_orgao` (org_conc) `ON p.codigo_orgao = org_conc.codigo_orgao` (N:1, role: concedente)
- `LEFT JOIN dim_data` (dt_prop) `ON p.data_proposta_sk = dt_prop.data_sk` (N:1)
- `LEFT JOIN fct_convenio` (c) `ON p.id_proposta = c.id_proposta` (1:0..1)
- `LEFT JOIN dim_data` (dt_ass) `ON c.data_assinatura_sk = dt_ass.data_sk` (N:1)

### 3.3 Guardrails e Joins Proibidos
1. **Programa (N:N)**: É expressamente proibida a inclusão de `dim_programa` e `bridge_programa_proposta` nesta view principal. A relação Programa ↔ Proposta é N:N e multiplicaria indevidamente as métricas financeiras.
2. **Saldos Bancários Observacionais**: É proibida a inclusão de `valor_saldo_conta` e `fct_convenio_saldo_observacao`, dado que os saldos possuem natureza temporal observacional pontual (não aditiva).
3. **Colunas Técnicas Excluídas**: Metadados de pipeline (`__ingestion_run_id`, `__source_sha256`, etc.) e dados bancários de detalhe foram removidos da camada de consumo.

### 3.4 Semântica Temporal
A dimensão `dim_data` trata sentinelas (`-1` = Não informado, `-2` = Fora da janela analítica) com atributos civis `NULL`. Consequentemente, para registros sem data informada ou propostas sem convênio, as colunas `data_proposta`, `ano_proposta`, `data_assinatura` e `ano_assinatura` assumem `NULL` de forma legítima e transparente.

---

## 4. Métricas Oficiais do Dataset no Superset

O dataset aponta diretamente para a view semântica física `gold.vw_superset_proposta_convenio` sem joins adicionais no BI. As 7 métricas principais foram implementadas com expressões SQL otimizadas para o Spark SQL:

| ID | Nome da Métrica | Expressão SQL Spark | Descrição |
|:---|:---|:---|:---|
| **M1** | **Propostas** | `COUNT(DISTINCT id_proposta)` | Total de propostas únicas registradas |
| **M2** | **Convênios formalizados** | `COUNT(DISTINCT numero_convenio)` | Total de convênios emitidos |
| **M3** | **Propostas com convênio (%)** | `CASE WHEN COUNT(DISTINCT id_proposta) = 0 THEN NULL ELSE 100.0 * COUNT(DISTINCT CASE WHEN numero_convenio IS NOT NULL THEN id_proposta END) / COUNT(DISTINCT id_proposta) END` | Proporção das propostas presentes no conjunto filtrado que possuem convênio formalizado |
| **M4** | **Valor global proposto** | `SUM(valor_global_proposta)` | Montante financeiro total solicitado |
| **M5** | **Valor global conveniado** | `SUM(valor_global_convenio)` | Montante financeiro pactuado em convênios |
| **M6** | **Valor empenhado** | `SUM(valor_empenhado_convenio)` | Montante empenhado pelo concedente |
| **M7** | **Valor desembolsado** | `SUM(valor_desembolsado_convenio)` | Montante repassado financeiramente ao convenente |
| *Opc.* | *Percentual empenhado* | `CASE WHEN SUM(valor_repasse_convenio) = 0 THEN NULL ELSE 100.0 * SUM(valor_empenhado_convenio) / SUM(valor_repasse_convenio) END` | Eficiência de empenho sobre o repasse |
| *Opc.* | *Percentual desembolsado* | `CASE WHEN SUM(valor_repasse_convenio) = 0 THEN NULL ELSE 100.0 * SUM(valor_desembolsado_convenio) / SUM(valor_repasse_convenio) END` | Eficiência de desembolso sobre o repasse |

---

## 5. Dashboard Executivo e Visualizações

- **Nome**: `Transferências Discricionárias e Legais — Visão Geral`
- **Slug**: `transferencias-visao-geral`
- **URL**: `http://localhost:8088/superset/dashboard/transferencias-visao-geral/`

### 5.1 Estrutura do Layout em Grid (12 Colunas)
```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ HEADER: Transferências Discricionárias e Legais — Visão Geral                         │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ BARRA DE FILTROS NATIVOS (8 filtros retráteis à esquerda)                              │
├───────────────────────────────────┬──────────────────────────────────┬─────────────────┤
│ Propostas (w=4)                   │ Convênios formalizados (w=4)     │ % c/ Convênio(4)│
├──────────────────┬────────────────┴─┬────────────────────────────────┼─────────────────┤
│ Vlr Proposto (3) │ Vlr Conveniado(3)│ Vlr Empenhado (3)              │ Vlr Desembols(3)│
├──────────────────┴──────────────────┴────────────────────────────────┴─────────────────┤
│ Propostas por ano (w=6)             │ Convênios formalizados por ano (w=6)             │
├─────────────────────────────────────┼──────────────────────────────────────────────────┤
│ Propostas por UF (w=6)              │ Convênios por órgão concedente (w=6)             │
├─────────────────────────────────────┴──────────────────────────────────────────────────┤
│ Detalhamento de propostas e convênios (Tabela com 13 colunas e paginação, w=12)        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Componentes e Gráficos
1. **7 Cards KPI (Big Number Total)**: Visão consolidada imediata de volumes operacionais e valores monetários.
2. **Gráfico 1 — Propostas por ano**: Gráfico de barras ordenado cronologicamente, com filtro `ano_proposta IS NOT NULL`.
3. **Gráfico 2 — Convênios formalizados por ano**: Gráfico de barras ordenado cronologicamente, com filtro `ano_assinatura IS NOT NULL`.
4. **Gráfico 3 — Propostas por Unidade da Federação**: Gráfico de barras horizontais/verticais ordenado decrescente por quantidade de propostas.
5. **Gráfico 4 — Convênios por órgão concedente**: Gráfico de barras com o ranking dos Top 10 órgãos federais concedentes.
6. **Tabela Detalhada — Detalhamento de propostas e convênios**: Tabela com paginação nativa (25 linhas/página, limite de 1.000 no lote), permitindo auditoria pontual sem sobrecarga do navegador.

### 5.3 Filtros Nativos (Native Filters)
O dashboard dispõe de 8 filtros nativos aplicados a todos os gráficos:
1. `Ano da proposta` (`ano_proposta`)
2. `Ano da assinatura` (`ano_assinatura`)
3. `UF` (`uf`)
4. `Município` (`municipio`)
5. `Órgão concedente` (`orgao_concedente`)
6. `Modalidade` (`modalidade`)
7. `Situação da proposta` (`situacao_proposta`)
8. `Situação do convênio` (`situacao_convenio`)

> [!NOTE]
> **Comportamento Semântico dos Filtros de Convênio**: Filtros como `Ano da assinatura` e `Situação do convênio` filtram naturalmente registros onde o convênio existe. Nesses casos, a taxa de convênios converge para 100% e as propostas sem convênio são omitidas de forma matematicamente e conceitualmente correta.

---

## 6. Resultados de Reconciliação e Validação

### 6.1 Resultados Oficiais (Baseline sem Filtros)
| Indicador / Métrica | Valor Baseline Lakehouse | Reconciliação R4 | Status |
|:---|:---|:---|:---:|
| **Total de Linhas da View** | 1.157.619 | 1.157.619 (`fct_proposta`) | **PASS** |
| **Propostas Distintas** | 1.157.619 | 1.157.619 (`fct_proposta`) | **PASS** |
| **Convênios Distintos** | 287.584 | 287.584 (`fct_convenio`) | **PASS** |
| **Propostas com Convênio (%)** | 24,84% | ~24,84% | **PASS** |
| **Valor Global Proposto** | R$ 1.495.209.875.334,42 | R$ 1.495.209.875.334,42 | **PASS (Tolerância R$ 0,00)** |
| **Valor Repasse Proposto** | R$ 1.425.758.135.735,63 | R$ 1.425.758.135.735,63 | **PASS (Tolerância R$ 0,00)** |
| **Valor Contrapartida Proposta** | R$ 69.451.779.598,79 | R$ 69.451.779.598,79 | **PASS (Tolerância R$ 0,00)** |
| **Valor Global Conveniado** | R$ 356.840.744.533,16 | R$ 356.840.744.533,16 | **PASS (Tolerância R$ 0,00)** |
| **Valor Repasse Conveniado** | R$ 334.341.282.802,87 | R$ 334.341.282.802,87 | **PASS (Tolerância R$ 0,00)** |
| **Valor Empenhado** | R$ 192.064.543.060,79 | R$ 192.064.543.060,79 | **PASS (Tolerância R$ 0,00)** |
| **Valor Desembolsado** | R$ 153.205.012.397,29 | R$ 153.205.012.397,29 | **PASS (Tolerância R$ 0,00)** |

### 6.2 Bateria de Testes dbt
Executado via comando:
`docker exec -w /home/airflow/dbt_lakehouse airflow dbt build --select vw_superset_proposta_convenio r5_row_count_superset_view r5_distinct_convenios_superset_view r5_reconcile_proposta_financials r5_reconcile_convenio_financials r5_tem_convenio_consistency --profiles-dir .`

- `vw_superset_proposta_convenio`: **OK created view** (0.65s)
- `not_null_vw_superset_proposta_convenio_id_proposta`: **PASS**
- `unique_vw_superset_proposta_convenio_id_proposta`: **PASS**
- `r5_row_count_superset_view`: **PASS**
- `r5_distinct_convenios_superset_view`: **PASS**
- `r5_reconcile_proposta_financials`: **PASS**
- `r5_reconcile_convenio_financials`: **PASS**
- `r5_tem_convenio_consistency`: **PASS**

**Resultado**: 8/8 aprovados (`PASS=8 WARN=0 ERROR=0 SKIP=0`).

### 6.3 Teste Dinâmico de Filtros no Superset
O script `superset/validate_r5_filters.py` submeteu consultas reais ao Spark Thrift Server sob 5 cenários:
1. **Sem filtros (Nacional)**: 1.157.619 propostas | 287.584 convênios (24,84%).
2. **Filtro UF = 'MG'**: 99.370 propostas | 29.757 convênios (29,95%) | R$ 111,62 bilhões propostos.
3. **Filtro Ano da Proposta = 2020**: 28.048 propostas | 11.880 convênios (42,36%) | R$ 34,37 bilhões propostos.
4. **Filtro Modalidade = 'CONVENIO'**: 499.163 propostas | 118.428 convênios (23,73%) | R$ 693,30 bilhões propostos.
5. **Filtro Situação do Convênio = 'Em execução'**: 46.069 propostas | 46.069 convênios (100,00%) | R$ 150,08 bilhões propostos.

Todos os cenários responderam com consistência analítica e sem qualquer erro de sintaxe SQL ou falha de timeout.

---

## 7. Como Acessar o Dashboard e Reproduzir os Assets

### 7.1 Acesso via Navegador
1. Abra o navegador e acesse:
   `http://localhost:8088/superset/dashboard/transferencias-visao-geral/`
2. Credenciais de acesso:
   - **Usuário**: `admin`
   - **Senha**: `admin`
3. Navegue pelos cards superiores de KPI, explore os 4 gráficos e utilize os 8 filtros nativos na barra lateral esquerda.

### 7.2 Reprodução / Importação Automatizada dos Assets
Para reconstruir ou aplicar todos os assets do R5 em um novo ambiente Superset:
```bash
docker exec superset /app/.venv/bin/python3 /app/superset/setup_r5_assets.py
```
O script é idempotente e realiza:
- Criação e validação da conexão JDBC/Hive com o `spark-thrift-server:10000/gold`;
- Registro e introspecção do dataset `vw_superset_proposta_convenio`;
- Configuração das métricas calculadas;
- Provisionamento dos 12 slices;
- Montagem do layout e dos native filters do dashboard;
- Exportação dos arquivos em `superset/assets/r5_mvp/`.

---

## 8. Guia para Captura de Screenshots Manuais

Como o ambiente do agente opera em modo headless sem servidor gráfico ou navegador automatizado instalado (Playwright/Chrome), o usuário pode realizar as capturas oficiais diretamente no seu navegador desktop:

1. **Visão Geral Completa**:
   - Acesse `http://localhost:8088/superset/dashboard/transferencias-visao-geral/`.
   - Aguarde o carregamento completo dos 7 KPIs e dos 4 gráficos.
   - Capture a tela e salve em: `docs/images/r5_dashboard_visao_geral.png`.

2. **Visão com Filtro Aplicado (Ex: UF = 'MG' ou 'SP')**:
   - Na barra lateral esquerda de Filtros Nativos, selecione o filtro `UF`, escolha um estado (ex.: `MG`) e clique em **Apply Filters**.
   - Capture a tela demonstrando os KPIs e gráficos recalculados.
   - Salve em: `docs/images/r5_dashboard_filtro_uf.png`.

---

## 9. Limitações e Próximos Passos
- **Cardinalidade N:N de Programas**: Conforme diretriz contratual, dados de Programa não foram incluídos nesta view para evitar a duplicação de propostas e valores. Um dashboard analítico específico para Programas e Elegibilidade poderá ser concebido futuramente.
- **Saldos Bancários**: A evolução diária de saldos permanece na tabela fato especializada `fct_convenio_saldo_observacao`.
- **Mapas e Georreferenciamento**: O dashboard MVP utiliza gráficos de barras ordenados por UF para garantir máxima estabilidade e performance no Spark Thrift Server sem dependência de polígonos GeoJSON externos.
