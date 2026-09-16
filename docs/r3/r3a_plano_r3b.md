# R3-A — Plano de Implementação da Camada Silver (R3-B)

**Documento Orientador para a Etapa R3-B**
**Autor:** Antigravity / Lakehouse Transferegov
**Data:** 2026-09-16
**Status:** PLANO PROPOSTO / AGUARDANDO APROVAÇÃO HUMANA

---

## 1. Proposta Arquitetural da Camada Silver

Com base nas evidências empíricas levantadas no diagnóstico R3-A, a camada Silver **NÃO deve achatar os dados em uma única tabela**. A desnormalização prematura provoca multiplicação exponencial de linhas (até 84x) e inflação bilionária/trilionária de métricas financeiras (+7.176%).

### Arquitetura de Modelos Proposta:

```
Camada Bronze (Snapshot Raw Strings)
  ├── bronze.siconv_programa (1.257.350 linhas)
  ├── bronze.siconv_programa_proposta (1.158.975 linhas)
  ├── bronze.siconv_proposta (1.157.619 linhas)
  └── bronze.siconv_convenio (287.586 linhas)
           │
           ▼ (dbt: staging / silver transformations)
Camada Silver (Entidades Tipadas e Relacionais)
  ├── silver.siconv_programa (ou silver_programa_elegibilidade) [Grão: Programa x Regra de Elegibilidade]
  │     └── [Opcional] silver.siconv_programa_cadastral [Grão: 1 linha por ID_PROGRAMA (53.018)]
  ├── silver.siconv_programa_proposta [Tabela associativa N:N, Grão: (id_programa, id_proposta)]
  ├── silver.siconv_proposta [Grão: 1 linha por Proposta, Chave: id_proposta]
  └── silver.siconv_convenio [Grão: 1 linha por Convênio, Chave: numero_convenio]
           │
           ▼ (Futuro: R4 / Camada Gold)
Camada Gold (Fatos e Dimensões Analíticas / Star Schema)
  ├── dim_programa, dim_proponente, dim_municipio, dim_tempo
  └── fct_proposta, fct_convenio, fct_execucao_financeira
```

### Por que manter entidades separadas na Silver?
1. **Separação de Grãos:** Proposta e Convênio têm grãos de negócio diferentes (1.157.619 propostas vs 287.584 convênios). Apenas 24,8% das propostas se tornam convênio; achatar forçaria 75% da tabela a conter nulos em todas as colunas de convênio.
2. **Cardinalidade N:N:** A relação Programa $\leftrightarrow$ Proposta é estritamente N:N (comprovada por 1.231 propostas multilinkadas e programas com milhares de propostas). A ponte associativa é a única estrutura relacionalmente correta.
3. **Integridade Financeira:** Valores financeiros como `valor_global_proposta` e `valor_global_convenio` residem exclusivamente em suas respectivas entidades, impossibilitando erros de agregação e dupla contagem na Silver.

---

## 2. Estratégia dbt para o R3-B

### 2.1. Estrutura de Diretórios e Camadas no dbt
No projeto `dbt_lakehouse`:
- `models/staging/`: Views ou tabelas de preparação inicial para expor as fontes Bronze (`stg_siconv__*`), renomeando colunas para `snake_case`, padronizando strings vazias para `NULL` e aplicando tipagens primitivas.
- `models/silver/`: Modelos Silver consolidados (`silver_siconv__*` ou `siconv_*` no schema `silver`), com deduplicações pontuais documentadas, validações de chaves e tratamento de regras de negócio de qualidade.

### 2.2. Recomendação de Materialização
- **Recomendação Técnica:** Materializar modelos Silver como **`table`** (Delta Lake).
- **Justificativa:**
  - A camada Bronze atual funciona como **snapshot completo** atualizado via batch (com controle de integridade por SHA256 e `data_carga`).
  - O volume total consolidado das 4 tabelas (~3,8 milhões de linhas) é moderado e perfeitamente gerenciável pelo cluster Spark em poucos segundos/minutos.
  - O uso de `incremental` nesta fase traria complexidade desnecessária de CDC (Change Data Capture) e risco de inconsistência em snapshots que reescrevem partições, sem ganho substancial de performance.
  - A materialização como tabela Delta física no bucket `silver/` garante alto desempenho de leitura analítica para a Gold e para o Trino/Superset.

---

## 3. Roteiro de Implementação R3-B (Passo a Passo)

### R3-B1 — Sources e Contratos dbt
- **Objetivo:** Declarar formalmente os sources da camada Bronze no dbt com catálogo, descrições, tipos e restrições.
- **Arquivos:**
  - `dbt_lakehouse/models/staging/sources_bronze.yml`
- **Dependências:** Conexão Spark Thrift Server validada no `profiles.yml`.
- **Riscos:** Incompatibilidade de tipos de dados nos metadados do Spark catalog.
- **Critério de Aceite:** `dbt compile` e `dbt source freshness` executados com sucesso.

### R3-B2 — Padronização Estrutural e Nomenclatura
- **Objetivo:** Criar modelos staging implementando o mapeamento oficial Bronze $\rightarrow$ Silver em `snake_case`.
- **Arquivos:**
  - `dbt_lakehouse/models/staging/stg_siconv_programa.sql`
  - `dbt_lakehouse/models/staging/stg_siconv_programa_proposta.sql`
  - `dbt_lakehouse/models/staging/stg_siconv_proposta.sql`
  - `dbt_lakehouse/models/staging/stg_siconv_convenio.sql`
- **Dependências:** R3-B1.
- **Riscos:** Erros de digitação de nomes de colunas ou omissão de colunas oficiais.
- **Critério de Aceite:** Todas as colunas oficiais mapeadas com 100% de rastreabilidade.

### R3-B3 — Tipagem e Parsing Seguro
- **Objetivo:** Implementar macros dbt reutilizáveis para conversão segura de datas brasileiras (`dd/MM/yyyy` $\rightarrow$ `DATE`) e valores monetários (substituição de vírgula por ponto $\rightarrow$ `DECIMAL(17, 2)`).
- **Arquivos:**
  - `dbt_lakehouse/macros/parse_date_br.sql`
  - `dbt_lakehouse/macros/parse_decimal_br.sql`
  - `dbt_lakehouse/macros/clean_empty_strings.sql`
- **Dependências:** R3-B2.
- **Riscos:** Parsing quebrar em valores nulos ou sentinelas não previstos.
- **Critério de Aceite:** 100% dos valores numéricos e datas válidos parseados sem falha de execução.

### R3-B4 — Regras de Qualidade e Deduplicação Controlada
- **Objetivo:** Aplicar a regra de deduplicação das 2 linhas de convênio com oscilação de saldo e tratamento de strings sentinelas (`'-'`).
- **Arquivos:**
  - `dbt_lakehouse/models/silver/silver_siconv_convenio.sql`
  - `dbt_lakehouse/models/silver/silver_siconv_proposta.sql`
  - `dbt_lakehouse/models/silver/silver_siconv_programa_proposta.sql`
  - `dbt_lakehouse/models/silver/silver_siconv_programa.sql`
- **Dependências:** R3-B3.
- **Riscos:** Descartar linhas indevidamente.
- **Critério de Aceite:** Convênio reduz exatamente 2 linhas (de 287.586 para 287.584); Proposta, Ponte e Programa mantêm 100% das linhas.

### R3-B5 — Relacionamentos e Testes dbt
- **Objetivo:** Configurar suíte de testes dbt validando chaves primárias, unicidades compostas, valores aceitos e integridade referencial.
- **Arquivos:**
  - `dbt_lakehouse/models/silver/schema.yml`
  - Testes singulares em `dbt_lakehouse/tests/` para validar integridade referencial da ponte.
- **Dependências:** R3-B4.
- **Riscos:** Falha de testes por pré-condições mal calibradas.
- **Critério de Aceite:** `dbt test` executado com 100% de testes verdes (`PASS`).

### R3-B6 — Reconciliação Bronze $\rightarrow$ Silver
- **Objetivo:** Criar script ou teste de reconciliação automatizado comparando métricas de entrada (Bronze) e saída (Silver) para comprovar ausência de perda de dados.
- **Arquivos:**
  - `scripts/reconcile_bronze_silver.py` (ou teste singular dbt)
- **Dependências:** R3-B5.
- **Riscos:** Divergência de centavos decorrente de conversão de ponto flutuante (mitigada pelo uso estrito de `DECIMAL(17, 2)`).
- **Critério de Aceite:** Linhas e somas financeiras reconciliadas com tolerância zero.

### R3-B7 — Documentação, Catálogo e CI
- **Objetivo:** Gerar documentação dbt (`dbt docs generate`), expor no container Airflow/dbt Docs UI e integrar validações ao pipeline de CI.
- **Arquivos:**
  - `docs/r3/r3b_relatorio_entrega.md`
  - Atualização do README e workflows do GitHub Actions se aplicável.
- **Dependências:** R3-B6.
- **Critério de Aceite:** Documentação dbt navegável e CI verde.

---

## 4. Matriz de Riscos do R3-B e Mitigações

| Risco Técnico Identificado | Severidade | Impacto | Ação de Mitigação Planejada |
| :--- | :--- | :--- | :--- |
| **Tentativa de achatar Programa em Proposta/Convênio** | Crítica | Explosão de dados (84x) e distorção financeira de R$ 25,9 trilhões | Bloqueio arquitetural: manter entidades separadas com relacionamento relacional explícito. |
| **Casting de CNPJ/CPF/CEP/Agência para inteiro** | Alta | Perda de zeros à esquerda em 39% dos proponentes e 45% das contas | Regra de contrato: tipagem estrita como `STRING` no schema.yml. |
| **Falha de parsing em `VL_SALDO_CONTA`** | Média | Valores nulos indevidos | Macro de parsing que lida tanto com separador vírgula quanto ponto. |
| **Data aberrante quebrando agregações de tempo** | Média | Erro em análises cronológicas na Gold | Validação de limites históricos (`BETWEEN '1990-01-01' AND '2050-12-31'`). |
| **Falha em teste de `unique` de `id_programa`** | Baixa | Falso positivo no CI | Não aplicar teste `unique` em `id_programa` sem redefinir o grão da tabela. |
