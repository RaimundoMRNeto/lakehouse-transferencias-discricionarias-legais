-- Reconciliacao financeira dinamica de convenios entre vw_superset_proposta_convenio e fct_convenio (tolerancia zero)
WITH view_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado
    FROM {{ ref('vw_superset_proposta_convenio') }}
),
fct_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado
    FROM {{ ref('fct_convenio') }}
)
SELECT *
FROM view_financials v
JOIN fct_financials f ON 1=1
WHERE v.sum_global != f.sum_global
   OR v.sum_repasse != f.sum_repasse
   OR v.sum_empenhado != f.sum_empenhado
   OR v.sum_desembolsado != f.sum_desembolsado
