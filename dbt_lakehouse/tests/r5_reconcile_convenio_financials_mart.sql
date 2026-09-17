-- Reconciliacao financeira de convenios entre o mart e a view semantica (tolerancia zero)
WITH mart_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado
    FROM {{ ref('mart_superset_proposta_convenio') }}
),
view_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado
    FROM {{ ref('vw_superset_proposta_convenio') }}
)
SELECT *
FROM mart_financials m
JOIN view_financials v ON 1=1
WHERE m.sum_global != v.sum_global
   OR m.sum_repasse != v.sum_repasse
   OR m.sum_empenhado != v.sum_empenhado
   OR m.sum_desembolsado != v.sum_desembolsado
