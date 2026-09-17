-- Reconciliacao financeira de propostas entre o mart e a view semantica (tolerancia zero)
WITH mart_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('mart_superset_proposta_convenio') }}
),
view_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('vw_superset_proposta_convenio') }}
)
SELECT *
FROM mart_financials m
JOIN view_financials v ON 1=1
WHERE m.sum_global != v.sum_global
   OR m.sum_repasse != v.sum_repasse
   OR m.sum_contrapartida != v.sum_contrapartida
