-- Reconciliacao financeira dinamica de propostas entre vw_superset_proposta_convenio e fct_proposta (tolerancia zero)
WITH view_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('vw_superset_proposta_convenio') }}
),
fct_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('fct_proposta') }}
)
SELECT *
FROM view_financials v
JOIN fct_financials f ON 1=1
WHERE v.sum_global != f.sum_global
   OR v.sum_repasse != f.sum_repasse
   OR v.sum_contrapartida != f.sum_contrapartida
