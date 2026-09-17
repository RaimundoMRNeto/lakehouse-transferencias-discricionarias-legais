-- Reconciliação financeira dinâmica de fct_proposta contra silver.siconv_proposta com tolerância zero
WITH silver_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('siconv_proposta') }}
),
gold_financials AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida
    FROM {{ ref('fct_proposta') }}
)
SELECT *
FROM silver_financials s
JOIN gold_financials g ON 1=1
WHERE s.sum_global != g.sum_global
   OR s.sum_repasse != g.sum_repasse
   OR s.sum_contrapartida != g.sum_contrapartida
