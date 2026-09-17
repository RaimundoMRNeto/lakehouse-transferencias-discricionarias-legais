-- Valida reconciliação dinâmica de contagem de linhas entre silver.siconv_proposta e gold.fct_proposta
WITH silver_cnt AS (
    SELECT COUNT(*) AS cnt_silver FROM {{ ref('siconv_proposta') }}
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold FROM {{ ref('fct_proposta') }}
)
SELECT *
FROM silver_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
