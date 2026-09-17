-- Valida reconciliação dinâmica de contagem de linhas entre silver.siconv_proposta e gold.dim_municipio
WITH silver_distinct AS (
    SELECT COUNT(DISTINCT codigo_municipio_ibge) AS cnt_silver
    FROM {{ ref('siconv_proposta') }}
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold
    FROM {{ ref('dim_municipio') }}
)
SELECT *
FROM silver_distinct s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
