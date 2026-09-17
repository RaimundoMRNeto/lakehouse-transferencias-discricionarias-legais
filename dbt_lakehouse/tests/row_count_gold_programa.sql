-- Valida reconciliação dinâmica de contagem de linhas entre silver.siconv_programa_cadastral e gold.dim_programa
WITH silver_cnt AS (
    SELECT COUNT(DISTINCT id_programa) AS cnt_silver
    FROM {{ ref('siconv_programa_cadastral') }}
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold
    FROM {{ ref('dim_programa') }}
)
SELECT *
FROM silver_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
