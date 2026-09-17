-- Valida reconciliação dinâmica de contagem de linhas da ponte entre Silver e Gold
WITH silver_cnt AS (
    SELECT COUNT(*) AS cnt_silver FROM {{ ref('siconv_programa_proposta') }}
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold FROM {{ ref('bridge_programa_proposta') }}
)
SELECT *
FROM silver_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
