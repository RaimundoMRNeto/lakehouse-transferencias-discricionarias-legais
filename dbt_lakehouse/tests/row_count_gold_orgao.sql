-- Valida reconciliação dinâmica de contagem de linhas de gold.dim_orgao contra os códigos distintos da Silver
WITH silver_orgaos AS (
    SELECT codigo_orgao_superior AS cd FROM {{ ref('siconv_proposta') }}
    UNION
    SELECT codigo_orgao AS cd FROM {{ ref('siconv_proposta') }}
    UNION
    SELECT codigo_orgao_superior_programa AS cd FROM {{ ref('siconv_programa_cadastral') }}
),
silver_cnt AS (
    SELECT COUNT(DISTINCT cd) AS cnt_silver FROM silver_orgaos
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold FROM {{ ref('dim_orgao') }}
)
SELECT *
FROM silver_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
