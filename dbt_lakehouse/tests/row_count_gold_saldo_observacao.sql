-- Valida reconciliação dinâmica de contagem de linhas entre silver.siconv_convenio e gold.fct_convenio_saldo_observacao
WITH silver_cnt AS (
    SELECT COUNT(*) AS cnt_silver FROM {{ ref('siconv_convenio') }}
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold FROM {{ ref('fct_convenio_saldo_observacao') }}
)
SELECT *
FROM silver_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
