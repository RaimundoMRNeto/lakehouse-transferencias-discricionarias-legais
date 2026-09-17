-- Valida convenios e convenios distintos no mart vs fct_convenio
WITH mart_cnt AS (
    SELECT
        COUNT(numero_convenio) AS cnt_conv_mart,
        COUNT(DISTINCT numero_convenio) AS cnt_dist_conv_mart
    FROM {{ ref('mart_superset_proposta_convenio') }}
),
fct_cnt AS (
    SELECT COUNT(*) AS cnt_fct FROM {{ ref('fct_convenio') }}
)
SELECT *
FROM mart_cnt m
JOIN fct_cnt f ON 1=1
WHERE m.cnt_conv_mart != f.cnt_fct
   OR m.cnt_dist_conv_mart != f.cnt_fct
