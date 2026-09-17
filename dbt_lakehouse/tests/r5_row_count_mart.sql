-- Valida que a contagem de linhas do mart coincide com a view semantica
WITH mart_cnt AS (
    SELECT COUNT(*) AS cnt_mart FROM {{ ref('mart_superset_proposta_convenio') }}
),
view_cnt AS (
    SELECT COUNT(*) AS cnt_view FROM {{ ref('vw_superset_proposta_convenio') }}
)
SELECT *
FROM mart_cnt m
JOIN view_cnt v ON 1=1
WHERE m.cnt_mart != v.cnt_view
