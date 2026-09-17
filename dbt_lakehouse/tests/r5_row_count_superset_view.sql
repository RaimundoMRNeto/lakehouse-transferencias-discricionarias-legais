-- Valida que a contagem de linhas da view semântica coincide dinamicamente com fct_proposta
WITH view_cnt AS (
    SELECT COUNT(*) AS cnt_view FROM {{ ref('vw_superset_proposta_convenio') }}
),
fct_cnt AS (
    SELECT COUNT(*) AS cnt_fct FROM {{ ref('fct_proposta') }}
)
SELECT *
FROM view_cnt v
JOIN fct_cnt f ON 1=1
WHERE v.cnt_view != f.cnt_fct
