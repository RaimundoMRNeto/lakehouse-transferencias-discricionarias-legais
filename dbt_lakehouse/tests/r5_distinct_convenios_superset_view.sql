-- Valida que a quantidade de convenios distintos na view coincide dinamicamente com fct_convenio
WITH view_cnt AS (
    SELECT COUNT(DISTINCT numero_convenio) AS cnt_convenio_view FROM {{ ref('vw_superset_proposta_convenio') }}
),
fct_cnt AS (
    SELECT COUNT(*) AS cnt_convenio_fct FROM {{ ref('fct_convenio') }}
)
SELECT *
FROM view_cnt v
JOIN fct_cnt f ON 1=1
WHERE v.cnt_convenio_view != f.cnt_convenio_fct
