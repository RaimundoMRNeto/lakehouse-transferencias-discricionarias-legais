-- Valida que o par (id_programa, id_proposta) é estritamente único na ponte Gold
SELECT
    id_programa,
    id_proposta,
    COUNT(*) AS cnt
FROM {{ ref('bridge_programa_proposta') }}
GROUP BY
    id_programa,
    id_proposta
HAVING COUNT(*) > 1
