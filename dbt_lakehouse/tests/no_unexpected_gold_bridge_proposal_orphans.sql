-- Valida que não existem novos órfãos de proposta na bridge da Gold além dos históricos conhecidos
SELECT DISTINCT
    b.id_proposta
FROM {{ ref('bridge_programa_proposta') }} b
LEFT JOIN {{ ref('fct_proposta') }} p
    ON b.id_proposta = p.id_proposta
WHERE p.id_proposta IS NULL
  AND b.id_proposta NOT IN {{ known_bridge_orphan_proposals() }}
