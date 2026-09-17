-- Valida consistencia do flag tem_convenio com a presenca de numero_convenio
SELECT
    id_proposta,
    tem_convenio,
    numero_convenio
FROM {{ ref('vw_superset_proposta_convenio') }}
WHERE (tem_convenio = TRUE AND numero_convenio IS NULL)
   OR (tem_convenio = FALSE AND numero_convenio IS NOT NULL)
