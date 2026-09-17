{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

WITH roles AS (
    SELECT
        codigo_orgao_superior AS codigo_orgao,
        descricao_orgao_superior AS descricao_orgao,
        true AS is_superior,
        false AS is_concedente
    FROM {{ ref('siconv_proposta') }}

    UNION ALL

    SELECT
        codigo_orgao AS codigo_orgao,
        descricao_orgao AS descricao_orgao,
        false AS is_superior,
        true AS is_concedente
    FROM {{ ref('siconv_proposta') }}

    UNION ALL

    SELECT
        codigo_orgao_superior_programa AS codigo_orgao,
        descricao_orgao_superior_programa AS descricao_orgao,
        true AS is_superior,
        false AS is_concedente
    FROM {{ ref('siconv_programa_cadastral') }}
)
SELECT
    codigo_orgao,
    descricao_orgao,
    bool_or(is_superior) AS is_orgao_superior,
    bool_or(is_concedente) AS is_orgao_concedente
FROM roles
GROUP BY
    codigo_orgao,
    descricao_orgao
