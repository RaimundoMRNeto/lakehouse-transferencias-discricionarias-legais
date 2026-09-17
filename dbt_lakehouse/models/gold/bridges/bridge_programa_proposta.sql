{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT
    id_programa,
    id_proposta
FROM {{ ref('siconv_programa_proposta') }}
