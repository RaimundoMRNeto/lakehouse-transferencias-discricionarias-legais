{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT DISTINCT
    codigo_municipio_ibge,
    municipio_proponente AS nome_municipio,
    uf_proponente AS sigla_uf
FROM {{ ref('siconv_proposta') }}
