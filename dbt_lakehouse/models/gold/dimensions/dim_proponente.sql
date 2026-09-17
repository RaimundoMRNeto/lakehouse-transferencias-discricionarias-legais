{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT DISTINCT
    identificacao_proponente,
    nome_proponente,
    codigo_municipio_ibge,
    municipio_proponente,
    uf_proponente,
    cep_proponente,
    endereco_proponente,
    bairro_proponente,
    natureza_juridica
FROM {{ ref('siconv_proposta') }}
