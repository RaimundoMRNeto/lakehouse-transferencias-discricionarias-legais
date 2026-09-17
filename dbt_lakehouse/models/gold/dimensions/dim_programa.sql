{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT
    id_programa,
    codigo_programa,
    nome_programa,
    situacao_programa,
    ano_disponibilizacao,
    codigo_orgao_superior_programa,
    {{ date_to_gold_sk('data_disponibilizacao') }} AS data_disponibilizacao_sk
FROM {{ ref('siconv_programa_cadastral') }}
