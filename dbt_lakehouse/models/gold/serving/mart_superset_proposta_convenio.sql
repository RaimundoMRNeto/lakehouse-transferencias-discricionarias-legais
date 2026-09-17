{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT *
FROM {{ ref('vw_superset_proposta_convenio') }}
