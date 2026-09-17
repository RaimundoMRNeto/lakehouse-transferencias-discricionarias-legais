{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT
    id_convenio_observacao,
    numero_convenio,
    valor_saldo_conta,
    source_conflict_count,
    has_source_conflict,
    __ingested_at_utc,
    __ingestion_run_id,
    __source_file,
    __source_sha256
FROM {{ ref('siconv_convenio') }}
