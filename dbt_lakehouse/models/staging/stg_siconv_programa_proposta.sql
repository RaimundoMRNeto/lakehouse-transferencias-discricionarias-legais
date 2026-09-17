with source as (
    select * from {{ source('bronze', 'siconv_programa_proposta') }}
),

renamed_and_typed as (
    select
        try_cast(ID_PROGRAMA as bigint) as id_programa,
        try_cast(ID_PROPOSTA as bigint) as id_proposta,
        __ingested_at_utc,
        __ingestion_run_id,
        __source_file,
        __source_sha256
    from source
)

select * from renamed_and_typed
