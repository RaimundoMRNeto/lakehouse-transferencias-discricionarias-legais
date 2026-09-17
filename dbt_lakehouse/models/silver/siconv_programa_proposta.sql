with staging as (
    select * from {{ ref('stg_siconv_programa_proposta') }}
)

select
    id_programa,
    id_proposta,
    __ingested_at_utc,
    __ingestion_run_id,
    __source_file,
    __source_sha256
from staging
