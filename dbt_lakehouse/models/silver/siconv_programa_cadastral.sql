with staging as (
    select * from {{ ref('stg_siconv_programa') }}
),

cadastral as (
    select distinct
        id_programa,
        codigo_orgao_superior_programa,
        descricao_orgao_superior_programa,
        codigo_programa,
        nome_programa,
        situacao_programa,
        data_disponibilizacao,
        ano_disponibilizacao,
        __ingested_at_utc,
        __ingestion_run_id,
        __source_file,
        __source_sha256
    from staging
)

select
    id_programa,
    codigo_orgao_superior_programa,
    descricao_orgao_superior_programa,
    codigo_programa,
    nome_programa,
    situacao_programa,
    data_disponibilizacao,
    ano_disponibilizacao,
    __ingested_at_utc,
    __ingestion_run_id,
    __source_file,
    __source_sha256
from cadastral
