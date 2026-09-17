with staging as (
    select * from {{ ref('stg_siconv_programa') }}
)

select
    id_programa_elegibilidade,
    id_programa,
    codigo_orgao_superior_programa,
    descricao_orgao_superior_programa,
    codigo_programa,
    nome_programa,
    situacao_programa,
    data_disponibilizacao,
    ano_disponibilizacao,
    data_inicio_recebimento_proposta,
    data_fim_recebimento_proposta,
    data_inicio_emenda_parlamentar,
    data_fim_emenda_parlamentar,
    data_inicio_beneficiario_especifico,
    data_fim_beneficiario_especifico,
    modalidade_programa,
    natureza_juridica_programa,
    uf_programa,
    acao_orcamentaria,
    nome_subtipo_programa,
    descricao_subtipo_programa,
    __ingested_at_utc,
    __ingestion_run_id,
    __source_file,
    __source_sha256
from staging
