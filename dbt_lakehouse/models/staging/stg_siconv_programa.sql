with source as (
    select * from {{ source('bronze', 'siconv_programa') }}
),

renamed_and_typed as (
    select
        {{ sha256_surrogate_key([
            'ID_PROGRAMA',
            'MODALIDADE_PROGRAMA',
            'NATUREZA_JURIDICA_PROGRAMA',
            'UF_PROGRAMA',
            'ACAO_ORCAMENTARIA'
        ]) }} as id_programa_elegibilidade,
        try_cast(ID_PROGRAMA as bigint) as id_programa,
        {{ clean_string('COD_ORGAO_SUP_PROGRAMA') }} as codigo_orgao_superior_programa,
        {{ clean_string('DESC_ORGAO_SUP_PROGRAMA') }} as descricao_orgao_superior_programa,
        {{ clean_string('COD_PROGRAMA') }} as codigo_programa,
        {{ clean_string('NOME_PROGRAMA') }} as nome_programa,
        {{ clean_string('SIT_PROGRAMA') }} as situacao_programa,
        {{ parse_date_br('DATA_DISPONIBILIZACAO') }} as data_disponibilizacao,
        try_cast(ANO_DISPONIBILIZACAO as smallint) as ano_disponibilizacao,
        {{ parse_date_br('DT_PROG_INI_RECEB_PROP') }} as data_inicio_recebimento_proposta,
        {{ parse_date_br('DT_PROG_FIM_RECEB_PROP') }} as data_fim_recebimento_proposta,
        {{ parse_date_br('DT_PROG_INI_EMENDA_PAR') }} as data_inicio_emenda_parlamentar,
        {{ parse_date_br('DT_PROG_FIM_EMENDA_PAR') }} as data_fim_emenda_parlamentar,
        {{ parse_date_br('DT_PROG_INI_BENEF_ESP') }} as data_inicio_beneficiario_especifico,
        {{ parse_date_br('DT_PROG_FIM_BENEF_ESP') }} as data_fim_beneficiario_especifico,
        {{ clean_string('MODALIDADE_PROGRAMA') }} as modalidade_programa,
        {{ clean_string('NATUREZA_JURIDICA_PROGRAMA') }} as natureza_juridica_programa,
        upper({{ clean_string('UF_PROGRAMA') }}) as uf_programa,
        {{ clean_string('ACAO_ORCAMENTARIA') }} as acao_orcamentaria,
        {{ clean_string('NOME_SUBTIPO_PROGRAMA') }} as nome_subtipo_programa,
        {{ clean_string('DESCRICAO_SUBTIPO_PROGRAMA') }} as descricao_subtipo_programa,
        __ingested_at_utc,
        __ingestion_run_id,
        __source_file,
        __source_sha256
    from source
)

select * from renamed_and_typed
