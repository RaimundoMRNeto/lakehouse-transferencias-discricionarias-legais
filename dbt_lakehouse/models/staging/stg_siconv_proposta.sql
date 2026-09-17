with source as (
    select * from {{ source('bronze', 'siconv_proposta') }}
),

renamed_and_typed as (
    select
        try_cast(ID_PROPOSTA as bigint) as id_proposta,
        upper({{ clean_string('UF_PROPONENTE') }}) as uf_proponente,
        {{ clean_string('MUNIC_PROPONENTE') }} as municipio_proponente,
        {{ clean_string('COD_MUNIC_IBGE') }} as codigo_municipio_ibge,
        {{ clean_string('COD_ORGAO_SUP') }} as codigo_orgao_superior,
        {{ clean_string('DESC_ORGAO_SUP') }} as descricao_orgao_superior,
        {{ clean_string('NATUREZA_JURIDICA') }} as natureza_juridica,
        {{ clean_string('NR_PROPOSTA') }} as numero_proposta,
        try_cast(DIA_PROP as smallint) as dia_proposta,
        try_cast(MES_PROP as smallint) as mes_proposta,
        try_cast(ANO_PROP as smallint) as ano_proposta,
        {{ parse_date_br('DIA_PROPOSTA') }} as data_proposta,
        {{ clean_string('COD_ORGAO') }} as codigo_orgao,
        {{ clean_string('DESC_ORGAO') }} as descricao_orgao,
        {{ clean_string('MODALIDADE') }} as modalidade,
        {{ clean_string('IDENTIF_PROPONENTE') }} as identificacao_proponente,
        {{ clean_string('NM_PROPONENTE') }} as nome_proponente,
        {{ clean_string('CEP_PROPONENTE') }} as cep_proponente,
        {{ clean_string('ENDERECO_PROPONENTE') }} as endereco_proponente,
        {{ clean_string('BAIRRO_PROPONENTE') }} as bairro_proponente,
        {{ clean_string('NM_BANCO') }} as nome_banco,
        {{ clean_string('SITUACAO_CONTA') }} as situacao_conta,
        {{ clean_string('SITUACAO_PROJETO_BASICO') }} as situacao_projeto_basico,
        {{ clean_string('SIT_PROPOSTA') }} as situacao_proposta,
        {{ parse_date_br('DIA_INIC_VIGENCIA_PROPOSTA') }} as data_inicio_vigencia_proposta,
        {{ parse_date_br('DIA_FIM_VIGENCIA_PROPOSTA') }} as data_fim_vigencia_proposta,
        case
            when trim(OBJETO_PROPOSTA) in ('', '-') then null
            else trim(OBJETO_PROPOSTA)
        end as objeto_proposta,
        {{ clean_string('ITEM_INVESTIMENTO') }} as item_investimento,
        {{ clean_string('ENVIADA_MANDATARIA') }} as enviada_mandataria,
        {{ clean_string('NOME_SUBTIPO_PROPOSTA') }} as nome_subtipo_proposta,
        {{ clean_string('DESCRICAO_SUBTIPO_PROPOSTA') }} as descricao_subtipo_proposta,
        {{ parse_decimal_br('VL_GLOBAL_PROP') }} as valor_global_proposta,
        {{ parse_decimal_br('VL_REPASSE_PROP') }} as valor_repasse_proposta,
        {{ parse_decimal_br('VL_CONTRAPARTIDA_PROP') }} as valor_contrapartida_proposta,
        case
            when trim(CD_AGENCIA) in ('', '-') then null
            else trim(CD_AGENCIA)
        end as codigo_agencia,
        {{ clean_string('CD_CONTA') }} as codigo_conta,
        __ingested_at_utc,
        __ingestion_run_id,
        __source_file,
        __source_sha256
    from source
)

select * from renamed_and_typed
