{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

SELECT
    id_proposta,
    identificacao_proponente,
    codigo_municipio_ibge,
    codigo_orgao_superior,
    codigo_orgao,
    {{ date_to_gold_sk('data_proposta') }} AS data_proposta_sk,
    {{ date_to_gold_sk('data_inicio_vigencia_proposta') }} AS data_inicio_vigencia_proposta_sk,
    {{ date_to_gold_sk('data_fim_vigencia_proposta') }} AS data_fim_vigencia_proposta_sk,
    numero_proposta,
    modalidade,
    situacao_proposta,
    objeto_proposta,
    nome_subtipo_proposta,
    descricao_subtipo_proposta,
    CAST(1 AS INT) AS quantidade_propostas,
    valor_global_proposta,
    valor_repasse_proposta,
    valor_contrapartida_proposta
FROM {{ ref('siconv_proposta') }}
