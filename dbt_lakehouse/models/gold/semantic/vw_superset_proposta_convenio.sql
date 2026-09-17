{{ config(
    materialized='view',
    schema='gold'
) }}

SELECT
    p.id_proposta,
    p.numero_proposta,
    dt_prop.data AS data_proposta,
    dt_prop.ano AS ano_proposta,
    dt_prop.mes AS mes_proposta,
    p.identificacao_proponente,
    prop.nome_proponente,
    p.codigo_municipio_ibge,
    mun.nome_municipio AS municipio,
    mun.sigla_uf AS uf,
    p.codigo_orgao_superior,
    org_sup.descricao_orgao AS orgao_superior,
    p.codigo_orgao AS codigo_orgao_concedente,
    org_conc.descricao_orgao AS orgao_concedente,
    p.modalidade,
    p.situacao_proposta,
    p.valor_global_proposta,
    p.valor_repasse_proposta,
    p.valor_contrapartida_proposta,
    CASE
        WHEN c.numero_convenio IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS tem_convenio,
    c.numero_convenio,
    dt_ass.data AS data_assinatura,
    dt_ass.ano AS ano_assinatura,
    c.situacao_convenio,
    c.is_instrumento_ativo,
    c.valor_global_convenio,
    c.valor_repasse_convenio,
    c.valor_empenhado_convenio,
    c.valor_desembolsado_convenio
FROM {{ ref('fct_proposta') }} p
LEFT JOIN {{ ref('dim_proponente') }} prop
    ON p.identificacao_proponente = prop.identificacao_proponente
LEFT JOIN {{ ref('dim_municipio') }} mun
    ON p.codigo_municipio_ibge = mun.codigo_municipio_ibge
LEFT JOIN {{ ref('dim_orgao') }} org_sup
    ON p.codigo_orgao_superior = org_sup.codigo_orgao
LEFT JOIN {{ ref('dim_orgao') }} org_conc
    ON p.codigo_orgao = org_conc.codigo_orgao
LEFT JOIN {{ ref('dim_data') }} dt_prop
    ON p.data_proposta_sk = dt_prop.data_sk
LEFT JOIN {{ ref('fct_convenio') }} c
    ON p.id_proposta = c.id_proposta
LEFT JOIN {{ ref('dim_data') }} dt_ass
    ON c.data_assinatura_sk = dt_ass.data_sk
