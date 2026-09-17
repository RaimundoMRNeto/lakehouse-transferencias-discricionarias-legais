-- Valida reconciliação dinâmica de contagem de linhas entre a visão canônica da Silver e gold.fct_convenio
WITH canonical AS (
    SELECT DISTINCT
        numero_convenio,
        id_proposta,
        data_assinatura_convenio,
        data_publicacao_convenio,
        data_inicio_vigencia_convenio,
        data_fim_vigencia_convenio,
        data_limite_prestacao_contas,
        situacao_convenio,
        subsituacao_convenio,
        situacao_publicacao,
        situacao_contratacao,
        is_instrumento_ativo,
        is_opera_obtv,
        is_assinado,
        numero_processo,
        unidade_gestora_emitente,
        quantidade_termos_aditivos,
        quantidade_prorrogacoes,
        valor_global_convenio,
        valor_repasse_convenio,
        valor_contrapartida_convenio,
        valor_empenhado_convenio,
        valor_desembolsado_convenio,
        valor_saldo_remanescente_tesouro,
        valor_saldo_remanescente_convenente,
        valor_rendimento_aplicacao,
        valor_ingresso_contrapartida,
        valor_global_original_convenio
    FROM {{ ref('siconv_convenio') }}
),
silver_canonical_cnt AS (
    SELECT COUNT(*) AS cnt_silver FROM canonical
),
gold_cnt AS (
    SELECT COUNT(*) AS cnt_gold FROM {{ ref('fct_convenio') }}
)
SELECT *
FROM silver_canonical_cnt s
JOIN gold_cnt g ON 1=1
WHERE s.cnt_silver != g.cnt_gold
