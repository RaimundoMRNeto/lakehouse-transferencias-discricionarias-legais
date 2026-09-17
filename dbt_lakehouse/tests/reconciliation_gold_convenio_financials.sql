-- Reconciliação financeira dinâmica entre canonical silver.siconv_convenio e gold.fct_convenio
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
canonical_agg AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado,
        CAST(COALESCE(SUM(CAST(valor_saldo_remanescente_tesouro AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_tesouro,
        CAST(COALESCE(SUM(CAST(valor_saldo_remanescente_convenente AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_convenente,
        CAST(COALESCE(SUM(CAST(valor_rendimento_aplicacao AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_rendimento,
        CAST(COALESCE(SUM(CAST(valor_ingresso_contrapartida AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_ingresso,
        CAST(COALESCE(SUM(CAST(valor_global_original_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global_original
    FROM canonical
),
gold_agg AS (
    SELECT
        CAST(COALESCE(SUM(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global,
        CAST(COALESCE(SUM(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_repasse,
        CAST(COALESCE(SUM(CAST(valor_contrapartida_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_contrapartida,
        CAST(COALESCE(SUM(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_empenhado,
        CAST(COALESCE(SUM(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_desembolsado,
        CAST(COALESCE(SUM(CAST(valor_saldo_remanescente_tesouro AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_tesouro,
        CAST(COALESCE(SUM(CAST(valor_saldo_remanescente_convenente AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_convenente,
        CAST(COALESCE(SUM(CAST(valor_rendimento_aplicacao AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_rendimento,
        CAST(COALESCE(SUM(CAST(valor_ingresso_contrapartida AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_ingresso,
        CAST(COALESCE(SUM(CAST(valor_global_original_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)) AS sum_global_original
    FROM {{ ref('fct_convenio') }}
)
SELECT *
FROM canonical_agg c
JOIN gold_agg g ON 1=1
WHERE c.sum_global != g.sum_global
   OR c.sum_repasse != g.sum_repasse
   OR c.sum_contrapartida != g.sum_contrapartida
   OR c.sum_empenhado != g.sum_empenhado
   OR c.sum_desembolsado != g.sum_desembolsado
   OR c.sum_tesouro != g.sum_tesouro
   OR c.sum_convenente != g.sum_convenente
   OR c.sum_rendimento != g.sum_rendimento
   OR c.sum_ingresso != g.sum_ingresso
   OR c.sum_global_original != g.sum_global_original
