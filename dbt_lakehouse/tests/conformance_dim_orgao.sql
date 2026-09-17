-- Valida conformação estrita de descrições de órgãos entre os três papéis institucionais na Silver
WITH sup_prop AS (
    SELECT DISTINCT
        codigo_orgao_superior AS codigo_orgao,
        descricao_orgao_superior AS descricao_orgao
    FROM {{ ref('siconv_proposta') }}
),
conc_prop AS (
    SELECT DISTINCT
        codigo_orgao,
        descricao_orgao
    FROM {{ ref('siconv_proposta') }}
),
sup_prog AS (
    SELECT DISTINCT
        codigo_orgao_superior_programa AS codigo_orgao,
        descricao_orgao_superior_programa AS descricao_orgao
    FROM {{ ref('siconv_programa_cadastral') }}
),
divergences AS (
    -- Par 1: superior proposta <-> concedente proposta
    SELECT
        s.codigo_orgao,
        'superior_proposta_vs_concedente_proposta' AS comparison_pair,
        s.descricao_orgao AS desc_a,
        c.descricao_orgao AS desc_b
    FROM sup_prop s
    JOIN conc_prop c ON s.codigo_orgao = c.codigo_orgao
    WHERE s.descricao_orgao != c.descricao_orgao

    UNION ALL

    -- Par 2: superior proposta <-> superior programa
    SELECT
        s.codigo_orgao,
        'superior_proposta_vs_superior_programa' AS comparison_pair,
        s.descricao_orgao AS desc_a,
        p.descricao_orgao AS desc_b
    FROM sup_prop s
    JOIN sup_prog p ON s.codigo_orgao = p.codigo_orgao
    WHERE s.descricao_orgao != p.descricao_orgao

    UNION ALL

    -- Par 3: concedente proposta <-> superior programa
    SELECT
        c.codigo_orgao,
        'concedente_proposta_vs_superior_programa' AS comparison_pair,
        c.descricao_orgao AS desc_a,
        p.descricao_orgao AS desc_b
    FROM conc_prop c
    JOIN sup_prog p ON c.codigo_orgao = p.codigo_orgao
    WHERE c.descricao_orgao != p.descricao_orgao
)
SELECT *
FROM divergences
