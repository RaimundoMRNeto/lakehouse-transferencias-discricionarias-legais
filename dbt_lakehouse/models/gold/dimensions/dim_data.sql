{{ config(
    materialized='table',
    file_format='delta',
    schema='gold'
) }}

WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(DATE '1990-01-01', DATE '2050-12-31', INTERVAL 1 DAY)) AS data_dia
),
normal_days AS (
    SELECT
        CAST(DATE_FORMAT(data_dia, 'yyyyMMdd') AS INT) AS data_sk,
        data_dia AS data,
        CAST(YEAR(data_dia) AS INT) AS ano,
        CAST(MONTH(data_dia) AS INT) AS mes,
        CAST(DAY(data_dia) AS INT) AS dia,
        CAST(QUARTER(data_dia) AS INT) AS trimestre,
        CAST(CASE WHEN MONTH(data_dia) <= 6 THEN 1 ELSE 2 END AS INT) AS semestre,
        CAST(DAYOFWEEK(data_dia) AS INT) AS dia_semana,
        CASE MONTH(data_dia)
            WHEN 1 THEN 'Janeiro'
            WHEN 2 THEN 'Fevereiro'
            WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril'
            WHEN 5 THEN 'Maio'
            WHEN 6 THEN 'Junho'
            WHEN 7 THEN 'Julho'
            WHEN 8 THEN 'Agosto'
            WHEN 9 THEN 'Setembro'
            WHEN 10 THEN 'Outubro'
            WHEN 11 THEN 'Novembro'
            WHEN 12 THEN 'Dezembro'
        END AS nome_mes,
        CASE WHEN DAYOFWEEK(data_dia) IN (1, 7) THEN true ELSE false END AS is_fim_de_semana
    FROM date_spine
),
sentinels AS (
    SELECT
        -1 AS data_sk,
        CAST(NULL AS DATE) AS data,
        CAST(NULL AS INT) AS ano,
        CAST(NULL AS INT) AS mes,
        CAST(NULL AS INT) AS dia,
        CAST(NULL AS INT) AS trimestre,
        CAST(NULL AS INT) AS semestre,
        CAST(NULL AS INT) AS dia_semana,
        'Não Informado' AS nome_mes,
        CAST(NULL AS BOOLEAN) AS is_fim_de_semana
    UNION ALL
    SELECT
        -2 AS data_sk,
        CAST(NULL AS DATE) AS data,
        CAST(NULL AS INT) AS ano,
        CAST(NULL AS INT) AS mes,
        CAST(NULL AS INT) AS dia,
        CAST(NULL AS INT) AS trimestre,
        CAST(NULL AS INT) AS semestre,
        CAST(NULL AS INT) AS dia_semana,
        'Fora da Janela Analítica' AS nome_mes,
        CAST(NULL AS BOOLEAN) AS is_fim_de_semana
)
SELECT * FROM normal_days
UNION ALL
SELECT * FROM sentinels
