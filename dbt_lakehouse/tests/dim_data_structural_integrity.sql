-- Valida integridade estrutural e cobertura contínua de gold.dim_data
WITH stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN data_sk = -1 THEN 1 ELSE 0 END) AS cnt_minus_one,
        SUM(CASE WHEN data_sk = -2 THEN 1 ELSE 0 END) AS cnt_minus_two,
        SUM(CASE WHEN data_sk > 0 THEN 1 ELSE 0 END) AS cnt_normal,
        MIN(data) AS min_date,
        MAX(data) AS max_date,
        COUNT(DISTINCT data) AS distinct_dates
    FROM {{ ref('dim_data') }}
)
SELECT *
FROM stats
WHERE total_rows != 22282
   OR cnt_minus_one != 1
   OR cnt_minus_two != 1
   OR cnt_normal != 22280
   OR min_date != DATE '1990-01-01'
   OR max_date != DATE '2050-12-31'
   OR distinct_dates != 22280
