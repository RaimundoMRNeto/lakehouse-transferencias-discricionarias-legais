{% macro date_to_gold_sk(col) -%}
    CASE
        WHEN {{ col }} IS NULL THEN -1
        WHEN {{ col }} < DATE '1990-01-01' OR {{ col }} > DATE '2050-12-31' THEN -2
        ELSE CAST(DATE_FORMAT({{ col }}, 'yyyyMMdd') AS INT)
    END
{%- endmacro %}
