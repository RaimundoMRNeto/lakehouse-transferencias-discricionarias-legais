{% macro parse_date_br(col) -%}
    to_date(nullif(trim({{ col }}), ''), 'dd/MM/yyyy')
{%- endmacro %}
