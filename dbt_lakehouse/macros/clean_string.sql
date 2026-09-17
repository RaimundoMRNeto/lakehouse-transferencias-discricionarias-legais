{% macro clean_string(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}
