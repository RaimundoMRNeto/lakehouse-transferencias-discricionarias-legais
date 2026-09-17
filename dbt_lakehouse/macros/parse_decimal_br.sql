{% macro parse_decimal_br(col, precision=17, scale=2) -%}
    cast(replace(nullif(trim({{ col }}), ''), ',', '.') as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}
