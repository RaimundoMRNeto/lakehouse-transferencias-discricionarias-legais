{% macro parse_boolean_sim_nao(col) -%}
    case
        when upper(trim({{ col }})) = 'SIM' then true
        when upper(trim({{ col }})) = 'NÃO' then false
        else null
    end
{%- endmacro %}
