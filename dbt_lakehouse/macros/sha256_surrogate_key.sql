{% macro sha256_surrogate_key(fields) -%}
    sha2(
        concat(
            {%- for field in fields %}
            case
                when {{ field }} is null then '-1:'
                else concat(cast(length(cast({{ field }} as string)) as string), ':', cast({{ field }} as string))
            end
            {%- if not loop.last %}, {% endif %}
            {%- endfor %}
        ),
        256
    )
{%- endmacro %}
