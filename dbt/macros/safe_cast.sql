{% macro safe_cast(expr, dtype) -%}
    (NULLIF({{ expr }}::text, '')::{{ dtype }})
{%- endmacro %}
