{# String/date helpers #}
{% macro upper_col(c) -%}
upper({{ c }})
{%- endmacro %}

{% macro on_or_before(ts_col, day_var) -%}
-- day_var is a string like 'YYYY-MM-DD'
{{ ts_col }} <= cast({{ day_var }} as date)
{%- endmacro %}

{# Tiny quality helper: nullable->coalesce #}
{% macro nz(expr, fallback) -%}
coalesce({{ expr }}, {{ fallback }})
{%- endmacro %}
