{# Reusable SQL helpers #}

{%- macro email_domain(expr) -%}
    lower(split_part({{ expr }}, '@', 2))
{%- endmacro -%}

{%- macro safe_cast_amount(expr) -%}
{# engine-aware numeric type #}
{%- set e = engine('duckdb') -%}
{%- if e in ['duckdb', 'postgres'] -%}
    cast({{ expr }} as double)
{%- elif e == 'databricks_spark' -%}
    cast({{ expr }} as double)
{%- else -%}
    cast({{ expr }} as double)
{%- endif -%}
{%- endmacro -%}

{%- macro coalesce_any(expr, default) -%}
    coalesce({{ expr }}, {{ default }})
{%- endmacro -%}

{%- macro default_country() -%}
    '{{ var("default_country", "DE") }}'
{%- endmacro -%}
