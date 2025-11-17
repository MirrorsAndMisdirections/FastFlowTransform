{# Reusable SQL helpers #}

{%- macro email_domain(expr) -%}
{%- set e = engine('duckdb') -%}
{%- if e == 'bigquery' -%}
    lower(split({{ expr }}, '@')[SAFE_OFFSET(1)])
{%- else -%}
    lower(split_part({{ expr }}, '@', 2))
{%- endif -%}
{%- endmacro -%}

{%- macro safe_cast_amount(expr) -%}
{# engine-aware numeric type #}
{%- set e = engine('duckdb') -%}
{%- if e == 'duckdb' -%}
    cast({{ expr }} as double)
{%- elif e == 'postgres' -%}
    cast({{ expr }} as double precision)
{%- elif e == 'databricks_spark' -%}
    cast({{ expr }} as double)
{%- elif e == 'bigquery' -%}
    cast({{ expr }} as float64)
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
