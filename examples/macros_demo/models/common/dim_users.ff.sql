{{ config(
    materialized='table',
    tags=['example:macros_demo', 'scope:common', 'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark']
) }}

with u as (
  select * from {{ ref('stg_users.ff') }}
),
labels as (
  -- Tiny lookup generated at render time (engine-aware)
  {% set labels = [
        {"domain":"example.com", "label":"internal"},
        {"domain":"gmail.com",   "label":"consumer"},
     ] %}
  {%- if engine('duckdb') == 'bigquery' -%}
    select * from unnest([
      {%- for row in labels -%}
        struct('{{ row['domain'] }}' as domain, '{{ row['label'] }}' as label){% if not loop.last %},{% endif %}
      {%- endfor -%}
    ])
  {%- else -%}
    select * from (values
      {%- for row in labels -%}
        ('{{ row['domain'] }}', '{{ row['label'] }}'){% if not loop.last %},{% endif %}
      {%- endfor -%}
    ) as t(domain, label)
  {%- endif -%}
)
select
  u.user_id,
  u.email,
  u.email_domain,
  u.country,
  l.label as user_segment
from u
left join labels l on l.domain = u.email_domain;
