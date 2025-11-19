{{ config(
    materialized='incremental',
    tags=[
        'example:incremental_demo',
        'scope:common',
        'kind:incremental',
        'inc:type:yaml-config',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

with base as (
  select *
  from {{ ref('events_base.ff') }}
)
select
  event_id,
  updated_at,
  value
from base;
