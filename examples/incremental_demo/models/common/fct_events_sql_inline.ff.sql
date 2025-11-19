{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental={
        'updated_at_column': 'updated_at'
    },
    tags=[
        'example:incremental_demo',
        'scope:common',
        'kind:incremental',
        'inc:type:inline-sql',
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
from base
{% if is_incremental() %}
where updated_at > (
  select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
  from {{ this }}
)
{% endif %};
