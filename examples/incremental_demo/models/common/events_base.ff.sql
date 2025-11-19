{{ config(
    materialized='table',
    tags=[
        'example:incremental_demo',
        'scope:common',
        'kind:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

select
  event_id,
  cast(updated_at as timestamp) as updated_at,
  value
from {{ source('raw', 'events') }};
