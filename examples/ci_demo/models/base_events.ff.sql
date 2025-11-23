{{ config(
    materialized='table',
    tags=[
        'example:ci_demo',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

-- Base model: a tiny synthetic events table
select
  1 as event_id,
  'page_view' as event_type,
  current_timestamp as event_ts
