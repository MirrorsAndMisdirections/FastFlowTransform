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

-- Simple fact table built from base_events
select
  event_type,
  count(*) as event_count,
  min(event_ts) as first_event_ts,
  max(event_ts) as last_event_ts
from {{ ref('base_events.ff') }}
group by event_type
