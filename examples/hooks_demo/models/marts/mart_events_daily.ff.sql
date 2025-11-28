{{ config(
    materialized='table',
    tags=[
        'example:hooks_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

with base as (
    select
        event_date,
        event_type
    from {{ ref('events_clean.ff') }}
)

select
    event_date,
    event_type,
    count(*) as event_count
from base
group by event_date, event_type
order by event_date, event_type;
