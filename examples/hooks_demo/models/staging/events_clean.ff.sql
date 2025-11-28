{{ config(
    materialized='table',
    tags=[
        'example:hooks_demo',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

{# engine-aware string type #}
{% set string_type =
    "varchar"
    if engine() in ["duckdb", "postgres", "postgresql"]
    else "string"
%}

with src as (
    select
        cast(id as integer)            as event_id,
        cast(user_id as integer)       as user_id,
        cast(event_ts as timestamp)    as event_ts,
        cast(event_type as {{ string_type }})    as event_type
    from {{ source('raw', 'events') }}
)

select
    event_id,
    user_id,
    event_ts,
    event_type,
    cast(event_ts as date) as event_date
from src;
