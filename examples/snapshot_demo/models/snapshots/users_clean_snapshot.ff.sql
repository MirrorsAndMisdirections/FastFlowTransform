{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'timestamp',
        'updated_at': 'signup_date',
    },
    unique_key='user_id',
    tags=[
        'example:snapshot_demo',
        'scope:snapshot',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark',
    ],
) }}

select
    user_id,
    email,
    email_domain,
    signup_date
from {{ ref('users_clean.ff') }};
