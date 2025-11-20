{{ config(
    materialized='snapshot',
    snapshot={
        'strategy': 'check',
        'check_cols': ['user_count', 'first_signup', 'last_signup'],
    },
    unique_key='email_domain',
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
    email_domain,
    user_count,
    first_signup,
    last_signup
from {{ ref('mart_users_by_domain.ff') }};
