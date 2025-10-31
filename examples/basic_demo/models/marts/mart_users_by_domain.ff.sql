{{ config(
    materialized='table',
    tags=[
        'example:basic_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark'
    ],
) }}

with base as (
    select
        email_domain,
        signup_date
    from {{ ref('users_clean.ff') }}
)

select
    email_domain,
    count(*) as user_count,
    min(signup_date) as first_signup,
    max(signup_date) as last_signup
from base
group by email_domain
order by email_domain;
