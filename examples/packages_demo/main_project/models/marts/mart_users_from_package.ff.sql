{{ config(
    materialized='table',
    tags=[
        'example:packages_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery'
    ],
) }}

{#-
  This mart lives in the main project, but depends on a staging model
  defined in the shared_package:

    ../shared_package/models/staging/users_base.ff.sql

  The relation is referenced by its model name:

    ref('users_base.ff')
-#}

with base as (
    select
        email_domain,
        signup_date
    from {{ ref('users_base.ff') }}
)

select
    email_domain,
    count(*) as user_count,
    min(signup_date) as first_signup,
    max(signup_date) as last_signup
from base
group by email_domain
order by email_domain;
