{{ config(
    materialized='view',
    tags=[
        'example:packages_demo',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery'
    ],
) }}

{#-
  users_base.ff is a reusable staging model that expects a source:
    sources.yml → crm.users (identifier: seed_users)

  It:
  - casts id → user_id
  - normalizes email
  - derives email_domain using a shared macro
-#}

with raw_users as (
    select
        cast(id as integer)  as user_id,
        lower(email)         as email,
        cast(signup_date as date) as signup_date
    from {{ source('crm', 'users') }}
)

select
    user_id,
    email,
    {{ email_domain("email") }} as email_domain,
    signup_date
from raw_users;
