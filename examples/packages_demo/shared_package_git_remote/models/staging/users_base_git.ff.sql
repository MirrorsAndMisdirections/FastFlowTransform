{{ config(
    materialized='view',
    tags=[
        'example:packages_demo',
        'scope:staging',
        'origin:git_package',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery'
    ],
) }}

{#-
  users_base_git.ff is the git-backed variant of users_base.ff

  It:
  - casts id → user_id
  - normalizes email
  - derives email_domain using the *git* macro git_email_domain()
  - adds a literal column "source" so it’s obvious which package it came from
-#}

with raw_users as (
    select
        cast(id as integer)         as user_id,
        lower(email)                as email,
        cast(signup_date as date)   as signup_date
    from {{ source('crm', 'users') }}
)

select
    user_id,
    email,
    {{ git_email_domain("email") }} as email_domain,
    signup_date,
    'git_package'                   as source
from raw_users;
