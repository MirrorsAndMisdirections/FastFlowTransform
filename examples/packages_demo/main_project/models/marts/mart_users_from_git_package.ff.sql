-- examples/packages_demo/main_project/models/marts/mart_users_from_git_package.ff.sql

{{ config(
    materialized='table',
    tags=[
        'example:packages_demo',
        'scope:mart',
        'origin:git_package',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery'
    ],
) }}

{#-
  This mart depends on the staging model defined in the *git* package:

    examples/packages_demo/shared_package_git_remote/models/staging/users_base_git.ff.sql

  It is referenced by its model name:

    ref('users_base_git.ff')
-#}

with base as (
    select
        email_domain,
        signup_date,
        source
    from {{ ref('users_base_git.ff') }}
)

select
    email_domain,
    count(*)               as user_count,
    min(signup_date)       as first_signup,
    max(signup_date)       as last_signup,
    max(source)            as last_source_flag
from base
group by email_domain
order by email_domain;
