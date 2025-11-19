{{ config(
    materialized='table',
    tags=[
        'example:api_demo',
        'scope:common',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

{# Choose the producing model by variable. Default is the pandas HTTP version. #}
{% set api_users_model = var('api_users_model', 'api_users_http') %}

{# materialize literal refs so the loader sees them #}
{% set _api_users_refs = {
    'api_users_http': ref('api_users_http'),
    'api_users_requests': ref('api_users_requests')
} %}

{% set api_users_relation = _api_users_refs.get(api_users_model, _api_users_refs['api_users_http']) %}


-- Join local seed users with API users by email (demo-only; real keys will differ)
with a as (
  select u.id as user_id, u.email
  from {{ ref('users.ff') }} u
),
b as (
  select * from {{ api_users_relation }}
)
select
  a.user_id,
  a.email,
  b.api_user_id,
  b.username,
  b.name
from a
left join b on lower(a.email) = lower(b.email);
