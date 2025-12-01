{{ config(
    materialized='view',
    tags=[
      'example:macros_demo', 
      'scope:common', 
      'engine:duckdb', 
      'engine:postgres', 
      'engine:databricks_spark', 
      'engine:bigquery', 
      'engine:snowflake_snowpark'
    ]
) }}

with src as (
  select
    cast(id as int) as user_id,
    lower(email)     as email,
    {{ coalesce_any("country", default_country()) }} as country
  from {{ source('crm', 'users') }}
)
select
  user_id,
  email,
  {{ email_domain("email") }} as email_domain,
  country,
  -- Render-time Python macro usage (literal in SQL)
  '{{ slugify(var("site_name", "My Site")) }}' as site_slug
from src;
