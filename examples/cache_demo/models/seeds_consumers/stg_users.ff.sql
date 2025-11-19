{{ config(materialized='table', tags=['example:cache_demo','engine:duckdb','engine:postgres','engine:databricks_spark','engine:bigquery','engine:snowflake_snowpark']) }}
select cast(id as int) as user_id, lower(email) as email
from {{ source('crm', 'users') }};
