{{ config(
  materialized='view', 
  tags=[
    'example:cache_demo',
    'engine:duckdb',
    'engine:postgres',
    'engine:databricks_spark',
    'engine:bigquery',
    'engine:snowflake_snowpark'
  ]) }}
select
  cast(order_id as int) as order_id,
  cast(customer_id as int) as user_id,
  cast(amount as decimal) as amount
from {{ source('crm', 'orders') }};
