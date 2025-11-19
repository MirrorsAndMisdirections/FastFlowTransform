{{ config(
    materialized='view',
    tags=['example:macros_demo', 'scope:common', 'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark']
) }}

select
  cast(order_id as int)     as order_id,
  cast(customer_id as int)  as user_id,
  {{ safe_cast_amount("amount") }} as amount,
  cast(order_ts as timestamp) as order_ts
from {{ source('sales', 'orders') }};
