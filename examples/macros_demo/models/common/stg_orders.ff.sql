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

select
  cast(order_id as int)     as order_id,
  cast(customer_id as int)  as user_id,
  {{ safe_cast_amount("amount") }} as amount,
  {{ ff_safe_cast("amount", "numeric", default = "0") }} as amount_safe,
  {{ ff_date_trunc("order_ts", "day") }} as order_day,
  {{ ff_date_add("order_ts", "day", 1) }} as order_ts_plus_1d,
  cast(order_ts as timestamp) as order_ts
from {{ source('sales', 'orders') }};
