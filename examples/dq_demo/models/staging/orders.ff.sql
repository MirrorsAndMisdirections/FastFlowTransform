{{ config(
    materialized='table',
    tags=[
        'example:dq_demo',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark'
    ],
) }}

-- Staging table for orders with proper types for DQ checks
select
  cast(order_id    as int)        as order_id,
  cast(customer_id as int)        as customer_id,
  cast(amount      as numeric)     as amount,
  cast(order_ts    as timestamp)  as order_ts
from {{ source('crm', 'orders') }};
