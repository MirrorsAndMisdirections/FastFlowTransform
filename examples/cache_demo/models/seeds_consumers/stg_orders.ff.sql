{{ config(materialized='view', tags=['example:cache_demo','engine:duckdb']) }}
select
  cast(order_id as int) as order_id,
  cast(customer_id as int) as user_id,
  cast(amount as double) as amount
from {{ source('crm', 'orders') }};
