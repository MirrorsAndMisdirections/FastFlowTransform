{{ config(
    materialized='table',
    tags=['example:materializations_demo', 'scope:mart',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark']
) }}

with orders as (
  select * from {{ ref('stg_orders.ff') }}
),
flags as (
  -- EPHEMERAL model gets inlined here
  select * from {{ ref('order_flags_ephemeral.ff') }}
),
joined as (
  select
    o.customer_id,
    count(*)                          as order_count,
    sum(o.amount)                     as total_amount,
    sum(case when f.is_big_order then 1 else 0 end) as big_order_count,
    min(o.order_ts)                   as first_order_ts,
    max(o.order_ts)                   as last_order_ts
  from orders o
  left join flags f
    on o.order_id = f.order_id
  group by o.customer_id
)
select
  j.customer_id,
  c.customer_name,
  c.customer_status,
  j.order_count,
  j.big_order_count,
  j.total_amount,
  j.first_order_ts,
  j.last_order_ts
from joined j
join {{ ref('stg_customers.ff') }} c
  on j.customer_id = c.customer_id
