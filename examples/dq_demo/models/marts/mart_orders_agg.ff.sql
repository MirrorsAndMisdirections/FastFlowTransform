{{ config(
    materialized='table',
    tags=[
        'example:dq_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark'
    ],
) }}

-- Aggregate orders per customer for reconciliation & freshness tests
with base as (
  select
    o.order_id,
    o.customer_id,
    -- Ensure numeric and timestamp types for downstream DQ checks
    cast(o.amount   as numeric)    as amount,
    cast(o.order_ts as timestamp) as order_ts,
    c.name   as customer_name,
    c.status as customer_status
  from {{ ref('orders.ff') }} o
  join {{ ref('customers.ff') }} c
    on o.customer_id = c.customer_id
)
select
  customer_id,
  customer_name,
  customer_status as status,
  count(*)        as order_count,
  sum(amount)     as total_amount,
  min(order_ts)   as first_order_ts,
  max(order_ts)   as last_order_ts
from base
group by customer_id, customer_name, customer_status;
