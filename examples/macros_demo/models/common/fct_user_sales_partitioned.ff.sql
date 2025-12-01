{{ config(
    materialized = 'table',
    tags = [
        'example:macros_demo', 
        'scope:common', 
        'engine:duckdb', 
        'engine:postgres', 
        'engine:databricks_spark', 
        'engine:bigquery', 
        'engine:snowflake_snowpark'
    ]
) }}

with sales as (
  select
    u.user_id,
    u.user_segment,
    o.order_ts,
    o.amount
  from {{ ref('stg_orders.ff') }} as o
  join {{ ref('dim_users.ff') }} as u
    on u.user_id = o.user_id
  where
    -- demo: engine-aware partition predicate using stdlib
    {{ ff_partition_filter('o.order_ts', var('from_date', '2025-10-01'), var('to_date', '2025-10-31')) }}
)
select
  user_id,
  user_segment,
  count(*) as order_count,
  sum(amount) as total_amount
from sales
group by user_id, user_segment;
