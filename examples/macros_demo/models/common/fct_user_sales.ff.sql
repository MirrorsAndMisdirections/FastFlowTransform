{{ config(
    materialized='table',
    tags=['example:macros_demo', 'scope:common', 'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark']
) }}

with o as (
  select * from {{ ref('stg_orders.ff') }}
),
u as (
  select * from {{ ref('dim_users.ff') }}
)
select
  u.user_id,
  u.user_segment,
  count(*) as order_count,
  sum(o.amount) as total_amount,
  min(o.order_ts) as first_order_ts,
  max(o.order_ts) as last_order_ts
from o
join u on u.user_id = o.user_id
group by u.user_id, u.user_segment;
