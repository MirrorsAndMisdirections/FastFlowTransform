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
    u.country,
    o.amount,
    o.order_ts
  from {{ ref('stg_orders.ff') }} as o
  join {{ ref('dim_users.ff') }} as u
    on u.user_id = o.user_id
  where
    -- demo: stdlib partition IN helper on a dimension
    {{ ff_partition_in('u.country', var('partition_countries', ['DE', 'AT'])) }}
)
select
  user_id,
  user_segment,
  country,
  count(*) as order_count,
  sum(amount) as total_amount
from sales
group by user_id, user_segment, country;
