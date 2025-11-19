{{ config(materialized='table', tags=['example:cache_demo','engine:duckdb','engine:postgres','engine:databricks_spark','engine:bigquery','engine:snowflake_snowpark']) }}
with u as (
  select user_id, email from {{ ref('stg_users.ff') }}
),
o as (
  select user_id, amount from {{ ref('stg_orders.ff') }}
)
select u.user_id, u.email, coalesce(sum(o.amount),0) as total_amount
from u
left join o using (user_id)
group by 1,2
order by 1;
