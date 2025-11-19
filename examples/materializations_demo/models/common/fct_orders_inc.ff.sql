{{ config(
    materialized='incremental',
    tags=['example:materializations_demo', 'scope:fct',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark'],
    incremental={
        'updated_at_column': 'order_ts'
    },
    unique_key=['order_id']
) }}

-- Full refresh (first run) or delta (later runs).
-- Use a portable TIMESTAMP cast for the lower bound.
{% set lb = "coalesce((select max(order_ts) from " ~ this ~ "), cast('1970-01-01 00:00:00' as timestamp))" %}

select
  o.order_id,
  o.customer_id,
  o.amount,
  o.order_ts
from {{ ref('stg_orders.ff') }} o
{% if is_incremental() %}
where o.order_ts > {{ lb }}
{% endif %}
