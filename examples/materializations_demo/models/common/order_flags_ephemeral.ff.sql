{{ config(
    materialized='ephemeral',
    tags=['example:materializations_demo', 'scope:helpers',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark']
) }}

-- Not persisted; will be inlined as a CTE where referenced
select
  o.order_id,
  o.customer_id,
  (o.amount >= 100.0) as is_big_order
from {{ ref('stg_orders.ff') }} o
