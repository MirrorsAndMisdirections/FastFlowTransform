{{ config(
    materialized='table',
    tags=['example:materializations_demo', 'scope:dim',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark']
) }}

-- Dimension table; stable per customer
select
  c.customer_id,
  c.customer_name,
  c.customer_status,
  current_timestamp as loaded_at
from {{ ref('stg_customers.ff') }} c
