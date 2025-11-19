{{ config(
    materialized='view',
    tags=['example:materializations_demo', 'scope:staging',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark', 'engine:bigquery', 'engine:snowflake_snowpark']
) }}

-- Lightweight projection and type normalization from the seed
select
  cast(customer_id as int)        as customer_id,
  name                            as customer_name,
  status                          as customer_status
from {{ source('demo', 'customers') }};
