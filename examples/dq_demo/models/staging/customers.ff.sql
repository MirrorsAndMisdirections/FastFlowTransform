{{ config(
    materialized='table',
    tags=[
        'example:dq_demo',
        'scope:staging',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}

-- Staging table for customers
select
  customer_id,
  name,
  status,
  created_at
from {{ source('crm', 'customers') }};
