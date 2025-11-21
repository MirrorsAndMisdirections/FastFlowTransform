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
  cast(customer_id as int)        as customer_id,
  name,
  status,
  cast(created_at as timestamp)   as created_at
from {{ source('crm', 'customers') }};
