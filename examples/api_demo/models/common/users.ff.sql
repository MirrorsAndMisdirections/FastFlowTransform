{{ config(
    materialized='table',
    tags=[
        'example:api_demo',
        'scope:common',
        'kind:seed-consumer',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark'
    ],
) }}
-- Simple staging table from seed
select id, email
from {{ source('crm', 'users') }};
