{{ config(
    materialized='view',
    tags=['example:materializations_demo', 'scope:staging',
          'engine:duckdb', 'engine:postgres', 'engine:databricks_spark']
) }}

-- Normalize order columns and force types portable across engines
select
  cast(order_id    as int)                        as order_id,
  cast(customer_id as int)                        as customer_id,
  /* Engine-portable DOUBLE via macro (Postgres → double precision, Spark/DuckDB → double) */
  cast(amount      as {{ dtype_double() }})       as amount,
  cast(order_ts    as timestamp)                  as order_ts
from {{ source('demo', 'orders') }};
