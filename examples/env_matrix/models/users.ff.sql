{{ config(materialized='table', tags=['demo', 'env', 'seed']) }}
select
  id,
  email
from {{ source('raw', 'seed_users') }}
