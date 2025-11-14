{{ config(materialized='table', tags=['example:cache_demo','engine:duckdb']) }}
select cast(id as int) as user_id, lower(email) as email
from {{ source('crm', 'users') }};
