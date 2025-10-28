{{ config(materialized='table', tags=['example:api_demo','scope:common','kind:seed-consumer']) }}
-- Simple staging table from seed
select id, email
from {{ source('crm', 'users') }};
