{{ config(materialized='table', tags=['staging']) }}
select id, email
from {{ source('app','users') }};