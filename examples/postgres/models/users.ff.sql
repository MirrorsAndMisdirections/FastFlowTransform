create or replace table users as
select id, email
from {{ source('crm','users') }};