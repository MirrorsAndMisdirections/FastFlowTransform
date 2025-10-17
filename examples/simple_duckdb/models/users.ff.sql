-- materialized table
create or replace table users as
select
  id,
  email,
  cast(signup_ts as date) as signup_ts
from {{ source('crm','users') }};
