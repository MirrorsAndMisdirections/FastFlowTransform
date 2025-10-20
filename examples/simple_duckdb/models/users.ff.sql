-- This staging node has no upstream model dependencies and can run in parallel
-- with other independent nodes (v0.3 parallel scheduler).
create or replace table users as
select
  id,
  email,
  cast(signup_ts as date) as signup_ts
from {{ source('crm','users') }};
