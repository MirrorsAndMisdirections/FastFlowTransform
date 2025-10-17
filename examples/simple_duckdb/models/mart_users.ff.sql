-- materialized table from a view (keeps example simple)
create or replace table mart_users as
select
  id,
  email,
  is_gmail,
  {{ sql_email_domain("email") }} as email_domain
from {{ ref('users_enriched') }};
