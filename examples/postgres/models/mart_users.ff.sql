create or replace table mart_users as
select id, email, is_gmail
from {{ ref('users_enriched') }};
