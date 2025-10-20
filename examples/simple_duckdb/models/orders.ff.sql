-- Independent of users.ff → eligible for parallel execution within the same level (v0.3).
create or replace table orders as
select order_id, user_id, amount
from {{ source('erp','orders') }};
