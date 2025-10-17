create or replace table orders as
select order_id, user_id, amount
from {{ source('erp','orders') }};
