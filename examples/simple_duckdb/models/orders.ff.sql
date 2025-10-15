-- materialises a table "orders"
create or replace table orders as
select *
from {{ source('crm', 'orders') }};
