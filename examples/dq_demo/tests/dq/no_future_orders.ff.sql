{{ config(
    type="no_future_orders",
    params=["where"]
) }}

-- Custom DQ test: fail if any row has a timestamp in the future.
--
-- Context variables injected by the runner:
--   {{ table }}   : table name (e.g. "orders")
--   {{ column }}  : timestamp column (e.g. "order_ts")
--   {{ where }}   : optional filter (string), from params["where"]
--   {{ params }}  : full params dict (validated), if you ever need it

select count(*) as failures
from {{ table }}
where {{ column }} > current_timestamp
  {%- if where %} and ({{ where }}){%- endif %}
