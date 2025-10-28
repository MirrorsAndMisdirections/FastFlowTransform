{{ config(
  materialized='incremental',
  unique_key=['event_id'],
  on_schema_change='append_new_columns',
  tags=['fact','incremental']
) }}
with base as (
  select *
  from {{ source('app','events') }}
  {% if is_incremental() %}
    where cast(ingested_at as timestamp)
         > coalesce(
             (select max(cast(ingested_at as timestamp)) from {{ this }}),
             timestamp '1970-01-01'
           )
  {% endif %}
)
select
  event_id,
  user_id,
  event_type,
  ingested_at,
  meta_json
from base;