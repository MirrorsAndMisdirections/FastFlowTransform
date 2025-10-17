{{ config(materialized='view', tags=['mart']) }}
select
    u.id,
    u.email,
    {{ upper_col("u.email") }} as email_upper
from {{ ref('users.ff') }} u
join {{ ref('ephemeral_ids.ff') }} e using(id)
