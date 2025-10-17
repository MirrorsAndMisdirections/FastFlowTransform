{{ config(materialized='ephemeral', tags=['helper']) }}

-- Use var('day') and macro on_or_before()
-- In this demo we just return all user ids; tweak to your data shape if you add a date column later.
select id
from {{ ref('users.ff') }}
where {{ on_or_before("signup_ts", "'" ~ var('day','2000-01-01') ~ "'") }}
