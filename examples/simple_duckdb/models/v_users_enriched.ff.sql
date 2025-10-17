{{ config(materialized='view', tags=['mart']) }}
select id, email, is_gmail
from {{ ref('users_enriched') }}
