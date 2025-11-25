{{ config(materialized='table') }}

select 
    id, 
    -- Standardizing email casing
    lower(email) as email, 
    -- Casting types explicitly
    cast(signup_date as date) as signup_date
from {{ source('raw', 'users') }}