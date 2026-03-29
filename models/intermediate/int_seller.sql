with seller as (
    select *
    from {{ref('stg_seller')}}
)
select *
from seller