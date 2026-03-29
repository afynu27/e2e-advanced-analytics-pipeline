with source as (
    select *
    from {{ref('olist_orders_dataset')}} 
),

staged as (
    select *
    from source
)

select *
from staged