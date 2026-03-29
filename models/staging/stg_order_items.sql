with source as (
    select *
    from {{ref('olist_order_items_dataset')}}
),

staged as (
    select *
    from source
)

select *
from staged