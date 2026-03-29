with source as (
    select *
    from {{ref('olist_order_payments_dataset')}} 

),

staged as (
    select *
    from source
)

select *
from staged