with source as (
    select *
    from {{ref('olist_order_reviews_dataset')}}
    
),

staged as (
    select *
    from source
)

select *
from staged