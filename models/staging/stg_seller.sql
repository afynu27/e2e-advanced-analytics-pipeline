with source as (
    select *
    from {{ref('olist_sellers_dataset')}}
),

staged as (
    select seller_id,
    seller_zip_code_prefix as location_id
    from source
)

select *
from staged