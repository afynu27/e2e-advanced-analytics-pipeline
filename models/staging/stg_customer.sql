with source as (

    select *
    from {{ ref('olist_customers_dataset') }}

),

staged as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as location_id
    from source

)

select *
from staged
