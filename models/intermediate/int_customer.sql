with source as (
    select * from {{ ref('stg_customer') }}
),

final as (
    select
        customer_unique_id,
        max(location_id) as location_id
    from source
    group by 1
)

select 
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_sk,
    *
from final