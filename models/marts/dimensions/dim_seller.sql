with source as (
    select *
    from {{ ref('int_seller') }}
),

location as (
    select 
        location_id,
        city_names, 
        state_names,
        geolocation_lat,
        geolocation_lng
    from {{ ref('int_location') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }} as seller_sk,
    s.seller_id,
    s.location_id,
    INITCAP(l.city_names) AS city_names, 
    l.state_names,
    l.geolocation_lat,
    l.geolocation_lng
from source s
left join location l 
    on s.location_id = l.location_id