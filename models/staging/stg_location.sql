with source as(
    select *
    from {{ref('olist_geolocation_dataset')}}
),

staged as (
    select geolocation_zip_code_prefix as location_id,
    geolocation_city as city_names,
    geolocation_state as state_names,
    geolocation_lat,
    geolocation_lng
    from source
)

select *
from staged