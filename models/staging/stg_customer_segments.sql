WITH source as (
    SELECT *
    FROM {{ref('customer_segments_clv')}}
)

SELECT *
FROM source