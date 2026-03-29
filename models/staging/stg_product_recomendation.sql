WITH source as (
    SELECT *
    FROM {{ref('product_recomendation')}}
)
SELECT *
FROM source 