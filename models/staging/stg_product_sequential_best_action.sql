WITH source AS (
    SELECT *
    FROM {{ref('product_sequential_best_action')}}
)
SELECT *
FROM source