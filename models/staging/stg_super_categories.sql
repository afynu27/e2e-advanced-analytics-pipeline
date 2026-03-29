WITH source AS (
    SELECT *
    FROM {{ref('super_categories')}}
)
SELECT * 
FROM source 