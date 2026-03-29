WITH category_mapping AS (
    SELECT DISTINCT 
        product_category_name,
        MIN(product_sk) OVER(PARTITION BY product_category_name) AS category_sk
    FROM {{ ref('dim_product') }}
)

SELECT 
    CAST(dp_a.category_sk AS STRING) AS product_category_sk,
    INITCAP(REPLACE(sba.english_name, '_', ' ')) AS product_category_name,
    CAST(dp_b.category_sk AS STRING) AS next_product_category_sk,
    INITCAP(REPLACE(sba.next_category, '_', ' ')) AS next_product_category,
    sba.pair_count
FROM {{ ref('stg_product_sequential_best_action') }} sba
JOIN category_mapping dp_a 
    ON dp_a.product_category_name = INITCAP(REPLACE(sba.english_name, '_', ' '))
JOIN category_mapping dp_b 
    ON dp_b.product_category_name = INITCAP(REPLACE(sba.next_category, '_', ' '))