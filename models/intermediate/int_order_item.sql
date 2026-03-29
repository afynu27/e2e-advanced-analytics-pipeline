WITH order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        seller_id,
        DATE(CAST(shipping_limit_date AS TIMESTAMP), "Asia/Jakarta") AS shipping_limit_date_local,
        price,
        freight_value
    FROM {{ ref('stg_order_items') }}
)
SELECT *
FROM order_items
