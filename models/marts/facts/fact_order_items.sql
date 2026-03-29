WITH ioi AS (
    SELECT * FROM {{ ref('int_order_item') }}
),

dp AS (
    SELECT * FROM {{ ref('dim_product') }}
),

ds AS (
    SELECT * FROM {{ ref('dim_seller') }}
),

dd AS (
    SELECT * FROM {{ ref('dim_date') }}
),

ioe AS (
    SELECT * FROM {{ ref('int_order_enriched') }}
),

ir AS (
    SELECT 
        order_id, 
        review_score AS order_review
    FROM {{ ref('int_reviews') }}
),

order_metrics AS (
    SELECT 
        order_id,
        SUM(price) AS total_order_value,
        COUNT(order_item_id) AS items_per_order
    FROM ioi
    GROUP BY 1
)

SELECT
    CAST({{ dbt_utils.generate_surrogate_key(['ioi.order_id', 'ioi.order_item_id']) }} AS STRING) AS order_item_sk,
    CAST(ioi.order_id AS STRING) AS order_id,
    CAST(ioi.order_item_id AS STRING) AS order_item_id,
    CAST(dp.product_sk AS STRING) AS product_sk,
    CAST(ds.seller_sk AS STRING) AS seller_sk,
    CAST(dd_ship.date_sk AS STRING) AS shipping_limit_date_sk,
    CAST(dd_purch.date_sk AS STRING) AS purchase_date_sk,
    ioi.price,
    ioi.freight_value,
    ioe.purchase_timestamp_local,
    ioe.order_status,
    SAFE_MULTIPLY(
        SAFE_DIVIDE(ioi.price, om.total_order_value), 
        ir.order_review
    ) AS weighted_review_score,
    SAFE_DIVIDE(ioe.delivery_delay_days, om.items_per_order) AS weighted_delivery_delay

FROM ioi
JOIN dp ON ioi.product_id = dp.product_id
JOIN ds ON ioi.seller_id = ds.seller_id
JOIN ioe ON ioi.order_id = ioe.order_id
LEFT JOIN ir ON ioi.order_id = ir.order_id
LEFT JOIN order_metrics om ON ioi.order_id = om.order_id
JOIN dd AS dd_purch 
    ON CAST(ioe.purchase_timestamp_local AS DATE) = CAST(dd_purch.full_date AS DATE)
LEFT JOIN dd AS dd_ship 
    ON CAST(ioi.shipping_limit_date_local AS DATE) = CAST(dd_ship.full_date AS DATE)