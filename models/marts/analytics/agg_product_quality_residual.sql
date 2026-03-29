WITH ioi AS (
    SELECT * FROM {{ ref('int_order_item') }}
),

ioe AS (
    SELECT
        order_id,
        delivery_delay_days,
        order_status,
        purchase_timestamp_local
    FROM {{ ref('int_order_enriched') }}
    WHERE order_status = 'delivered'
),

ir AS (
    SELECT
        order_id,
        review_score
    FROM {{ ref('int_reviews') }}
),

dd AS (
    SELECT
        date_sk,
        full_date,
        month,
        year
    FROM {{ ref('dim_date') }}
),

order_metrics AS (
    SELECT
        order_id,
        SUM(price) AS total_order_value,
        COUNT(order_item_id) AS items_per_order
    FROM ioi
    GROUP BY 1
),

base AS (
    SELECT
        ioi.order_id,
        ioi.order_item_id,
        ioi.product_id,
        dd.date_sk,
        dd.month,
        dd.year,
        SAFE_MULTIPLY(
            SAFE_DIVIDE(ioi.price, om.total_order_value),
            ir.review_score
        ) AS weighted_review_score,
        SAFE_DIVIDE(
            ioe.delivery_delay_days,
            om.items_per_order
        ) AS weighted_delivery_delay
    FROM ioi
    JOIN ioe ON ioi.order_id = ioe.order_id
    LEFT JOIN ir ON ioi.order_id = ir.order_id
    LEFT JOIN order_metrics om ON ioi.order_id = om.order_id
    LEFT JOIN dd ON CAST(ioe.purchase_timestamp_local AS DATE) = CAST(dd.full_date AS DATE)
    WHERE ir.review_score IS NOT NULL
      AND ioe.delivery_delay_days IS NOT NULL
),

pre_ols AS (
    SELECT
        *,
        AVG(weighted_review_score) OVER() AS global_mean_y,
        AVG(weighted_delivery_delay) OVER() AS global_mean_x
    FROM base
),

ols AS (
    SELECT
        ANY_VALUE(global_mean_y) AS mean_y,
        ANY_VALUE(global_mean_x) AS mean_x,
        SAFE_DIVIDE(
            SUM((weighted_delivery_delay - global_mean_x) * (weighted_review_score - global_mean_y)),
            SUM(POW(weighted_delivery_delay - global_mean_x, 2))
        ) AS slope
    FROM pre_ols
),

residuals AS (
    SELECT
        b.product_id,
        b.date_sk,
        b.month,
        b.year,
        b.weighted_review_score,
        b.weighted_delivery_delay,
        (o.mean_y - o.slope * o.mean_x) + (o.slope * b.weighted_delivery_delay) AS predicted_score,
        b.weighted_review_score - ((o.mean_y - o.slope * o.mean_x) + (o.slope * b.weighted_delivery_delay)) AS residual
    FROM base b
    CROSS JOIN ols o
)

SELECT
    dp.product_sk,
    dp.product_category_name,
    r.date_sk,
    r.year,
    r.month,
    COUNT(DISTINCT r.product_id) AS total_orders,
    ROUND(AVG(r.weighted_review_score), 3) AS avg_weighted_review,
    ROUND(AVG(r.weighted_delivery_delay), 3) AS avg_weighted_delay,
    ROUND(AVG(r.predicted_score), 3) AS avg_predicted_score,
    ROUND(AVG(r.residual), 3) AS avg_residual,
    ROUND(STDDEV(r.residual), 3) AS stddev_residual
FROM residuals r
JOIN {{ ref('dim_product') }} dp
    ON r.product_id = dp.product_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY r.year DESC, r.month DESC, avg_residual ASC