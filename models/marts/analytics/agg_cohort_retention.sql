
WITH orders AS (
    SELECT
        customer_sk,
        customer_unique_id,
        order_status,
        purchase_timestamp_local,
        order_revenue,
        EXTRACT(YEAR  FROM purchase_timestamp_local) AS order_year,
        EXTRACT(MONTH FROM purchase_timestamp_local) AS order_month
    FROM {{ ref('int_order_enriched') }}
    WHERE order_status = 'delivered'
),

first_purchase AS (
    SELECT
        customer_sk,
        MIN(purchase_timestamp_local)                             AS first_purchase_date,
        EXTRACT(YEAR  FROM MIN(purchase_timestamp_local))         AS cohort_year,
        EXTRACT(MONTH FROM MIN(purchase_timestamp_local))         AS cohort_month
    FROM orders
    GROUP BY customer_sk
),

cohort_orders AS (
    SELECT
        o.customer_sk,
        o.order_year,
        o.order_month,
        o.order_revenue,
        fp.cohort_year,
        fp.cohort_month,
        (o.order_year  - fp.cohort_year)  * 12
        + (o.order_month - fp.cohort_month) AS period_number
    FROM orders o
    INNER JOIN first_purchase fp
        ON o.customer_sk = fp.customer_sk
),

cohort_base AS (
    SELECT
        fp.cohort_year,
        fp.cohort_month,
        dc.rfm_segment,
        dc.clv_badge,
        COUNT(DISTINCT fp.customer_sk) AS cohort_size,
        AVG(dc.predicted_clv)          AS avg_clv
    FROM first_purchase fp
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON fp.customer_sk = dc.customer_sk
    GROUP BY 1, 2, 3, 4
),

cohort_activity AS (
    SELECT
        co.cohort_year,
        co.cohort_month,
        co.period_number,
        dc.rfm_segment,
        dc.clv_badge,
        COUNT(DISTINCT co.customer_sk) AS active_customers,
        SUM(co.order_revenue)          AS total_revenue,
        AVG(co.order_revenue)          AS avg_order_revenue
    FROM cohort_orders co
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON co.customer_sk = dc.customer_sk
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    ca.cohort_year,
    ca.cohort_month,
    CAST(ca.cohort_year AS STRING)
        || '-'
        || LPAD(CAST(ca.cohort_month AS STRING), 2, '0') AS cohort_label,
    CAST(dd.date_sk AS STRING) AS date_sk,
    ca.period_number,
    ca.rfm_segment,
    ca.clv_badge,
    cb.cohort_size,
    ca.active_customers,
    SAFE_DIVIDE(ca.active_customers, cb.cohort_size) AS retention_rate,
    ROUND(ca.avg_order_revenue, 2) AS avg_order_revenue,
    ROUND(cb.avg_clv, 2)           AS avg_clv,
    ROUND(ca.total_revenue, 2)     AS total_revenue
FROM cohort_activity ca
LEFT JOIN cohort_base cb
    ON  ca.cohort_year  = cb.cohort_year
    AND ca.cohort_month = cb.cohort_month
    AND ca.rfm_segment  = cb.rfm_segment
    AND ca.clv_badge    = cb.clv_badge
LEFT JOIN (
    SELECT
        year,
        month,
        MIN(date_sk) AS date_sk
    FROM {{ ref('dim_date') }}
    GROUP BY year, month
) dd
    ON  dd.year  = ca.cohort_year
    AND dd.month = ca.cohort_month
ORDER BY 1, 2, 4, 5