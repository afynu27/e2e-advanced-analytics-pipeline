WITH daily_revenue_delivered AS (
    SELECT
        dd.date_sk, 
        CAST(dd.full_date AS DATE) AS sales_date,
        SUM(ioe.order_revenue + ioe.order_freight_value) AS revenue
    FROM {{ ref('int_order_enriched') }} ioe
    JOIN {{ ref('dim_date') }} dd 
        ON CAST(ioe.purchase_timestamp_local AS DATE) = CAST(dd.full_date AS DATE)
    WHERE ioe.order_status = 'delivered'
    GROUP BY 1, 2
),

date_range AS (
    SELECT 
        MIN(sales_date) as start_date,
        MAX(sales_date) as end_date
    FROM daily_revenue_delivered
),

spined_revenue AS (
    SELECT
        dd.date_sk,
        CAST(dd.full_date AS DATE) as full_date,
        COALESCE(r.revenue, 0) AS daily_revenue
    FROM {{ ref('dim_date') }} dd
    LEFT JOIN daily_revenue_delivered r 
        -- Paksa kedua sisi menjadi INT64 agar cocok
        ON CAST(dd.date_sk AS INT64) = CAST(r.date_sk AS INT64)
    CROSS JOIN date_range
    WHERE CAST(dd.full_date AS DATE) BETWEEN date_range.start_date AND date_range.end_date
),

moving_averages AS (
    SELECT
        date_sk,
        full_date,
        daily_revenue,
        AVG(daily_revenue) OVER (
            ORDER BY full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d_ema,
        AVG(daily_revenue) OVER (
            ORDER BY full_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS revenue_30d_ema
    FROM spined_revenue
)

SELECT
    CAST(date_sk AS STRING) AS date_sk,     
    CAST(full_date AS DATE) AS full_date,
    CAST(daily_revenue AS FLOAT64) AS daily_revenue,
    CAST(revenue_7d_ema AS FLOAT64) AS revenue_7d_ema,
    CAST(revenue_30d_ema AS FLOAT64) AS revenue_30d_ema,
    CASE 
        WHEN revenue_7d_ema > revenue_30d_ema THEN 'Upward'
        ELSE 'Downward'
    END AS momentum_status
FROM moving_averages
ORDER BY full_date
