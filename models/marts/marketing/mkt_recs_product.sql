WITH customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

antecedent AS (
    SELECT INITCAP(REPLACE(antecedents, '_', ' ')) as antecedents, 
    INITCAP(REPLACE(consequents, '_', ' ')) as consequents 
    FROM {{ ref('stg_product_recomendation') }} LIMIT 1
),

last_purchase AS (
    SELECT *
    FROM (
        SELECT 
            ioe.*, 
            ioi.*,
            a.consequents AS consequent_cat,
            row_number() OVER(
                PARTITION BY ioe.customer_unique_id
                ORDER BY ioe.purchase_timestamp_local DESC
            ) AS ls
        FROM {{ ref('int_order_enriched') }} ioe
        JOIN {{ ref('int_order_item') }} ioi
            ON ioe.order_id = ioi.order_id
        JOIN {{ ref('dim_product') }} dp 
            ON ioi.product_id = dp.product_id
        CROSS JOIN antecedent a
        WHERE dp.product_category_name = a.antecedents
    ) t
    WHERE ls = 1
)

SELECT 
    CAST(c.customer_sk AS STRING) AS customer_sk,
    c.rfm_segment,
    c.clv_badge,
    CASE 
        WHEN c.rfm_segment IN ('Champions', 'Loyal Customers') AND c.clv_badge = 'Platinum' 
            THEN 'VIP White-Glove: Exclusive Catalog for ' || lp.consequent_cat || ' (No Discount)'
        WHEN c.rfm_segment IN ('Champions', 'Loyal Customers') AND c.clv_badge IN ('Gold', 'Silver') 
            THEN 'Loyalty Reward: 10x Points for ' || lp.consequent_cat || ' Purchase'
        WHEN c.rfm_segment = 'New Customers' 
            THEN 'Activation: 20% Discount for First ' || lp.consequent_cat || ' Order'
        WHEN c.rfm_segment IN ('At Risk', 'Hibernating') AND c.clv_badge IN ('Platinum', 'Gold') 
            THEN 'High-Value Recovery: 25% Off + Free Shipping on ' || lp.consequent_cat
        ELSE 'Re-activate: Limited Time 15% Voucher for ' || lp.consequent_cat
    END AS marketing_action
FROM customers c
INNER JOIN last_purchase lp 
    ON c.customer_unique_id = lp.customer_unique_id
WHERE c.rfm_segment IS NOT NULL AND c.clv_badge IS NOT NULL