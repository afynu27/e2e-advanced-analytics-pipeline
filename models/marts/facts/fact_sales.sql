with ioe as (
    select * from {{ ref('int_order_enriched') }}
),

dc as (
    select * from {{ ref('dim_customer') }}
),

dd as (
    select * from {{ ref('dim_date') }}
)

select 
    CAST(ioe.order_id AS STRING) AS order_id, 
    CAST(dc.customer_sk AS STRING) AS customer_sk, 
    CAST(dd_a.date_sk AS STRING) AS purchase_date_sk, 
    CAST(dd_b.date_sk AS STRING) AS approved_date_sk,
    CAST(dd_c.date_sk AS STRING) AS delivered_carrier_date_sk, 
    CAST(dd_d.date_sk AS STRING) AS delivered_customer_date_sk,
    CAST(dd_e.date_sk AS STRING) AS estimated_delivery_date_sk,
    ioe.shipping_days,
    ioe.delivery_days,
    ioe.delivery_estimation_days,
    ioe.delivery_delay_days,
    ioe.order_item_quantity,
    ioe.order_revenue,
    ioe.order_freight_value,
    ioe.payment_count,
    ioe.total_payment_value,
    ioe.order_status
from ioe
join dc on dc.customer_sk = ioe.customer_sk 
left join dd dd_a on dd_a.full_date = ioe.purchase_timestamp_local
left join dd dd_b on dd_b.full_date = ioe.approved_at_local
left join dd dd_c on dd_c.full_date = ioe.delivered_carrier_date_local
left join dd dd_d on dd_d.full_date = ioe.delivered_customer_date_local
left join dd dd_e on dd_e.full_date = ioe.estimated_delivery_date_local