with orders as (
    select * from {{ ref('stg_orders') }}
),

order_item as (
    select * from {{ ref('stg_order_items') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

customer as (
    select * from {{ ref('stg_customer') }}
),

deduped_orders as (
    select *
    from (
        select *,
            row_number() over (
                partition by order_id
                order by order_purchase_timestamp desc
            ) as rn
        from orders
    )
    where rn = 1
),

orders_local as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        DATE(CAST(o.order_purchase_timestamp AS TIMESTAMP), 'Asia/Jakarta') as purchase_timestamp_local,
        DATE(CAST(o.order_approved_at AS TIMESTAMP), 'Asia/Jakarta') as approved_at_local,
        DATE(CAST(o.order_delivered_carrier_date AS TIMESTAMP), 'Asia/Jakarta') as delivered_carrier_date_local,
        DATE(CAST(o.order_delivered_customer_date AS TIMESTAMP), 'Asia/Jakarta') as delivered_customer_date_local,
        DATE(CAST(o.order_estimated_delivery_date AS TIMESTAMP), 'Asia/Jakarta') as estimated_delivery_date_local
    from deduped_orders o
),

order_enriched as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_status,
        o.purchase_timestamp_local,
        o.approved_at_local,
        o.delivered_carrier_date_local,
        o.delivered_customer_date_local,
        o.estimated_delivery_date_local,
        DATE_DIFF(o.delivered_carrier_date_local, o.approved_at_local, DAY) as shipping_days,
        DATE_DIFF(o.delivered_customer_date_local, o.approved_at_local, DAY) as delivery_days,
        DATE_DIFF(o.estimated_delivery_date_local, o.approved_at_local, DAY) as delivery_estimation_days,
        DATE_DIFF(o.delivered_customer_date_local, o.estimated_delivery_date_local, DAY) as delivery_delay_days,
        count(oi.order_item_id) as order_item_quantity,
        sum(oi.price) as order_revenue,
        sum(oi.freight_value) as order_freight_value,
        count(p.order_id) as payment_count,
        coalesce(sum(p.payment_value), 0) as total_payment_value
    from orders_local o
    join order_item oi on o.order_id = oi.order_id
    join customer c on o.customer_id = c.customer_id
    left join payments p on o.order_id = p.order_id
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select 
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_sk,
    *
from order_enriched