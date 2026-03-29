with ip as (

    select *
    from {{ ref('int_payment') }}

),

dp as (

    select *
    from {{ ref('dim_payment') }}

),
ioe as (
    select order_id, order_status
    from {{ref('int_order_enriched')}}
)

select
    CAST(dp.payment_sk AS STRING) AS payment_sk,
    CAST(ip.order_id AS STRING) AS order_id,
    ip.payment_sequential,
    ip.payment_value,
    ioe.order_status
from ip
join dp
    on ip.order_id = dp.order_id
    and ip.payment_sequential = dp.payment_sequential
join ioe 
    on ip.order_id = ioe.order_id