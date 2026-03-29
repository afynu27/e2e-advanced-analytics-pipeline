with reviews as (
    select
        review_id,
        order_id,
        review_score,
        review_creation_date_local,
        review_answer_timestamp_local
    from {{ ref('int_reviews') }}
)

select
    CAST(r.review_id AS STRING) AS review_id,
    CAST(r.order_id AS STRING) AS order_id,
    r.review_score,
    CAST(cust.customer_sk AS STRING) AS customer_sk, 
    dd_create.date_sk as review_creation_date_sk,
    dd_answer.date_sk as review_answer_date_sk,
    ioe.delivery_delay_days,
    ioe.order_status

from reviews r

left join {{ ref('dim_date') }} dd_create
    on r.review_creation_date_local = dd_create.full_date

left join {{ ref('dim_date') }} dd_answer
    on r.review_answer_timestamp_local = dd_answer.full_date

left join {{ ref('int_order_enriched') }} ioe 
    on ioe.order_id = r.order_id

left join {{ ref('dim_customer') }} cust 
    on cust.customer_unique_id = ioe.customer_unique_id