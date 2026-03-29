with all_dates as (

    select date(purchase_timestamp_local) as dt
    from {{ ref('int_order_enriched') }}

    union all
    select date(delivered_customer_date_local)
    from {{ ref('int_order_enriched') }}

    union all
    select date(estimated_delivery_date_local)
    from {{ ref('int_order_enriched') }}

    union all
    select date(shipping_limit_date_local)
    from {{ ref('int_order_item') }}

    union all
    select date(review_creation_date_local)
    from {{ ref('int_reviews') }}

    union all
    select date(review_answer_timestamp_local)
    from {{ ref('int_reviews') }}

),

date_bounds as (

    select
        DATE_TRUNC(min(dt), MONTH) as start_date,
        max(dt)                    as end_date
    from all_dates

),

date_series as (

    select
        day as full_date
    from date_bounds,
    unnest(generate_date_array(start_date, end_date)) as day

)

select
    CAST(CAST(format_date('%Y%m%d', full_date) as int64) AS STRING) AS date_sk,
    full_date,
    FORMAT_DATE('%A', full_date) AS full_day_name,
    MOD(EXTRACT(DAYOFWEEK FROM full_date) + 5, 7) + 1 AS day_num,
    extract(day from full_date) as day,
    extract(month from full_date) as month,
    FORMAT_DATE('%B', full_date) as month_name,
    extract(year from full_date) as year,
    extract(quarter from full_date) as quarter
from date_series
