with reviews as (
    select * from (
        select
            review_id,
            order_id,
            review_score,
            DATE(CAST(review_creation_date AS TIMESTAMP), 'Asia/Jakarta') as review_creation_date_local,
            DATE(CAST(review_answer_timestamp AS TIMESTAMP), 'Asia/Jakarta') as review_answer_timestamp_local,
            row_number() over(
                partition by order_id 
                order by DATE(CAST(review_creation_date AS TIMESTAMP), 'Asia/Jakarta') desc
            ) as rn
        from {{ ref('stg_reviews') }}
    )
    where rn = 1
)
select *
from reviews