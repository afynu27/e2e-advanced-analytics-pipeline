with source as (
    select *
    from {{ ref('olist_products_dataset') }}
),

source_translated as (
    select *
    from {{ ref('product_category_name_translation') }}
),

english_product_name as (
    select
        product_category_name as original_name,
        product_category_name_english as english_name
    from (
        select
            *,
            row_number() over () as rn
        from source_translated
    )
    where rn > 1
)

select
    sc.*,
    en.english_name
from source sc
left join english_product_name en
    on sc.product_category_name = en.original_name
