with source as (

    select *
    from {{ ref('int_product') }}

),

super_categories as (
    select *
    from {{ref('stg_super_categories')}}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.product_id']) }} as product_sk,
    s.product_id, 
    INITCAP(REPLACE(s.english_name, '_', ' ')) as product_category_name, 
    s.product_name_lenght,
    s.product_description_lenght, 
    s.product_photos_qty, 
    s.product_weight_g, 
    s.product_length_cm, 
    s.product_height_cm, 
    s.product_width_cm,
    INITCAP(REPLACE(sc.super_category, '_', ' ')) AS super_category
from source s
join super_categories sc on s.english_name = sc.english_name