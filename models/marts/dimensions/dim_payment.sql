with source as (

    select *
    from {{ ref('int_payment') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
    'order_id',
    'payment_sequential'
]) }} as payment_sk, order_id, payment_sequential, payment_installments, 
INITCAP(REPLACE(payment_type_standard, '_', ' ')) AS payment_type_standard
from source