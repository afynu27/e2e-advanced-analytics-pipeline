with payment as (
    select order_id,
    payment_sequential,
    payment_installments,
    payment_value,
    case
        when payment_type = 'boleto' then 'payment_slip'
        else payment_type
    end as payment_type_standard
    from {{ref('stg_payments')}}
)
select *
from payment