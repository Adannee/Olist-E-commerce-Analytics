with orders as (
    select * from {{ ref('int_v2_orders_enriched') }}
)

select
    order_id,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    order_status,
    order_purchased_at,
    order_approved_at,
    order_delivered_customer_at,
    order_estimated_delivery_at,
    total_items,
    total_price,
    total_freight,
    total_payment_value,
    total_payments,
    avg_review_score,
    actual_delivery_days,
    estimated_delivery_days,
    case
        when actual_delivery_days <= estimated_delivery_days then 'On Time'
        when actual_delivery_days > estimated_delivery_days then 'Late'
        else 'Not Delivered'
    end as delivery_status
from orders