with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_items as (
    select
        order_id,
        count(order_item_id)        as total_items,
        sum(price)                  as total_price,
        sum(freight_value)          as total_freight
    from {{ ref('stg_order_items') }}
    group by order_id
),

order_payments as (
    select
        order_id,
        sum(payment_value)          as total_payment_value,
        count(payment_sequential)   as total_payments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

order_reviews as (
    select
        order_id,
        avg(review_score)           as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        o.order_status,
        o.order_purchased_at,
        o.order_approved_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        i.total_items,
        i.total_price,
        i.total_freight,
        p.total_payment_value,
        p.total_payments,
        r.avg_review_score,
        datediff('day',
            o.order_purchased_at,
            o.order_delivered_customer_at)  as actual_delivery_days,
        datediff('day',
            o.order_purchased_at,
            o.order_estimated_delivery_at)  as estimated_delivery_days
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join order_items i
        on o.order_id = i.order_id
    left join order_payments p
        on o.order_id = p.order_id
    left join order_reviews r
        on o.order_id = r.order_id
)

select * from final