with products as (
    select * from {{ ref('stg_v2_products') }}
),

translations as (
    select * from {{ ref('stg_v2_product_category_translation') }}
),

order_items as (
    select
        product_id,
        count(order_id)         as total_orders,
        sum(price)              as total_revenue,
        avg(price)              as avg_price
    from {{ ref('stg_v2_order_items') }}
    group by product_id
),

final as (
    select
        p.product_id,
        coalesce(t.product_category_name_english,
            p.product_category_name, 'uncategorized') as product_category,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        i.total_orders,
        i.total_revenue,
        i.avg_price
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
    left join order_items i
        on p.product_id = i.product_id
)

select * from final