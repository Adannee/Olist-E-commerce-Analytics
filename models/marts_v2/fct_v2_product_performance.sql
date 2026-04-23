with products as (
    select * from {{ ref('int_v2_products_enriched') }}
)

select
    product_id,
    product_category,
    product_weight_g,
    total_orders,
    total_revenue,
    avg_price,
    case
        when total_orders >= 100 then 'High Demand'
        when total_orders >= 50 then 'Medium Demand'
        else 'Low Demand'
    end as demand_tier
from products