with source as (
    select * from {{ source('raw_v2', 'ORDER_ITEMS') }}
),

renamed as (
    select
        ORDER_ID                            as order_id,
        ORDER_ITEM_ID                       as order_item_id,
        PRODUCT_ID                          as product_id,
        SELLER_ID                           as seller_id,
        SHIPPING_LIMIT_DATE::timestamp      as shipping_limit_at,
        PRICE::float                        as price,
        FREIGHT_VALUE::float                as freight_value
    from source
)

select * from renamed