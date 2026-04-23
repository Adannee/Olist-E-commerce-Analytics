with source as (
    select * from {{ source('raw_v2', 'ORDERS') }}
),

renamed as (
    select
        ORDER_ID                                    as order_id,
        CUSTOMER_ID                                 as customer_id,
        ORDER_STATUS                                as order_status,
        ORDER_PURCHASE_TIMESTAMP::timestamp         as order_purchased_at,
        ORDER_APPROVED_AT::timestamp                as order_approved_at,
        ORDER_DELIVERED_CARRIER_DATE::timestamp     as order_delivered_carrier_at,
        ORDER_DELIVERED_CUSTOMER_DATE::timestamp    as order_delivered_customer_at,
        ORDER_ESTIMATED_DELIVERY_DATE::timestamp    as order_estimated_delivery_at
    from source
)

select * from renamed