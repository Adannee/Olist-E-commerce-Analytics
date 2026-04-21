with source as (
    select* from {{source('raw', 'ORDER_REVIEWS')}}
),

renamed as (
    select
    REVIEW_ID as review_id,
    ORDER_ID  as order_id,
    REVIEW_SCORE:: int as review_score,
    REVIEW_COMMENT_TITLE as review_comment_title,
    REVIEW_COMMENT_MESSAGE as review_comment_message,
    REVIEW_CREATION_DATE:: timestamp as review_creation_at,
    REVIEW_ANSWER_TIMESTAMP::timestamp as review_answered_at
    from source
)

select * from renamed