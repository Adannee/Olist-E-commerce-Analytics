with source as (
    select * from {{source('marketing_raw', 'MARKETING_CAMPAIGNS')}}
),
renamed as (
    select
    CAMPAIGN_ID           as campaign_id,
    COMPANY               as company,
    CAMPAIGN_TYPE         as campaign_type,
    TARGET_AUDIENCE       as target_audience,
    DURATION::int         as duration_days,
    CHANNEL_USED          as channel,
    CONVERSION_RATE::float as conversion_rate,
    ACQUISITION_COST::float as acquisition_cost,
    ROI::float              as roi,
    LOCATION                as location,
    LANGUAGE                as language,
    CLICKS::int             as clicks,
    IMPRESSIONS::int        as impressions,
    ENGAGEMENT_SCORE::int   as engagement_score,
    CUSTOMER_SEGMENT        as customer_segment,
    DATE::date              as campaign_date
    from source

)

select * from renamed