with campaigns as (
    select * from {{ ref('int_marketing_campaigns_enriched') }}
)

select
    campaign_id,
    company,
    campaign_type,
    channel,
    channel_group,
    location,
    language,
    customer_segment,
    target_audience,
    duration_days,
    campaign_date,
    campaign_month,
    campaign_quarter,
    campaign_day_of_week,
    impressions,
    clicks,
    conversion_rate,
    acquisition_cost,
    roi,
    engagement_score,
    ctr,
    cost_per_click,
    cost_per_conversion,
    roas,
    performance_tier,
    round(clicks * conversion_rate) as estimated_conversions,
    round(acquisition_cost * (1 + roi)) as estimated_revenue
from campaigns