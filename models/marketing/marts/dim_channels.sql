with campaigns as (
    select * from {{ref('int_marketing_campaigns_enriched')}}
),

channel_summary as (
    select
    channel,
    channel_group,
    count(campaign_id)            as total_campaigns,
    sum(impressions)              as total_impressions,
    sum(clicks)                   as total_clicks,
    sum(acquisition_cost)          as total_spent,
    sum(round(acquisition_cost *(1+roi))) as total_revenue,
    avg(conversion_rate)          as avg_conversion_rate,
    avg(roi)                      as avg_roi,
    avg(ctr)                      as avg_ctr,
    avg(cost_per_click)           as avg_cpc,
    avg(engagement_score)         as avg_engagement_score,
    case
       when sum(acquisition_cost) > 0
       then round(sum(round(acquisition_cost * (1 + roi)))/ sum(acquisition_cost), 2)
       else 0
    end                          as overall_roas
    from campaigns
    group by channel, channel_group
)

select * from channel_summary