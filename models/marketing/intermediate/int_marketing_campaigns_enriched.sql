with campaigns as (
    select * from {{ ref('stg_marketing_campaigns') }}
),

final as (
    select
        campaign_id,
        company,
        campaign_type,
        target_audience,
        duration_days,
        channel,
        conversion_rate,
        acquisition_cost,
        roi,
        location,
        language,
        clicks,
        impressions,
        engagement_score,
        customer_segment,
        campaign_date,

        case
            when impressions > 0
            then round(clicks / impressions * 100, 2)
            else 0
        end as ctr,

        case
            when clicks > 0
            then round(acquisition_cost / clicks, 2)
            else 0
        end as cost_per_click,

        case
            when conversion_rate > 0
            then round(acquisition_cost / (conversion_rate * clicks), 2)
            else 0
        end as cost_per_conversion,

        case
            when acquisition_cost > 0
            then round((roi * acquisition_cost) / acquisition_cost * 100, 2)
            else 0
        end as roas,

        case
            when roi >= 5 then 'High Performer'
            when roi >= 2 then 'Mid Performer'
            else 'Low Performer'
        end as performance_tier,

        case
            when channel = 'Google Ads' then 'Paid Search'
            when channel = 'YouTube' then 'Paid Search'
            when channel = 'Facebook' then 'Social Media'
            when channel = 'Instagram' then 'Social Media'
            when channel = 'Email' then 'Email'
            else 'Other'
        end as channel_group,

        date_trunc('month', campaign_date) as campaign_month,
        date_trunc('quarter', campaign_date) as campaign_quarter,
        dayname(campaign_date) as campaign_day_of_week

    from campaigns
)

select * from final