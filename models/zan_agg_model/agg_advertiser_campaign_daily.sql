select
    to_date(timestamp_pst) date,
    account_id,
    campaign_id,
    placement_id,
    publisher_id,
    sum(case when event_type = 'campaign_clicked' then 1 else 0 end) clicks,
    sum(case when event_type = 'campaign_impressed' then 1 else 0 end) impressions,
    sum(
        case when event_type = 'campaign_recommended' then 1 else 0 end
    ) recommendations,
    sum(case when event_type = 'campaign_converted' then 1 else 0 end) conversions,
    sum(
        case when event_type = 'campaign_converted' then revenue else 0 end
    ) advertiser_cost,
    sum(case when event_type = 'campaign_converted' then revenue else 0 end) - sum(
        case
            when event_type = 'campaign_converted' then revenue * rev_share / 1e6 else 0
        end
    ) zeeto_revenue,
    sum(case when event_type = 'lead_accepted' then 1 else 0 end) leads_accepted,
    sum(case when event_type = 'lead_rejected' then 1 else 0 end) leads_rejected,
    (
        sum(case when event_type = 'lead_accepted' then 1 else 0 end)
        + sum(case when event_type = 'lead_rejected' then 1 else 0 end)
        + sum(case when event_type = 'lead_failed' then 1 else 0 end)
    ) delivery_count
from {{ source("zan_zevent", "event_visit_campaign") }}
where date is not null and date < current_date
group by all
