SELECT
    id,
    visit_tracking_id,
    campaign_tracking_id,
    purchase,
    revenue/1e6 AS revenue,
    timestamp_max,
    timestamp_min
FROM {{ source("zan_zmetrics", "agg_visit_campaign_pixel") }}
