SELECT
    id,
    visit_tracking_id,
    campaign_tracking_id,
    purchase,
    revenue/1e6 AS revenue,
    CONVERT_TIMEZONE('US/Pacific', 'UTC', timestamp_max) timestamp_max
    CONVERT_TIMEZONE('US/Pacific', 'UTC', timestamp_min) timestamp_min
FROM {{ source("zan_zmetrics", "agg_visit_campaign_pixel") }}