SELECT
    id,
    visit_tracking_id,
    campaign_tracking_id,
    purchase,
    revenue/1e6 AS revenue,
    timestamp_max
FROM ZAN_PROD.ZMETRICS.AGG_VISIT_CAMPAIGN_PIXEL agg
WHERE 1=1
--AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(timestamp_min AS TIMESTAMP_NTZ)) >= '2025-03-01'
--AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(timestamp_min AS TIMESTAMP_NTZ)) < '2025-03-25'
AND {% incrementcondition %} CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(timestamp_min AS TIMESTAMP_NTZ)) {% endincrementcondition %}