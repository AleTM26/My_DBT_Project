SELECT
DATE(ec.timestamp) AS visit_date_utc,
DATE(ec.timestamp_pst) AS visit_date,
LOWER(TRIM(pp.name)) AS property_name,
LOWER(TRIM(pp.publisher_id)) AS publisher_id,
LOWER(TRIM(ec.publisher_visit_id)) AS publisher_visit_id,
LOWER(TRIM(ps.name)) AS publisher_name,
ec.campaign_id AS campaign_id,
c.name AS campaign_name,
a.name AS advertiser_name,
LOWER(TRIM(ec.utm_source)) AS utm_source,

CASE
WHEN ec.utm_term IS NULL
THEN 'no term'
ELSE LOWER(ec.utm_term)
END AS utm_term,

CASE
WHEN ec.utm_medium IS NOT NULL THEN LOWER(TRIM(ec.utm_medium))
WHEN ec.utm_medium IS NULL AND LOWER(TRIM(ec.utm_source)) LIKE 'ev%' AND LENGTH(ec.c3) = 0 THEN 'no medium'
WHEN ec.utm_medium IS NULL AND LOWER(TRIM(ec.utm_source)) LIKE 'ev%' AND ec.c3 IS NOT NULL THEN LOWER(TRIM(ec.c3))
ELSE 'no medium'
END AS utm_medium,

CASE
WHEN ec.utm_content IS NULL
THEN 'no content'
ELSE LOWER(TRIM(ec.utm_content))
END AS utm_content,

COUNT(DISTINCT(CASE WHEN ec.event_type = 'campaign_impressed' THEN ec.campaign_tracking_id ELSE NULL END)) AS campaign_impressions,
COUNT(DISTINCT(CASE WHEN ec.event_type = 'campaign_converted' THEN ec.campaign_tracking_id ELSE NULL END)) AS campaign_conversions,
COUNT(DISTINCT(CASE WHEN ec.event_type = 'campaign_clicked' THEN ec.campaign_tracking_id ELSE NULL END)) AS campaign_clicked,
COUNT(DISTINCT(CASE WHEN ec.event_type = 'campaign_converted' AND px.purchase >0 THEN ec.campaign_tracking_id ELSE NULL END)) AS purchase_count,
SUM(CASE WHEN ec.event_type = 'campaign_converted' AND px.purchase >0 THEN px.revenue ELSE NULL END) AS pixel_revenue

FROM {{ source("zan_zevent", "event_visit_campaign") }} ec
LEFT JOIN zan_prod.zpg_placement.properties AS pp ON ec.property_id = pp.id
LEFT JOIN zan_prod.zpg_placement.publishers AS ps ON ec.publisher_id = ps.id
LEFT JOIN zan_prod.zpg_placement.placements AS p ON ec.placement_id = p.id
LEFT JOIN zan_prod.zpg_campaign.campaigns c ON ec.campaign_id = c.id
LEFT JOIN zan_prod.zpg_campaign.advertisers a ON c.account_id = a.id
LEFT JOIN pixel_data as px ON ec.visit_tracking_id = px.visit_tracking_id AND ec.campaign_tracking_id = px.campaign_tracking_id AND ec.event_type = 'campaign_converted'

WHERE 1=1
--AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(ec.timestamp_pst AS TIMESTAMP_NTZ)) >= '2025-03-01'
--AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(ec.timestamp_pst AS TIMESTAMP_NTZ)) < '2025-03-25'
AND ec.event_type IN ('campaign_impressed', 'campaign_clicked', 'campaign_converted')
AND LOWER(ec.placement_type) = 'linkout'
AND LOWER(ec.rate_type) = 'cpc'
GROUP BY ALL