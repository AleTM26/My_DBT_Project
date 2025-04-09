
SELECT
c.campaign_id,
c.campaign_name,
c.advertiser_name,
SUM(c.campaign_impressions) AS campaign_impressions,
SUM(c.campaign_conversions) AS campaign_conversions,
SUM(c.campaign_clicked) AS campaign_clicked,
SUM(c.purchase_count) AS purchase_count,
SUM(c.pixel_revenue) AS pixel_revenue
FROM {{ ref('stg_campaign_event_data') }} c
GROUP BY ALL
HAVING SUM(c.purchase_count) >0
ORDER BY 7 DESC