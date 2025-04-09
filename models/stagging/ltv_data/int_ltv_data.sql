SELECT
c.visit_date_utc,
c.property_name,
c.publisher_visit_id,
c.publisher_id,
c.publisher_name,
c.utm_source,
c.utm_term,
c.utm_medium,
c.utm_content,
SUM(c.campaign_impressions) AS campaign_impressions,
SUM(c.campaign_conversions) AS campaign_conversions,
SUM(c.campaign_clicked) AS campaign_clicks,
SUM(c.purchase_count) AS purchase_count,
SUM(c.pixel_revenue) AS pixel_revenue
FROM {{ ref('stg_campaign_event_data') }} c
INNER JOIN {{ ref('stg_campaign_with_purchases') }} fc ON c.campaign_id = fc.campaign_id
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER (partition by c.publisher_visit_id ORDER BY c.visit_date_utc DESC) = 1