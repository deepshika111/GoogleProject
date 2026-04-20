DECLARE start_date STRING DEFAULT '20201101';
DECLARE end_date STRING DEFAULT '20210131';

WITH base_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    user_pseudo_id,
    CAST(
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_id'
      ) AS INT64
    ) AS ga_session_id,
    event_name,
    device.category AS device_category,
    COALESCE(NULLIF(traffic_source.source, ''), '(direct)') AS source,
    COALESCE(NULLIF(traffic_source.medium, ''), '(none)') AS medium,
    ecommerce.purchase_revenue_in_usd AS purchase_revenue_usd
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),
session_rollup AS (
  SELECT
    event_date AS session_date,
    CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING)) AS session_key,
    device_category,
    CASE
      WHEN source = '(direct)' AND medium IN ('(none)', '(not set)') THEN 'Direct'
      WHEN REGEXP_CONTAINS(medium, r'(?i)^(cpc|ppc|paidsearch)$') THEN 'Paid Search'
      WHEN REGEXP_CONTAINS(medium, r'(?i)^organic$') THEN 'Organic Search'
      WHEN REGEXP_CONTAINS(medium, r'(?i)^referral$') THEN 'Referral'
      WHEN REGEXP_CONTAINS(medium, r'(?i)^email$') THEN 'Email'
      WHEN REGEXP_CONTAINS(medium, r'(?i)affiliate') THEN 'Affiliates'
      WHEN REGEXP_CONTAINS(medium, r'(?i)(social|paid_social|social[-_ ]network|social[-_ ]media)') THEN 'Social'
      WHEN REGEXP_CONTAINS(medium, r'(?i)(display|banner|cpm)') THEN 'Display'
      ELSE 'Other'
    END AS channel_group,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS converted,
    SUM(COALESCE(purchase_revenue_usd, 0)) AS revenue_usd
  FROM base_events
  WHERE ga_session_id IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT
  session_date,
  channel_group,
  device_category,
  COUNT(*) AS sessions,
  SUM(converted) AS purchasers,
  ROUND(SAFE_DIVIDE(SUM(converted), COUNT(*)), 4) AS conversion_rate,
  ROUND(SUM(revenue_usd), 2) AS revenue_usd
FROM session_rollup
GROUP BY 1, 2, 3
ORDER BY session_date, sessions DESC;
