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
    CAST(
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'ga_session_number'
      ) AS INT64
    ) AS ga_session_number,
    event_name,
    device.category AS device_category,
    geo.country AS country,
    COALESCE(NULLIF(traffic_source.source, ''), '(direct)') AS source,
    COALESCE(NULLIF(traffic_source.medium, ''), '(none)') AS medium,
    (
      SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING))
      FROM UNNEST(event_params)
      WHERE key = 'session_engaged'
    ) AS session_engaged,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
    ) AS page_location,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue_in_usd AS purchase_revenue_usd
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
)
SELECT
  event_date,
  event_timestamp,
  user_pseudo_id,
  ga_session_id,
  CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING)) AS session_key,
  ga_session_number,
  event_name,
  device_category,
  country,
  source,
  medium,
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
  session_engaged,
  page_location,
  transaction_id,
  purchase_revenue_usd
FROM base_events
WHERE ga_session_id IS NOT NULL
ORDER BY event_timestamp;
