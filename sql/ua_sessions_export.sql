DECLARE start_date STRING DEFAULT '20160801';
DECLARE end_date STRING DEFAULT '20170801';

SELECT
  PARSE_DATE('%Y%m%d', date) AS session_date,
  CONCAT(fullVisitorId, '.', CAST(visitId AS STRING)) AS session_key,
  fullVisitorId AS user_id,
  visitId AS session_id,
  channelGrouping AS channel_group,
  trafficSource.source AS source,
  trafficSource.medium AS medium,
  device.deviceCategory AS device_category,
  geoNetwork.country AS country,
  totals.visits AS visits,
  totals.pageviews AS pageviews,
  totals.timeOnSite AS time_on_site_seconds,
  totals.bounces AS bounces,
  totals.transactions AS transactions,
  SAFE_DIVIDE(totals.totalTransactionRevenue, 1000000) AS revenue_usd
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
ORDER BY session_date;
