# Dashboard And Handoff Spec

## Power BI pages

1. Funnel Overview
   Show sessions, product views, add-to-cart sessions, checkout sessions, purchasers, conversion rate, and cart abandonment.
2. Channel Performance
   Rank channels by sessions, conversion rate, revenue, and revenue per session. Add slicers for device and date.
3. Device Breakdown
   Compare desktop, mobile, and tablet across conversion rate, bounce-like rate, and session duration.
4. Trend Monitoring
   Plot daily sessions, daily purchasers, and daily conversion rate by channel.

## Recommended visuals

- Funnel visual with stage-to-stage drop-off percentages
- Clustered bar chart for conversion rate by channel
- Matrix for channel x device with conditional formatting
- Daily line chart with channel legend
- KPI cards for sessions, purchasers, conversion rate, revenue, and revenue per session

## Excel workbook tabs

1. `Executive Summary`
   One-page stakeholder summary with top metrics and takeaway notes.
2. `Funnel Detail`
   Session counts and drop-off rates across the funnel.
3. `Channel Pivot`
   Channel-level sessions, purchases, conversion, and revenue.
4. `Device Pivot`
   Device-level conversion and bounce-like behavior.

## Talking points for interviews

- Why channel was treated as a pseudo-experimental group
- Why proportions testing and chi-square were both included
- How the logistic regression separated behavioral effects from acquisition effects
- How the dashboard was designed for a non-technical stakeholder
