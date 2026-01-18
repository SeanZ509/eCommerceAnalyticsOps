-- sql/analytics_fact_views.sql
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1) Order-level revenue (keeps user_id + order_id + date)
CREATE OR REPLACE VIEW analytics.fact_order_revenue AS
SELECT
  o.order_id,
  o.user_id,
  o.created_at::date AS order_date,
  SUM(oi.sale_price) AS order_revenue,
  COUNT(*) AS items_count
FROM raw.orders o
JOIN raw.order_items oi
  ON oi.order_id = o.order_id
GROUP BY
  o.order_id, o.user_id, o.created_at::date;