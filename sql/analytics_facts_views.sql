CREATE SCHEMA IF NOT EXISTS analytics;

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

CREATE OR REPLACE VIEW analytics.fact_order_items AS
SELECT
  o.order_id,
  o.user_id,
  o.created_at::date AS order_date,
  oi.product_id,
  oi.sale_price,
  1 AS item_qty
FROM raw.orders o
JOIN raw.order_items oi
  ON oi.order_id = o.order_id;

CREATE OR REPLACE VIEW analytics.customer_quarterly AS
SELECT
  user_id,
  date_trunc('quarter', order_date)::date AS quarter_start,
  COUNT(DISTINCT order_id) AS orders,
  SUM(order_revenue) AS revenue,
  AVG(order_revenue) AS aov
FROM analytics.fact_order_revenue
GROUP BY
  user_id, date_trunc('quarter', order_date)::date;