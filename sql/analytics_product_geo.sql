CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.category_quarterly AS
SELECT
  date_trunc('quarter', oi.order_date)::date AS quarter_start,
  COALESCE(p.category, 'unknown') AS category,
  COUNT(*) AS items_sold,
  SUM(oi.sale_price) AS revenue
FROM analytics.fact_order_items oi
LEFT JOIN raw.products p
  ON p.id = oi.product_id
GROUP BY
  date_trunc('quarter', oi.order_date)::date, COALESCE(p.category, 'unknown')
ORDER BY quarter_start, revenue DESC;

CREATE OR REPLACE VIEW analytics.state_quarterly AS
SELECT
  date_trunc('quarter', o.order_date)::date AS quarter_start,
  COALESCE(u.state, 'unknown') AS state,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(o.order_revenue) AS revenue
FROM analytics.fact_order_revenue o
JOIN raw.users u
  ON u.id = o.user_id
GROUP BY
  date_trunc('quarter', o.order_date)::date, COALESCE(u.state, 'unknown')
ORDER BY quarter_start, revenue DESC;

CREATE OR REPLACE VIEW analytics.top_products_alltime AS
SELECT
  oi.product_id,
  COALESCE(p.name, 'unknown') AS product_name,
  COALESCE(p.category, 'unknown') AS category,
  COUNT(*) AS items_sold,
  SUM(oi.sale_price) AS revenue
FROM analytics.fact_order_items oi
LEFT JOIN raw.products p
  ON p.id = oi.product_id
GROUP BY
  oi.product_id, COALESCE(p.name, 'unknown'), COALESCE(p.category, 'unknown')
ORDER BY revenue DESC;