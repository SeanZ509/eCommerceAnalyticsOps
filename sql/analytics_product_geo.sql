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