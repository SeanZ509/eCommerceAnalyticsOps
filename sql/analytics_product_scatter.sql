CREATE OR REPLACE VIEW analytics.product_sales_summary AS
SELECT
  p.id AS product_id,
  p.name AS product_name,
  p.category,
  COUNT(*) AS units_sold,
  SUM(oi.sale_price) AS revenue,
  ROUND(AVG(oi.sale_price), 2) AS avg_price
FROM analytics.fact_order_items oi
JOIN raw.products p
  ON p.id = oi.product_id
GROUP BY
  p.id, p.name, p.category;