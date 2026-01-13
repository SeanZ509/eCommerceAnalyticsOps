CREATE SCHEMA IF NOT EXISTS analytics;

DROP VIEW IF EXISTS analytics.kpi_daily;
CREATE VIEW analytics.kpi_daily AS
SELECT
  DATE(oi.created_at::timestamp) AS order_date,
  COUNT(DISTINCT oi.order_id) AS orders,
  COUNT(*) AS items,
  SUM(COALESCE(oi.sale_price::numeric,0)) AS revenue,
  CASE
    WHEN COUNT(DISTINCT oi.order_id) > 0
    THEN SUM(COALESCE(oi.sale_price::numeric,0)) / COUNT(DISTINCT oi.order_id)
    ELSE NULL
  END AS aov
FROM raw.order_items oi
GROUP BY 1
ORDER BY 1;

DROP VIEW IF EXISTS analytics.category_alltime;
CREATE VIEW analytics.category_alltime AS
SELECT
  p.category,
  SUM(COALESCE(oi.sale_price::numeric,0)) AS revenue,
  COUNT(*) AS items
FROM raw.order_items oi
JOIN raw.products p
  ON p.id = oi.product_id
GROUP BY 1
ORDER BY revenue DESC;

DROP VIEW IF EXISTS analytics.customer_repeat_rate;
CREATE VIEW analytics.customer_repeat_rate AS
WITH orders_by_user AS (
  SELECT
    user_id,
    COUNT(*) AS orders
  FROM raw.orders
  WHERE user_id IS NOT NULL
  GROUP BY 1
)
SELECT
  COUNT(*) FILTER (WHERE orders = 1)::float / NULLIF(COUNT(*),0) AS pct_one_time,
  COUNT(*) FILTER (WHERE orders >= 2)::float / NULLIF(COUNT(*),0) AS pct_repeat
FROM orders_by_user;

DROP VIEW IF EXISTS analytics.fulfillment_times;
CREATE VIEW analytics.fulfillment_times AS
SELECT
  DATE(o.created_at::timestamp) AS order_date,
  COUNT(*) AS orders,
  AVG(
    EXTRACT(EPOCH FROM (o.shipped_at::timestamp - o.created_at::timestamp)) / 3600.0
  ) AS avg_hours_to_ship
FROM raw.orders o
WHERE o.shipped_at IS NOT NULL
  AND o.created_at IS NOT NULL
GROUP BY 1
ORDER BY 1;