CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.dim_quarter AS
WITH bounds AS (
  SELECT
    date_trunc('quarter', MIN(created_at::timestamp))::date AS min_q,
    date_trunc('quarter', MAX(created_at::timestamp))::date AS max_q
  FROM raw.orders
  WHERE created_at IS NOT NULL AND created_at <> ''
)
SELECT
  gs::date AS quarter_start,
  EXTRACT(YEAR FROM gs)::int AS year,
  EXTRACT(QUARTER FROM gs)::int AS quarter,
  (EXTRACT(YEAR FROM gs)::int || ' Q' || EXTRACT(QUARTER FROM gs)::int)::text AS year_quarter_label
FROM bounds,
LATERAL generate_series(bounds.min_q, bounds.max_q, interval '3 months') gs;

CREATE OR REPLACE VIEW analytics.customer_segment_quarterly AS
SELECT
  cq.quarter_start,
  u.id AS user_id,
  u.gender,
  u.state,
  u.country,
  NULLIF(u.age,'')::int AS age,
  CASE
    WHEN NULLIF(u.age,'') IS NULL THEN 'unknown'
    WHEN NULLIF(u.age,'')::int < 18 THEN '<18'
    WHEN NULLIF(u.age,'')::int BETWEEN 18 AND 24 THEN '18-24'
    WHEN NULLIF(u.age,'')::int BETWEEN 25 AND 34 THEN '25-34'
    WHEN NULLIF(u.age,'')::int BETWEEN 35 AND 44 THEN '35-44'
    WHEN NULLIF(u.age,'')::int BETWEEN 45 AND 54 THEN '45-54'
    WHEN NULLIF(u.age,'')::int BETWEEN 55 AND 64 THEN '55-64'
    ELSE '65+'
  END AS age_group,
  cq.orders,
  cq.revenue,
  cq.aov
FROM analytics.customer_quarterly cq
JOIN raw.users u
  ON u.id = cq.user_id;

CREATE OR REPLACE VIEW analytics.repeat_customer_rate_quarterly AS
WITH cust_q AS (
  SELECT
    user_id,
    date_trunc('quarter', order_date)::date AS quarter_start,
    COUNT(DISTINCT order_id) AS orders_in_quarter
  FROM analytics.fact_order_revenue
  GROUP BY 1,2
)
SELECT
  quarter_start,
  COUNT(*) AS active_customers,
  SUM(CASE WHEN orders_in_quarter >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
  ROUND(100.0 * SUM(CASE WHEN orders_in_quarter >= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS repeat_rate_pct
FROM cust_q
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW analytics.basket_kpis_quarterly AS
SELECT
  date_trunc('quarter', o.order_date)::date AS quarter_start,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(o.order_revenue) AS revenue,
  ROUND(SUM(o.order_revenue) / NULLIF(COUNT(DISTINCT o.order_id),0), 2) AS aov,
  SUM(o.items_count) AS items_sold,
  ROUND(SUM(o.items_count)::numeric / NULLIF(COUNT(DISTINCT o.order_id),0), 2) AS items_per_order
FROM analytics.fact_order_revenue o
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW analytics.category_kpis_quarterly AS
SELECT
  date_trunc('quarter', oi.order_date)::date AS quarter_start,
  COALESCE(p.category, 'unknown') AS category,
  COUNT(*) AS items_sold,
  SUM(oi.sale_price) AS revenue,
  ROUND(SUM(oi.sale_price) / NULLIF(COUNT(*),0), 2) AS revenue_per_item
FROM analytics.fact_order_items oi
LEFT JOIN raw.products p
  ON p.id = oi.product_id
GROUP BY 1,2
ORDER BY 1, revenue DESC;

CREATE OR REPLACE VIEW analytics.state_kpis_quarterly AS
SELECT
  date_trunc('quarter', o.order_date)::date AS quarter_start,
  COALESCE(u.state, 'unknown') AS state,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(o.order_revenue) AS revenue,
  ROUND(SUM(o.order_revenue) / NULLIF(COUNT(DISTINCT o.order_id),0), 2) AS aov
FROM analytics.fact_order_revenue o
JOIN raw.users u
  ON u.id = o.user_id
GROUP BY 1,2
ORDER BY 1, revenue DESC;

CREATE OR REPLACE VIEW analytics.fulfillment_kpis_quarterly AS
WITH t AS (
  SELECT
    date_trunc('quarter', created_at::timestamp)::date AS quarter_start,
    EXTRACT(EPOCH FROM (shipped_at::timestamp - created_at::timestamp)) / 3600.0 AS hours_to_ship
  FROM raw.orders
  WHERE created_at IS NOT NULL AND created_at <> ''
    AND shipped_at IS NOT NULL AND shipped_at <> ''
)
SELECT
  quarter_start,
  ROUND(AVG(hours_to_ship), 2) AS avg_hours_to_ship,
  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY hours_to_ship)::numeric, 2) AS p90_hours_to_ship
FROM t
GROUP BY 1
ORDER BY 1;