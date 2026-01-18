CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.new_vs_returning_customers_quarterly AS
WITH first_order AS (
  SELECT
    user_id,
    MIN(order_date) AS first_order_date
  FROM analytics.fact_order_revenue
  GROUP BY user_id
),
orders_q AS (
  SELECT
    user_id,
    date_trunc('quarter', order_date)::date AS quarter_start
  FROM analytics.fact_order_revenue
  GROUP BY user_id, date_trunc('quarter', order_date)::date
)
SELECT
  q.quarter_start,
  COUNT(*) AS active_customers,
  SUM(CASE WHEN date_trunc('quarter', f.first_order_date)::date = q.quarter_start THEN 1 ELSE 0 END) AS new_customers,
  SUM(CASE WHEN date_trunc('quarter', f.first_order_date)::date < q.quarter_start THEN 1 ELSE 0 END) AS returning_customers,
  ROUND(
    100.0 * SUM(CASE WHEN date_trunc('quarter', f.first_order_date)::date < q.quarter_start THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0),
    2
  ) AS returning_customer_pct
FROM orders_q q
JOIN first_order f
  ON f.user_id = q.user_id
GROUP BY q.quarter_start
ORDER BY q.quarter_start;

CREATE OR REPLACE VIEW analytics.customer_profile_quarterly AS
SELECT
  cq.quarter_start,
  u.id AS user_id,
  u.gender,
  u.age,
  u.state,
  u.country,
  cq.orders,
  cq.revenue,
  cq.aov
FROM analytics.customer_quarterly cq
JOIN raw.users u
  ON u.id = cq.user_id;