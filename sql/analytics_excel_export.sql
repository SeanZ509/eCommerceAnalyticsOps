CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.excel_sales_model AS
SELECT
    o.order_id,
    o.created_at::timestamp::date AS order_date,

    u.id AS customer_id,
    NULLIF(u.age,'')::int AS age,
    u.gender,
    u.state,
    u.country,

    p.id AS product_id,
    p.name AS product_name,
    p.category,
    p.department,

    1 AS quantity,
    oi.sale_price::numeric AS unit_price,
    oi.sale_price::numeric AS revenue

FROM raw.orders o
JOIN raw.order_items oi ON o.order_id = oi.order_id
JOIN raw.products p ON oi.product_id = p.id
JOIN raw.users u ON o.user_id = u.id;