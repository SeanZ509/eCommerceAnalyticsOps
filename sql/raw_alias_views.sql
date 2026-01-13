CREATE OR REPLACE VIEW raw.events AS
SELECT * FROM raw."thelook_ecommerce.events";

CREATE OR REPLACE VIEW raw.orders AS
SELECT * FROM raw."thelook_ecommerce.orders";

CREATE OR REPLACE VIEW raw.order_items AS
SELECT * FROM raw."thelook_ecommerce.order_items";

CREATE OR REPLACE VIEW raw.products AS
SELECT * FROM raw."thelook_ecommerce.products";

CREATE OR REPLACE VIEW raw.users AS
SELECT * FROM raw."thelook_ecommerce.users";

CREATE OR REPLACE VIEW raw.inventory_items AS
SELECT * FROM raw."thelook_ecommerce.inventory_items";

CREATE OR REPLACE VIEW raw.distribution_centers AS
SELECT * FROM raw."thelook_ecommerce.distribution_centers";