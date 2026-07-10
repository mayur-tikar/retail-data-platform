-- ============================================================
-- Retail Data Warehouse Schema
-- Running on Postgres locally (Redshift substitute)
-- Same SQL works on Redshift with minor changes:
--   1. Add DISTKEY/SORTKEY to CREATE TABLE statements
--   2. Replace \COPY with Redshift COPY command from S3
-- ============================================================

-- WHY two schemas?
-- 'staging' = landing zone, data lands here first unvalidated
-- 'warehouse' = production tables, only clean validated data
-- If staging load fails, warehouse is untouched

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ─────────────────────────────────────────────
-- STAGING TABLES
-- ─────────────────────────────────────────────

DROP TABLE IF EXISTS staging.fact_orders;
DROP TABLE IF EXISTS staging.dim_customers;
DROP TABLE IF EXISTS staging.dim_products;
DROP TABLE IF EXISTS staging.dim_sellers;
DROP TABLE IF EXISTS staging.dim_date;
DROP TABLE IF EXISTS staging.revenue_by_category;
DROP TABLE IF EXISTS staging.customer_lifetime_value;
DROP TABLE IF EXISTS staging.seller_performance;

CREATE TABLE staging.dim_date (
    date_key        INTEGER,
    full_date       DATE,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week            INTEGER,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    fiscal_year     INTEGER,
    fiscal_quarter  INTEGER,
    year_month      VARCHAR(10)
);

CREATE TABLE staging.dim_customers (
    customer_unique_id      VARCHAR(50),
    customer_id             VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(5),
    dw_created_at           TIMESTAMP
);

CREATE TABLE staging.dim_products (
    product_id              VARCHAR(50),
    product_category_name   VARCHAR(100),
    category_english        VARCHAR(100),
    product_weight_g        DOUBLE PRECISION,
    product_length_cm       DOUBLE PRECISION,
    product_height_cm       DOUBLE PRECISION,
    product_width_cm        DOUBLE PRECISION,
    product_photos_qty      INTEGER,
    volume_cm3              DOUBLE PRECISION,
    weight_tier             VARCHAR(20),
    dw_created_at           TIMESTAMP
);

CREATE TABLE staging.dim_sellers (
    seller_id               VARCHAR(50),
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5),
    dw_created_at           TIMESTAMP
);

CREATE TABLE staging.fact_orders (
    order_id                VARCHAR(50),
    order_item_id           INTEGER,
    product_id              VARCHAR(50),
    seller_id               VARCHAR(50),
    customer_unique_id      VARCHAR(50),
    order_date_key          INTEGER,
    price                   DOUBLE PRECISION,
    freight_value           DOUBLE PRECISION,
    gross_revenue           DOUBLE PRECISION,
    total_payment_value     DOUBLE PRECISION,
    max_installments        INTEGER,
    primary_payment_type    VARCHAR(30),
    order_status            VARCHAR(30),
    was_delivered_on_time   BOOLEAN,
    delivery_delay_days     INTEGER,
    purchase_year           INTEGER,
    purchase_month          INTEGER
);

CREATE TABLE staging.revenue_by_category (
    purchase_year           INTEGER,
    purchase_month          INTEGER,
    category_english        VARCHAR(100),
    total_revenue           DOUBLE PRECISION,
    total_freight           DOUBLE PRECISION,
    total_product_revenue   DOUBLE PRECISION,
    order_count             BIGINT,
    item_sold              BIGINT,
    avg_item_price          DOUBLE PRECISION
);

CREATE TABLE staging.customer_lifetime_value (
    customer_unique_id      VARCHAR(50),
    total_orders            BIGINT,
    lifetime_value          DOUBLE PRECISION,
    avg_order_value         DOUBLE PRECISION,
    total_freight_paid      DOUBLE PRECISION,
    first_order_date        VARCHAR(10),
    last_order_date         VARCHAR(10),
    total_items_purchased   BIGINT,
    customer_id             VARCHAR(50),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(5),
    customer_tier           VARCHAR(20),
    is_repeat_buyer         BOOLEAN
);

CREATE TABLE staging.seller_performance (
    seller_id               VARCHAR(50),
    purchase_year           INTEGER,
    purchase_month          INTEGER,
    total_orders            BIGINT,
    total_item_sold        BIGINT,
    total_revenue           DOUBLE PRECISION,
    avg_item_price          DOUBLE PRECISION,
    avg_delivery_delay_days DOUBLE PRECISION,
    on_time_deliveries      BIGINT,
    total_deliveries        BIGINT,
    on_time_delivery_rate   DOUBLE PRECISION,
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5),
    processed_at            TIMESTAMP
);

-- ─────────────────────────────────────────────
-- WAREHOUSE TABLES (production)
-- Identical structure to staging
-- WHY duplicate? Staging is throwaway — truncated every run.
-- Warehouse is permanent — only swapped after validation.
-- ─────────────────────────────────────────────

CREATE TABLE warehouse.dim_date             (LIKE staging.dim_date);
CREATE TABLE warehouse.dim_customers        (LIKE staging.dim_customers);
CREATE TABLE warehouse.dim_products         (LIKE staging.dim_products);
CREATE TABLE warehouse.dim_sellers          (LIKE staging.dim_sellers);
CREATE TABLE warehouse.fact_orders          (LIKE staging.fact_orders);
CREATE TABLE warehouse.revenue_by_category  (LIKE staging.revenue_by_category);
CREATE TABLE warehouse.customer_lifetime_value (LIKE staging.customer_lifetime_value);
CREATE TABLE warehouse.seller_performance   (LIKE staging.seller_performance);

-- ─────────────────────────────────────────────
-- INDEXES on warehouse tables
-- WHY indexes on Postgres but not Redshift?
-- Redshift uses SORTKEY instead of indexes.
-- Postgres needs explicit indexes for fast lookups.
-- ─────────────────────────────────────────────

CREATE INDEX idx_fact_orders_date_key   ON warehouse.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_customer   ON warehouse.fact_orders(customer_unique_id);
CREATE INDEX idx_fact_orders_product    ON warehouse.fact_orders(product_id);
CREATE INDEX idx_fact_orders_year_month ON warehouse.fact_orders(purchase_year, purchase_month);
CREATE INDEX idx_dim_date_key           ON warehouse.dim_date(date_key);