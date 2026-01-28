-- =====================================================
-- STAGING LAYER SCHEMA
-- Raw data landing zone with minimal transformation
-- Flexible, permissive, run-from-scratch safe
-- =====================================================

-- Drop existing staging tables if they exist
DROP TABLE IF EXISTS stg_crm_customers CASCADE;
DROP TABLE IF EXISTS stg_ecom_transactions CASCADE;
DROP TABLE IF EXISTS stg_ecom_transaction_items CASCADE;
DROP TABLE IF EXISTS stg_products CASCADE;
DROP TABLE IF EXISTS stg_marketing_campaigns CASCADE;
DROP TABLE IF EXISTS stg_campaign_responses CASCADE;
DROP TABLE IF EXISTS stg_audit_log CASCADE;
DROP TABLE IF EXISTS stg_data_profile CASCADE;

-- =====================================================
-- CRM Customer Data (Raw)
-- =====================================================
CREATE TABLE stg_crm_customers (
    -- Source system fields
    customer_id        VARCHAR(50),
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    email              VARCHAR(255),
    phone_number       VARCHAR(50),
    date_of_birth      DATE,
    gender             VARCHAR(20),
    address            VARCHAR(255),
    city               VARCHAR(100),
    state              VARCHAR(50),
    postal_code        VARCHAR(20),
    country            VARCHAR(100),
    registration_date  DATE,
    customer_segment   VARCHAR(50),
    account_status     VARCHAR(50),

    -- Technical audit columns
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash           VARCHAR(64),
    load_id            BIGINT
);

COMMENT ON TABLE stg_crm_customers IS 'Raw customer data from CRM system';

-- =====================================================
-- E-commerce Transaction Data (Raw)
-- =====================================================
CREATE TABLE stg_ecom_transactions (
    -- Source system fields
    transaction_id     VARCHAR(50),
    customer_id        VARCHAR(50),
    transaction_date   TIMESTAMP,
    order_number       VARCHAR(50),
    payment_method     VARCHAR(50),
    payment_status     VARCHAR(50),
    total_amount       DECIMAL(10,2),
    tax_amount         DECIMAL(10,2),
    shipping_amount    DECIMAL(10,2),
    discount_amount    DECIMAL(10,2),
    currency_code      VARCHAR(10),

    -- Technical audit columns
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash           VARCHAR(64),
    load_id            BIGINT
);

COMMENT ON TABLE stg_ecom_transactions IS 'Raw transaction header data from e-commerce platform';

-- =====================================================
-- Transaction Line Items (Raw)
-- =====================================================
CREATE TABLE stg_ecom_transaction_items (
    -- Source system fields
    transaction_item_id VARCHAR(50),
    transaction_id      VARCHAR(50),
    product_id          VARCHAR(50),
    product_name        VARCHAR(255),
    quantity            INTEGER,
    unit_price          DECIMAL(10,2),
    line_total          DECIMAL(10,2),
    discount_amount     DECIMAL(10,2),

    -- Technical audit columns
    source_file         VARCHAR(255),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash            VARCHAR(64),
    load_id             BIGINT
);

COMMENT ON TABLE stg_ecom_transaction_items IS 'Raw transaction line item details';

-- =====================================================
-- Product Catalog (Raw)
-- =====================================================
CREATE TABLE stg_products (
    -- Source system fields
    product_id          VARCHAR(50),
    product_name        VARCHAR(255),
    product_category    VARCHAR(100),
    product_subcategory VARCHAR(100),
    brand               VARCHAR(100),
    unit_cost            DECIMAL(10,2),
    retail_price         DECIMAL(10,2),
    product_status       VARCHAR(50),

    -- Technical audit columns
    source_file         VARCHAR(255),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash            VARCHAR(64),
    load_id             BIGINT
);

COMMENT ON TABLE stg_products IS 'Raw product catalog data';

-- =====================================================
-- Marketing Campaign Data (Raw)
-- =====================================================
CREATE TABLE stg_marketing_campaigns (
    -- Source system fields
    campaign_id        VARCHAR(50),
    campaign_name      VARCHAR(255),
    campaign_type      VARCHAR(100),
    channel            VARCHAR(100),
    start_date         DATE,
    end_date           DATE,
    budget             DECIMAL(12,2),
    target_audience    VARCHAR(100),
    campaign_status    VARCHAR(50),

    -- Technical audit columns
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash           VARCHAR(64),
    load_id            BIGINT
);

COMMENT ON TABLE stg_marketing_campaigns IS 'Raw marketing campaign master data';

-- =====================================================
-- Campaign Response Data (Raw)
-- =====================================================
CREATE TABLE stg_campaign_responses (
    -- Source system fields
    response_id        VARCHAR(50),
    campaign_id        VARCHAR(50),
    customer_id        VARCHAR(50),
    response_date      DATE,
    response_type      VARCHAR(50),
    conversion_value   DECIMAL(10,2),

    -- Technical audit columns
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash           VARCHAR(64),
    load_id            BIGINT
);

COMMENT ON TABLE stg_campaign_responses IS 'Raw campaign response/engagement data';

-- =====================================================
-- Audit Log (Tracks all data loads)
-- =====================================================
CREATE TABLE stg_audit_log (
    audit_id        SERIAL PRIMARY KEY,
    load_id         BIGINT,
    table_name      VARCHAR(100),
    load_type       VARCHAR(20),
    source_file     VARCHAR(255),
    rows_loaded     INTEGER,
    rows_rejected   INTEGER,
    load_start_time TIMESTAMP,
    load_end_time   TIMESTAMP,
    load_status     VARCHAR(20),
    error_message   TEXT,
    loaded_by       VARCHAR(100)
);

COMMENT ON TABLE stg_audit_log IS 'Audit trail for all staging loads';

-- =====================================================
-- Data Profile (Quality Metrics)
-- =====================================================
CREATE TABLE stg_data_profile (
    profile_id             SERIAL PRIMARY KEY,
    table_name             VARCHAR(100),
    column_name            VARCHAR(100),
    profile_date           DATE,
    total_rows             INTEGER,
    null_count             INTEGER,
    null_percentage        DECIMAL(5,2),
    distinct_count         INTEGER,
    min_value              VARCHAR(255),
    max_value              VARCHAR(255),
    avg_value              DECIMAL(18,2),
    most_frequent_value    VARCHAR(255),
    data_type              VARCHAR(50)
);

COMMENT ON TABLE stg_data_profile IS 'Data profiling statistics for quality monitoring';

-- =====================================================
-- Indexes for performance
-- =====================================================
CREATE INDEX idx_stg_customers_id
    ON stg_crm_customers(customer_id);

CREATE INDEX idx_stg_transactions_id
    ON stg_ecom_transactions(transaction_id);

CREATE INDEX idx_stg_transactions_customer
    ON stg_ecom_transactions(customer_id);

CREATE INDEX idx_stg_items_transaction
    ON stg_ecom_transaction_items(transaction_id);

CREATE INDEX idx_stg_products_id
    ON stg_products(product_id);

CREATE INDEX idx_stg_campaigns_id
    ON stg_marketing_campaigns(campaign_id);

CREATE INDEX idx_stg_responses_campaign
    ON stg_campaign_responses(campaign_id);

CREATE INDEX idx_stg_responses_customer
    ON stg_campaign_responses(customer_id);
