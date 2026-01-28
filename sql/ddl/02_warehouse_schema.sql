-- =====================================================
-- DATA WAREHOUSE SCHEMA - STAR SCHEMA DESIGN
-- Dimensional model optimized for analytics
-- =====================================================

-- Drop existing warehouse tables if they exist
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS fact_campaign_responses CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_campaign CASCADE;
DROP TABLE IF EXISTS dim_geography CASCADE;
DROP TABLE IF EXISTS agg_customer_monthly CASCADE;
DROP TABLE IF EXISTS agg_product_daily CASCADE;

-- =====================================================
-- DIMENSION: Customer (SCD Type 2)
-- =====================================================
CREATE TABLE dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    
    -- Business key
    customer_id VARCHAR(50) NOT NULL,
    
    -- Customer attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(20),
    date_of_birth DATE,
    age INTEGER,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    
    -- Address (SCD Type 2 tracked)
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    
    -- Customer lifecycle
    registration_date DATE,
    customer_segment VARCHAR(50),
    account_status VARCHAR(20),
    lifetime_value DECIMAL(12,2),
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    
    -- SCD Type 2 metadata
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Technical metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CRM'
);

COMMENT ON TABLE dim_customer IS 'Customer dimension with SCD Type 2 for address changes';

-- Indexes for performance
CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dim_customer(customer_id, is_current);
CREATE INDEX idx_dim_customer_dates ON dim_customer(valid_from, valid_to);

-- =====================================================
-- DIMENSION: Product
-- =====================================================
CREATE TABLE dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    
    -- Business key
    product_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Product attributes
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_subcategory VARCHAR(100),
    brand VARCHAR(100),
    
    -- Pricing
    unit_cost DECIMAL(10,2),
    retail_price DECIMAL(10,2),
    margin_percentage DECIMAL(5,2),
    
    -- Product status
    product_status VARCHAR(20),
    is_active BOOLEAN,
    
    -- Technical metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'E-COMMERCE'
);

COMMENT ON TABLE dim_product IS 'Product dimension with category hierarchy';

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(product_category, product_subcategory);

-- =====================================================
-- DIMENSION: Date (Preloaded calendar dimension)
-- =====================================================
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD format
    
    -- Date components
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    
    -- Week
    week_of_year INTEGER,
    week_start_date DATE,
    week_end_date DATE,
    
    -- Month
    month_number INTEGER,
    month_name VARCHAR(10),
    month_abbr VARCHAR(3),
    month_start_date DATE,
    month_end_date DATE,
    
    -- Quarter
    quarter_number INTEGER,
    quarter_name VARCHAR(2),
    quarter_start_date DATE,
    quarter_end_date DATE,
    
    -- Year
    year_number INTEGER,
    
    -- Fiscal (assuming fiscal year = calendar year for now)
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,
    
    -- Flags
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    
    -- Business day calculations
    is_business_day BOOLEAN
);

COMMENT ON TABLE dim_date IS 'Date dimension with calendar hierarchy';

CREATE INDEX idx_dim_date_actual ON dim_date(date_actual);
CREATE INDEX idx_dim_date_year_month ON dim_date(year_number, month_number);

-- =====================================================
-- DIMENSION: Campaign
-- =====================================================
CREATE TABLE dim_campaign (
    campaign_key BIGSERIAL PRIMARY KEY,
    
    -- Business key
    campaign_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Campaign attributes
    campaign_name VARCHAR(255),
    campaign_type VARCHAR(50),
    channel VARCHAR(50),
    
    -- Campaign timeline
    start_date DATE,
    end_date DATE,
    duration_days INTEGER,
    
    -- Budget
    budget DECIMAL(12,2),
    
    -- Targeting
    target_audience VARCHAR(100),
    
    -- Status
    campaign_status VARCHAR(20),
    
    -- Technical metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'MARKETING'
);

COMMENT ON TABLE dim_campaign IS 'Marketing campaign dimension';

CREATE INDEX idx_dim_campaign_id ON dim_campaign(campaign_id);
CREATE INDEX idx_dim_campaign_dates ON dim_campaign(start_date, end_date);

-- =====================================================
-- FACT TABLE: Transactions (Grain: One row per transaction)
-- =====================================================
CREATE TABLE fact_transactions (
    transaction_key BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    product_key BIGINT NOT NULL REFERENCES dim_product(product_key),
    transaction_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    
    -- Degenerate dimensions (transaction details stored in fact)
    transaction_id VARCHAR(50) NOT NULL,
    order_number VARCHAR(50),
    
    -- Measures (additive facts)
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    
    -- Transaction attributes
    payment_method VARCHAR(50),
    payment_status VARCHAR(20),
    currency_code VARCHAR(3),
    
    -- Technical metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'E-COMMERCE'
);

COMMENT ON TABLE fact_transactions IS 'Transaction fact table - grain: one row per transaction line item';

-- Indexes for query performance
CREATE INDEX idx_fact_trans_customer ON fact_transactions(customer_key);
CREATE INDEX idx_fact_trans_product ON fact_transactions(product_key);
CREATE INDEX idx_fact_trans_date ON fact_transactions(transaction_date_key);
CREATE INDEX idx_fact_trans_id ON fact_transactions(transaction_id);

-- =====================================================
-- FACT TABLE: Campaign Responses (Grain: One row per response)
-- =====================================================
CREATE TABLE fact_campaign_responses (
    response_key BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys to dimensions
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    campaign_key BIGINT NOT NULL REFERENCES dim_campaign(campaign_key),
    response_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    
    -- Degenerate dimension
    response_id VARCHAR(50) NOT NULL,
    
    -- Measures
    response_type VARCHAR(50),  -- opened, clicked, converted
    conversion_value DECIMAL(10,2),
    
    -- Flags (semi-additive facts)
    is_opened BOOLEAN,
    is_clicked BOOLEAN,
    is_converted BOOLEAN,
    
    -- Technical metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'MARKETING'
);

COMMENT ON TABLE fact_campaign_responses IS 'Campaign response fact table - grain: one row per customer response';

CREATE INDEX idx_fact_response_customer ON fact_campaign_responses(customer_key);
CREATE INDEX idx_fact_response_campaign ON fact_campaign_responses(campaign_key);
CREATE INDEX idx_fact_response_date ON fact_campaign_responses(response_date_key);

-- =====================================================
-- AGGREGATE: Customer Monthly Summary (Pre-calculated)
-- =====================================================
CREATE TABLE agg_customer_monthly (
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    year_month INTEGER NOT NULL,  -- YYYYMM format
    
    -- Aggregated measures
    total_transactions INTEGER,
    total_quantity INTEGER,
    total_amount DECIMAL(12,2),
    total_discount DECIMAL(12,2),
    avg_transaction_value DECIMAL(10,2),
    
    -- Calculated date
    month_start_date DATE,
    month_end_date DATE,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (customer_key, year_month)
);

COMMENT ON TABLE agg_customer_monthly IS 'Pre-aggregated customer metrics by month for performance';

-- =====================================================
-- AGGREGATE: Product Daily Sales (Pre-calculated)
-- =====================================================
CREATE TABLE agg_product_daily (
    product_key BIGINT NOT NULL REFERENCES dim_product(product_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    
    -- Aggregated measures
    units_sold INTEGER,
    total_revenue DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    unique_customers INTEGER,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (product_key, date_key)
);

COMMENT ON TABLE agg_product_daily IS 'Pre-aggregated product sales by day for dashboard performance';