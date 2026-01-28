-- =====================================================
-- ANALYTICAL QUERIES FOR ENTERPRISE DATA WAREHOUSE
-- Sample queries demonstrating business insights
-- =====================================================

-- =====================================================
-- 1. CUSTOMER ANALYTICS
-- =====================================================

-- Top 20 Customers by Lifetime Value
SELECT 
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.city,
    c.state,
    c.total_orders,
    c.total_spent as lifetime_value,
    ROUND(c.total_spent / NULLIF(c.total_orders, 0), 2) as avg_order_value,
    c.registration_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.registration_date)) as customer_tenure_years
FROM dim_customer c
WHERE c.is_current = TRUE
  AND c.total_orders > 0
ORDER BY c.total_spent DESC
LIMIT 20;

-- Customer Segmentation by Value Tiers
SELECT 
    CASE 
        WHEN lifetime_value >= 10000 THEN 'VIP'
        WHEN lifetime_value >= 5000 THEN 'High Value'
        WHEN lifetime_value >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_tier,
    COUNT(*) as customer_count,
    ROUND(AVG(lifetime_value), 2) as avg_lifetime_value,
    ROUND(SUM(lifetime_value), 2) as total_revenue,
    ROUND(AVG(total_orders), 1) as avg_orders_per_customer
FROM dim_customer
WHERE is_current = TRUE
GROUP BY value_tier
ORDER BY avg_lifetime_value DESC;

-- Customer Churn Analysis (No purchases in last 6 months)
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) as total_customers,
    COUNT(DISTINCT CASE 
        WHEN last_purchase.days_since_purchase > 180 
        THEN c.customer_key 
    END) as at_risk_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN last_purchase.days_since_purchase > 180 THEN c.customer_key END) * 100.0 / 
        NULLIF(COUNT(DISTINCT c.customer_key), 0), 
        2
    ) as churn_risk_percentage
FROM dim_customer c
LEFT JOIN (
    SELECT 
        customer_key,
        MAX(d.date_actual) as last_purchase_date,
        CURRENT_DATE - MAX(d.date_actual) as days_since_purchase
    FROM fact_transactions f
    JOIN dim_date d ON f.transaction_date_key = d.date_key
    GROUP BY customer_key
) last_purchase ON c.customer_key = last_purchase.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_segment
ORDER BY churn_risk_percentage DESC;

-- =====================================================
-- 2. SALES ANALYTICS
-- =====================================================

-- Monthly Revenue Trend (Last 12 Months)
SELECT 
    d.year_number,
    d.month_name,
    d.month_number,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    SUM(f.quantity) as total_units_sold,
    ROUND(SUM(f.net_amount), 2) as total_revenue,
    ROUND(SUM(f.discount_amount), 2) as total_discounts,
    ROUND(AVG(f.net_amount), 2) as avg_transaction_value
FROM fact_transactions f
JOIN dim_date d ON f.transaction_date_key = d.date_key
WHERE d.date_actual >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY d.year_number, d.month_name, d.month_number
ORDER BY d.year_number, d.month_number;

-- Revenue by Day of Week
SELECT 
    d.day_name,
    d.day_of_week,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    ROUND(SUM(f.net_amount), 2) as total_revenue,
    ROUND(AVG(f.net_amount), 2) as avg_transaction_value,
    ROUND(SUM(f.net_amount) * 100.0 / SUM(SUM(f.net_amount)) OVER (), 2) as revenue_percentage
FROM fact_transactions f
JOIN dim_date d ON f.transaction_date_key = d.date_key
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;

-- Year-over-Year Growth Analysis
WITH monthly_revenue AS (
    SELECT 
        d.year_number,
        d.month_number,
        SUM(f.net_amount) as revenue
    FROM fact_transactions f
    JOIN dim_date d ON f.transaction_date_key = d.date_key
    GROUP BY d.year_number, d.month_number
)
SELECT 
    curr.year_number as current_year,
    curr.month_number,
    ROUND(curr.revenue, 2) as current_year_revenue,
    ROUND(prev.revenue, 2) as previous_year_revenue,
    ROUND(curr.revenue - prev.revenue, 2) as revenue_change,
    ROUND((curr.revenue - prev.revenue) * 100.0 / NULLIF(prev.revenue, 0), 2) as yoy_growth_percentage
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.month_number = prev.month_number 
    AND curr.year_number = prev.year_number + 1
WHERE prev.revenue IS NOT NULL
ORDER BY curr.year_number, curr.month_number;

-- =====================================================
-- 3. PRODUCT ANALYTICS
-- =====================================================

-- Top 20 Best-Selling Products
SELECT 
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_subcategory,
    p.brand,
    SUM(f.quantity) as units_sold,
    COUNT(DISTINCT f.transaction_id) as times_purchased,
    ROUND(SUM(f.net_amount), 2) as total_revenue,
    ROUND(AVG(f.unit_price), 2) as avg_selling_price,
    p.retail_price as list_price,
    ROUND(p.margin_percentage, 2) as margin_pct
FROM fact_transactions f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_name, p.product_category, 
         p.product_subcategory, p.brand, p.retail_price, p.margin_percentage
ORDER BY total_revenue DESC
LIMIT 20;

-- Product Category Performance
SELECT 
    p.product_category,
    COUNT(DISTINCT p.product_key) as product_count,
    SUM(f.quantity) as total_units_sold,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    ROUND(SUM(f.net_amount), 2) as total_revenue,
    ROUND(AVG(f.net_amount), 2) as avg_transaction_value,
    ROUND(SUM(f.net_amount - (f.quantity * p.unit_cost)), 2) as total_profit,
    ROUND(
        (SUM(f.net_amount - (f.quantity * p.unit_cost)) / NULLIF(SUM(f.net_amount), 0)) * 100, 
        2
    ) as profit_margin_pct
FROM fact_transactions f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category
ORDER BY total_revenue DESC;

-- Product Affinity Analysis (Products bought together)
SELECT 
    p1.product_name as product_1,
    p2.product_name as product_2,
    COUNT(DISTINCT f1.transaction_id) as times_bought_together,
    ROUND(AVG(f1.net_amount + f2.net_amount), 2) as avg_combined_value
FROM fact_transactions f1
JOIN fact_transactions f2 
    ON f1.transaction_id = f2.transaction_id 
    AND f1.product_key < f2.product_key
JOIN dim_product p1 ON f1.product_key = p1.product_key
JOIN dim_product p2 ON f2.product_key = p2.product_key
GROUP BY p1.product_name, p2.product_name
HAVING COUNT(DISTINCT f1.transaction_id) >= 5
ORDER BY times_bought_together DESC
LIMIT 20;

-- =====================================================
-- 4. MARKETING CAMPAIGN ANALYTICS
-- =====================================================

-- Campaign ROI Analysis
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.channel,
    TO_CHAR(c.start_date, 'YYYY-MM-DD') as start_date,
    TO_CHAR(c.end_date, 'YYYY-MM-DD') as end_date,
    c.duration_days,
    ROUND(c.budget, 2) as budget,
    COUNT(DISTINCT r.customer_key) as total_responses,
    SUM(CASE WHEN r.is_converted THEN 1 ELSE 0 END) as conversions,
    ROUND(
        SUM(CASE WHEN r.is_converted THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(DISTINCT r.customer_key), 0), 
        2
    ) as conversion_rate,
    ROUND(SUM(r.conversion_value), 2) as total_conversion_value,
    ROUND(SUM(r.conversion_value) - c.budget, 2) as roi_amount,
    ROUND(
        (SUM(r.conversion_value) - c.budget) * 100.0 / NULLIF(c.budget, 0), 
        2
    ) as roi_percentage
FROM dim_campaign c
LEFT JOIN fact_campaign_responses r ON c.campaign_key = r.campaign_key
WHERE c.campaign_status = 'Completed'
GROUP BY c.campaign_id, c.campaign_name, c.campaign_type, c.channel,
         c.start_date, c.end_date, c.duration_days, c.budget
ORDER BY roi_percentage DESC;

-- Campaign Response Funnel
SELECT 
    c.campaign_name,
    c.channel,
    COUNT(DISTINCT r.customer_key) as total_reached,
    SUM(CASE WHEN r.is_opened THEN 1 ELSE 0 END) as opened,
    SUM(CASE WHEN r.is_clicked THEN 1 ELSE 0 END) as clicked,
    SUM(CASE WHEN r.is_converted THEN 1 ELSE 0 END) as converted,
    ROUND(
        SUM(CASE WHEN r.is_opened THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(DISTINCT r.customer_key), 0), 
        2
    ) as open_rate,
    ROUND(
        SUM(CASE WHEN r.is_clicked THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN r.is_opened THEN 1 ELSE 0 END), 0), 
        2
    ) as click_through_rate,
    ROUND(
        SUM(CASE WHEN r.is_converted THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN r.is_clicked THEN 1 ELSE 0 END), 0), 
        2
    ) as conversion_rate
FROM dim_campaign c
LEFT JOIN fact_campaign_responses r ON c.campaign_key = r.campaign_key
WHERE c.campaign_status = 'Completed'
GROUP BY c.campaign_name, c.channel
ORDER BY total_reached DESC;

-- =====================================================
-- 5. COHORT ANALYSIS
-- =====================================================

-- Customer Cohort Revenue (by Registration Month)
SELECT 
    TO_CHAR(c.registration_date, 'YYYY-MM') as cohort_month,
    COUNT(DISTINCT c.customer_key) as cohort_size,
    ROUND(SUM(f.net_amount), 2) as total_revenue,
    ROUND(AVG(c.total_spent), 2) as avg_customer_value,
    ROUND(AVG(c.total_orders), 1) as avg_orders_per_customer
FROM dim_customer c
LEFT JOIN fact_transactions f ON c.customer_key = f.customer_key
WHERE c.is_current = TRUE
GROUP BY cohort_month
ORDER BY cohort_month DESC;

-- =====================================================
-- 6. CUSTOMER LIFETIME VALUE (CLV) PREDICTIONS
-- =====================================================

-- CLV Segments with Purchase Behavior
SELECT 
    CASE 
        WHEN c.total_spent >= 5000 THEN 'High CLV'
        WHEN c.total_spent >= 1000 THEN 'Medium CLV'
        ELSE 'Low CLV'
    END as clv_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(c.total_spent), 2) as avg_lifetime_value,
    ROUND(AVG(c.total_orders), 1) as avg_orders,
    ROUND(AVG(c.total_spent / NULLIF(c.total_orders, 0)), 2) as avg_order_value,
    ROUND(
        AVG(
            EXTRACT(DAY FROM CURRENT_DATE - c.registration_date) / 
            NULLIF(c.total_orders, 0)
        ), 
        1
    ) as avg_days_between_orders
FROM dim_customer c
WHERE c.is_current = TRUE
  AND c.total_orders > 0
GROUP BY clv_segment
ORDER BY avg_lifetime_value DESC;

-- =====================================================
-- 7. GEOGRAPHIC ANALYSIS
-- =====================================================

-- Revenue by State/Region
SELECT 
    c.state,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.net_amount) as total_revenue,
    ROUND(AVG(c.total_spent), 2) as avg_customer_value,
    ROUND(SUM(f.net_amount) * 100.0 / SUM(SUM(f.net_amount)) OVER (), 2) as revenue_percentage
FROM dim_customer c
JOIN fact_transactions f ON c.customer_key = f.customer_key
WHERE c.is_current = TRUE
GROUP BY c.state
ORDER BY total_revenue DESC
LIMIT 20;

-- =====================================================
-- 8. DATA QUALITY MONITORING
-- =====================================================

-- Check for Dimension Completeness
SELECT 
    'Customer' as dimension,
    COUNT(*) as total_records,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as missing_email,
    SUM(CASE WHEN phone_number IS NULL THEN 1 ELSE 0 END) as missing_phone,
    ROUND(
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as email_null_pct
FROM dim_customer
WHERE is_current = TRUE

UNION ALL

SELECT 
    'Product' as dimension,
    COUNT(*) as total_records,
    SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) as missing_category,
    SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) as missing_brand,
    ROUND(
        SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as category_null_pct
FROM dim_product;