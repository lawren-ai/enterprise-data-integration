# Key Performance Indicators (KPI) Definitions

## Enterprise Data Integration Platform
**Last Updated:** January 30, 2026  
**Version:** 1.0

---

## Purpose

This document defines all Key Performance Indicators (KPIs) tracked in the Enterprise Data Warehouse, including business definitions, targets, and measurement guidelines.

---

## Table of Contents

1. [Customer Metrics](#customer-metrics)
2. [Sales & Revenue Metrics](#sales--revenue-metrics)
3. [Product Metrics](#product-metrics)
4. [Marketing Metrics](#marketing-metrics)
5. [Operational Metrics](#operational-metrics)
6. [Data Quality Metrics](#data-quality-metrics)

---

## Customer Metrics

### 1. Total Active Customers

**Definition:** Count of customers with at least one transaction in the last 12 months.

**Business Rules:**
- Only includes customers with purchases in last 12 months
- Uses current customer record
- Excludes test/internal accounts

**Target:** Maintain or grow month-over-month

**Frequency:** Daily refresh, reported monthly

**Owner:** Customer Success Team

---

### 2. Customer Lifetime Value (CLV)

**Definition:** Average total revenue generated per customer since registration.

**Business Rules:**
- Includes all historical transactions
- Excludes customers with zero orders
- Updated daily after ETL

**Target:** 
- Overall: $2,500+
- VIP Segment: $10,000+
- Premium Segment: $5,000+

**Frequency:** Daily refresh, reported monthly

**Owner:** Finance Team

---

### 3. Customer Acquisition Rate

**Definition:** Number of new customers registered per month.

**Business Rules:**
- Counts first registration only
- Month based on registration date
- Excludes duplicates

**Target:** 
- Baseline: 500+ per month
- Growth: 10% month-over-month increase

**Frequency:** Daily refresh, reported monthly

**Owner:** Marketing Team

---

### 4. Customer Churn Rate

**Definition:** Percentage of customers who haven't made a purchase in 6+ months.

**Business Rules:**
- 6 months (180 days) threshold
- Only customers with at least one historical purchase
- Calculated as percentage of total customer base

**Target:** < 20% churn rate

**Frequency:** Weekly refresh, reported monthly

**Owner:** Customer Success Team

---

### 5. Repeat Purchase Rate

**Definition:** Percentage of customers who made more than one purchase.

**Business Rules:**
- Requires minimum of 2 purchases
- Only active customers included
- Based on total order count

**Target:** > 40% repeat rate

**Frequency:** Daily refresh, reported monthly

**Owner:** Sales Team

---

## Sales & Revenue Metrics

### 6. Total Revenue

**Definition:** Sum of all net transaction amounts in a period.

**Business Rules:**
- Uses net amount (after discounts)
- Excludes refunds/returns
- Reported in USD

**Target:** Varies by season, typically $1M+ monthly

**Frequency:** Real-time, reported daily

**Owner:** Finance Team

---

### 7. Average Order Value (AOV)

**Definition:** Average revenue per transaction.

**Calculation:** Total Revenue / Number of Transactions

**Business Rules:**
- Calculated at transaction level (all line items combined)
- Excludes cancelled orders
- Excludes outliers (>$10,000)

**Target:** $250+ per transaction

**Frequency:** Daily refresh, reported weekly

**Owner:** Sales Team

---

### 8. Month-over-Month Revenue Growth

**Definition:** Percentage change in revenue compared to previous month.

**Business Rules:**
- Compares current month to previous month
- Adjusts for seasonality where applicable
- Excludes one-time events

**Target:** 5-10% monthly growth

**Frequency:** Monthly

**Owner:** Finance Team

---

### 9. Year-over-Year Revenue Growth

**Definition:** Percentage change in revenue compared to same period last year.

**Business Rules:**
- Compares same calendar periods
- Adjusts for business days in month
- More accurate than MoM for trend analysis

**Target:** 15-20% annual growth

**Frequency:** Monthly reporting

**Owner:** Finance Team

---

### 10. Revenue by Customer Segment

**Definition:** Revenue distribution across customer segments (VIP, Premium, Standard, Basic).

**Business Rules:**
- Segments based on current classification
- Includes all transaction history
- Used for targeting strategies

**Target:** 
- VIP: 40% of total revenue
- Premium: 30% of total revenue
- Standard: 20% of total revenue
- Basic: 10% of total revenue

**Frequency:** Monthly

**Owner:** Sales Team

---

## Product Metrics

### 11. Units Sold

**Definition:** Total number of product units sold in a period.

**Business Rules:**
- Counts line item quantity
- Excludes returns
- All active products included

**Target:** Varies by category, typically 10,000+ monthly

**Frequency:** Daily refresh, reported weekly

**Owner:** Product Management

---

### 12. Top 10 Products by Revenue

**Definition:** Products generating highest revenue in reporting period.

**Business Rules:**
- Ranked by total net amount
- Updated monthly
- Used for inventory and promotion planning

**Target:** Top 10 represent 30%+ of total revenue

**Frequency:** Monthly

**Owner:** Product Management

---

### 13. Product Category Performance

**Definition:** Revenue and unit sales by product category.

**Business Rules:**
- Five main categories tracked
- Includes margin analysis
- Drives merchandising strategy

**Target:** All categories positive growth YoY

**Frequency:** Monthly

**Owner:** Product Management

---

### 14. Average Product Margin

**Definition:** Average profit margin across all products sold.

**Calculation:** ((Price - Cost) / Price) * 100

**Business Rules:**
- Weighted by sales volume
- Varies significantly by category
- Target margins set by finance

**Target:** 35-45% overall margin

**Frequency:** Weekly

**Owner:** Finance Team

---

### 15. Inventory Turnover

**Definition:** Rate at which inventory is sold and replaced.

**Business Rules:**
- Higher turnover indicates efficient inventory management
- Varies by product category
- Seasonal adjustments applied

**Target:** 6-8 times per year (varies by category)

**Frequency:** Monthly

**Owner:** Operations Team

---

## Marketing Metrics

### 16. Campaign ROI

**Definition:** Return on investment for marketing campaigns.

**Calculation:** ((Revenue - Cost) / Cost) * 100

**Business Rules:**
- Revenue from converted customers only
- Includes all campaign costs
- Positive ROI required for ongoing campaigns

**Target:** 
- Email: 300%+ ROI
- Social Media: 200%+ ROI
- Display Ads: 150%+ ROI
- Search Ads: 250%+ ROI

**Frequency:** After campaign completion

**Owner:** Marketing Team

---

### 17. Conversion Rate by Channel

**Definition:** Percentage of campaign recipients who make a purchase, by marketing channel.

**Business Rules:**
- Tracked separately for each channel
- 30-day attribution window
- First-touch attribution model

**Target:** 
- Email: 2-5%
- Social Media: 1-3%
- Display Ads: 0.5-1%
- Search Ads: 5-10%

**Frequency:** Weekly

**Owner:** Marketing Team

---

### 18. Cost Per Acquisition (CPA)

**Definition:** Average cost to acquire one customer through marketing efforts.

**Business Rules:**
- Campaign budget divided by conversions
- Lower CPA preferred
- Compared against customer lifetime value

**Target:** CPA < 20% of CLV

**Frequency:** After campaign completion

**Owner:** Marketing Team

---

### 19. Email Open Rate

**Definition:** Percentage of email recipients who open the message.

**Business Rules:**
- Tracked for email campaigns only
- Industry benchmark: 15-25%
- Varies by segment and content

**Target:** > 20% open rate

**Frequency:** Per campaign

**Owner:** Marketing Team

---

### 20. Email Click-Through Rate

**Definition:** Percentage of email recipients who click a link in the message.

**Business Rules:**
- Subset of opens
- Industry benchmark: 2-5%
- Strong indicator of engagement

**Target:** > 3% click-through rate

**Frequency:** Per campaign

**Owner:** Marketing Team

---

## Operational Metrics

### 21. ETL Success Rate

**Definition:** Percentage of successful ETL pipeline executions.

**Business Rules:**
- Tracks daily ETL runs
- Failure defined as incomplete or error-terminated run
- Alerts triggered on failure

**Target:** 99%+ success rate

**Frequency:** Daily monitoring

**Owner:** Data Engineering Team

---

### 22. Data Freshness

**Definition:** Time lag between source data creation and availability in warehouse.

**Business Rules:**
- Measured in hours
- Critical for real-time decision making
- Varies by data source

**Target:** < 24 hours for all sources

**Frequency:** Daily monitoring

**Owner:** Data Engineering Team

---

### 23. Query Performance

**Definition:** Average response time for analytical queries.

**Business Rules:**
- Measured in seconds
- Tracks top 20 most-used queries
- Optimization needed if > 30 seconds

**Target:** < 10 seconds for standard queries

**Frequency:** Weekly review

**Owner:** Data Engineering Team

---

### 24. User Adoption Rate

**Definition:** Percentage of target users actively using the data warehouse.

**Business Rules:**
- Active = ran at least one query in last 30 days
- Tracked by department
- Training provided for low adoption

**Target:** > 70% adoption across all teams

**Frequency:** Monthly

**Owner:** Analytics Team

---

## Data Quality Metrics

### 25. Overall Data Quality Score

**Definition:** Composite score measuring data fitness for use.

**Calculation:** Average of 7 quality dimensions (Completeness, Accuracy, Consistency, Validity, Uniqueness, Timeliness, Integrity)

**Thresholds:**
- **Excellent:** 95-100%
- **Good:** 80-94%
- **Fair:** 60-79%
- **Poor:** <60%

**Target:** > 95% overall quality score

**Frequency:** Daily after ETL

**Owner:** Data Engineering Team

---

### 26. Data Completeness

**Definition:** Percentage of required fields that are populated.

**Business Rules:**
- Critical fields must have >95% completeness
- Warning threshold: 80-95%
- Failure threshold: <80%

**Target:** > 95% for critical fields

**Frequency:** Daily

**Owner:** Data Engineering Team

---

### 27. Data Accuracy

**Definition:** Degree to which data correctly represents real-world values.

**Validation Methods:**
- Range checks (age, dates, amounts)
- Format validation (email, phone)
- Business rule compliance

**Target:** > 95% passing validation rules

**Frequency:** Daily

**Owner:** Data Engineering Team

---

### 28. Referential Integrity

**Definition:** Percentage of foreign key relationships that are valid.

**Business Rules:**
- All fact records must link to valid dimensions
- Orphaned records require investigation
- Critical for accurate reporting

**Target:** 100% referential integrity

**Frequency:** Daily

**Owner:** Data Engineering Team

---

### 29. Duplicate Records

**Definition:** Percentage of records that are exact duplicates.

**Business Rules:**
- Identified by matching natural keys
- Should be near zero
- Requires cleanup if detected

**Target:** < 0.1% duplicates

**Frequency:** Weekly

**Owner:** Data Engineering Team

---

### 30. Data Timeliness

**Definition:** Percentage of records loaded within expected timeframe.

**Business Rules:**
- Expected timeframe varies by source
- Late data triggers alerts
- Impacts decision making

**Target:** > 95% on-time delivery

**Frequency:** Daily

**Owner:** Data Engineering Team

---

## KPI Summary Dashboard

### Executive KPIs (Reviewed Weekly)
1. Total Revenue
2. Customer Lifetime Value
3. Overall Data Quality Score
4. Active Customers
5. Marketing Campaign ROI

### Operational KPIs (Monitored Daily)
1. ETL Success Rate
2. Data Freshness
3. Data Completeness
4. Referential Integrity
5. Query Performance

### Strategic KPIs (Reviewed Monthly)
1. Year-over-Year Revenue Growth
2. Customer Churn Rate
3. Product Category Performance
4. User Adoption Rate
5. Average Product Margin

---

## Notes

- All monetary values in USD
- Thresholds reviewed quarterly and adjusted based on business performance
- Custom KPIs can be defined for specific projects or initiatives
- Historical KPI values archived for trend analysis

---

*This document is reviewed and updated quarterly to ensure KPI definitions remain aligned with business objectives.*
