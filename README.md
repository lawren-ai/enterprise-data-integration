# 🏢 Enterprise Data Integration Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-316192.svg)](https://www.postgresql.org/)
[![Data Quality](https://img.shields.io/badge/Data%20Quality-95.6%25-brightgreen.svg)](reports/)

> A production-ready enterprise data warehouse showcasing end-to-end ETL pipeline development, dimensional modeling (Kimball), and comprehensive data quality management.

## 📖 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Database Schema](#-database-schema)
- [Data Quality](#-data-quality)
- [Documentation](#-documentation)

---

## 🎯 Overview

This project demonstrates **professional data engineering practices** through a complete data warehouse implementation. Built with PostgreSQL and Python, it processes data from multiple source systems into a star schema optimized for business intelligence and analytics.

### Business Context

The platform integrates data from three operational systems:

| Source System | Data Type | Volume |
|--------------|-----------|--------|
| 🏪 **CRM System** | Customer master data | 50,000 customers |
| 🛒 **E-commerce Platform** | Transaction & product data | 200,000+ transactions |
| 📧 **Marketing System** | Campaign performance | 25 campaigns, 14,000+ responses |

### What Makes This Project Stand Out

- ✅ **Production-Grade Code**: Error handling, logging (loguru), configuration management
- ✅ **Advanced SCD Implementation**: Type 1 (products) and Type 2 (customers with full history)
- ✅ **Data Quality Engine**: 15 validation rules across 7 quality dimensions (95.6% score)
- ✅ **Interactive Dashboards**: HTML reports with embedded charts using matplotlib
- ✅ **Scalable Design**: Modular architecture with staging → transformation → warehouse layers

**Project Status:** ✅ Production-Ready | 📊 95.6% Quality Score | 📁 685K+ Records Processed

---

## ✨ Key Features

### 🔄 ETL Pipeline
- **Multi-stage architecture**: Staging → Transformation → Warehouse layers
- **Batch processing**: Handles 200K+ transactions efficiently
- **Change detection**: MD5 row hashing for data integrity
- **Audit trails**: Complete lineage tracking in `stg_audit_log`

### 📊 Dimensional Model
- **Star schema**: 4 dimensions + 2 facts + 1 aggregate
- **SCD Type 1**: Products/campaigns (overwrite changes)
- **SCD Type 2**: Customers with `valid_from/valid_to` for historical accuracy
- **Date dimension**: Pre-populated 2020-2030 with calendar attributes

### 🎯 Data Quality
- **7 Quality Dimensions**: Completeness, Accuracy, Consistency, Validity, Uniqueness, Timeliness, Integrity
- **15 Validation Rules**: Automated checks with configurable thresholds
- **Visual Dashboards**: HTML reports with scorecards, trends, and exception tracking

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA FLOW PIPELINE                       │
└─────────────────────────────────────────────────────────────┘

1️⃣  DATA GENERATION (src/data_generation/)
    │
    ├─> Synthetic Data Generator (Faker)
    │   • 50,000 Customers
    │   • 500 Products
    │   • 200,000 Transactions
    │   • 25 Marketing Campaigns
    │
    └─> Output: data/raw/*.csv

2️⃣  STAGING LAYER (sql/ddl/01_staging_schema.sql)
    │
    ├─> Staging Loader (src/ingestion/load_staging.py)
    │   • CSV → PostgreSQL
    │   • Audit columns added
    │   • MD5 row hashing
    │
    └─> Tables: stg_* (8 staging tables)

3️⃣  TRANSFORMATION LAYER (src/transformation/)
    │
    ├─> Dimension Transformation
    │   • SCD Type 1 (Products, Campaigns)
    │   • SCD Type 2 (Customers)
    │   • Business logic applied
    │
    ├─> Fact Transformation
    │   • Dimension key lookups
    │   • Measure calculations
    │   • Aggregations
    │
    └─> Tables: dim_*, fact_*, agg_*

4️⃣  QUALITY LAYER (src/quality/)
    │
    ├─> Validation Engine
    │   • 15+ validation rules
    │   • Exception tracking
    │   • Scorecard generation
    │
    └─> Tables: dq_* (5 quality tables)

5️⃣  REPORTING (src/quality/quality_reports.py)
    │
    └─> HTML Dashboards & Excel Docs
        • Quality scorecards
        • Trend analysis
        • Data mapping docs
```

---

## 🛠️ Technology Stack

### Core Technologies
- **Database**: PostgreSQL 12+ (dimensional modeling, indexes, constraints)
- **Programming Language**: Python 3.11+ (pandas, SQLAlchemy, Faker)
- **Data Visualization**: Matplotlib, Seaborn (embedded charts in HTML)

### Key Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| `pandas` | 2.1.4 | Data manipulation and DataFrame operations |
| `SQLAlchemy` | 2.0.23 | Database ORM with 2.0 syntax |
| `psycopg2-binary` | 2.9.9 | PostgreSQL driver |
| `Faker` | 22.0.0 | Synthetic data generation |
| `loguru` | 0.7.2 | Advanced logging with rotation |
| `matplotlib` | 3.8.2 | Chart generation |
| `seaborn` | 0.13.0 | Statistical visualizations |
| `openpyxl` | 3.1.2 | Excel file generation |
| `PyYAML` | 6.0.1 | Configuration management |

### Infrastructure
- **Version Control**: Git
- **Configuration**: YAML + INI files
- **Logging**: Loguru with daily rotation and compression
- **Environment**: Python virtual environment (.venv)

---

## 🚀 Quick Start

### Prerequisites

1. **PostgreSQL 12+** installed and running
   ```bash
   # Check PostgreSQL version
   psql --version
   ```

2. **Python 3.11+** installed
   ```bash
   # Check Python version
   python --version
   ```

3. **Git** for cloning the repository

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/lawren-ai/enterprise-data-integration.git
   cd enterprise-data-integration
   ```

2. **Create Python virtual environment**
   ```bash
   # Windows
   python -m venv .venv
   .venv\Scripts\activate

   # Linux/Mac
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database connection**
   ```bash
   # Copy template and edit with your credentials
   cp config/database.ini.template config/database.ini
   ```

   Edit `config/database.ini`:
   ```ini
   [postgresql]
   host = localhost
   port = 5432
   database = enterprise_dw
   user = your_username
   password = your_password
   ```

5. **Create database and schema**
   ```bash
   # Create the database
   python setup_database.py
   ```

### Running the Pipeline

```bash
# Step 1: Generate synthetic data
python src/data_generation/generate_data.py

# Step 2: Load to staging tables
python src/ingestion/load_staging.py --table all

# Step 3: Run ETL transformation
python src/transformation/run_etl.py

# Step 4: Execute data quality checks
python src/quality/quality_engine.py

# Step 5: Generate quality reports
python src/quality/quality_reports.py

# Step 6: View dashboard
start reports\quality_dashboard_*.html
```

**Expected Results:**
- ✅ 50,500 customers loaded
- ✅ 500 products loaded
- ✅ 200,000 transactions loaded  
- ✅ 419,907 transaction items loaded
- ✅ Quality score: 95.6% (12 passed, 1 warning, 2 failed)

---

## 📁 Project Structure

```
enterprise-data-integration/
│
├── config/
│   ├── config.yaml              # Application settings
│   └── database.ini             # Database credentials (gitignored)
│
├── data/
│   ├── raw/                     # Source CSV files from data generation
│   └── processed/               # Processed output files
│
├── docs/
│   └── business_glossary/       # Business terms & KPI definitions
│
├── logs/                        # Daily application logs
│
├── reports/                     # Generated HTML dashboards
│
├── sql/
│   ├── ddl/                     # Table creation scripts
│   │   ├── 01_staging_schema.sql
│   │   ├── 02_warehouse_schema.sql
│   │   └── 04_quality_schema.sql
│   └── queries/                 # Sample analytical queries
│
├── src/
│   ├── data_generation/         # Synthetic data generator (Faker)
│   ├── ingestion/               # CSV to staging loader
│   ├── transformation/          # ETL pipeline (dimensions & facts)
│   ├── quality/                 # Validation engine & reports
│   └── utils/                   # Config, DB manager, logger
│
├── requirements.txt
├── setup_database.py
└── README.md
```

---

## 🗄️ Database Schema

### Staging Layer (6 Tables)

```
stg_crm_customers              (50,500 rows) 
stg_ecom_transactions          (200,000 rows)
stg_ecom_transaction_items     (419,907 rows)
stg_products                   (500 rows)
stg_marketing_campaigns        (25 rows)
stg_campaign_responses         (14,247 rows)
+ stg_audit_log                (tracking)
```

### Warehouse Layer (7 Tables)

#### Dimensions

**dim_customer** (SCD Type 2)
- Surrogate key: `customer_key` (auto-increment)
- Business key: `customer_id`
- SCD fields: `valid_from`, `valid_to`, `is_current`
- Metrics: `total_orders`, `total_spent` (calculated from facts)

**dim_product** (SCD Type 1)
- Surrogate key: `product_key`
- Business key: `product_id`
- Calculated: `margin_percentage = (price - cost) / price * 100`

**dim_date** (Pre-populated 2020-2030)
- Primary key: `date_key` (format: 20230115)
- Attributes: `day_name`, `month_name`, `quarter`, `is_weekend`, etc.

**dim_campaign** (SCD Type 1)
- Channels: Email, Social Media, Display Ads, Search Ads
- Calculated: `campaign_duration_days`

#### Facts

**fact_transactions** (Transaction line item grain)
- Foreign keys: `customer_key`, `product_key`, `transaction_date_key`
- Measures: `quantity`, `unit_price`, `line_total`, `discount_amount`, `net_amount`
- Degenerate dimensions: `transaction_id`, `order_number`, `payment_method`

**fact_campaign_responses** (Customer response grain)
- Foreign keys: `campaign_key`, `customer_key`, `response_date_key`
- Response types: Opened, Clicked, Converted
- Measure: `conversion_value`

#### Aggregates

**agg_customer_monthly** (Performance optimization)
- Monthly rollup: `customer_key`, `year_month`
- Pre-calculated: `total_transactions`, `total_revenue`, `avg_transaction_value`

### Quality Layer (5 Tables)

```
dq_rule_categories    - 7 quality dimensions
dq_rules              - 15 validation rules
dq_test_results       - Execution history
dq_exceptions         - Violation details (max 1000 per rule)
dq_scorecards         - Daily quality scores
```

---

## 🎯 Data Quality Framework

### 7 Quality Dimensions

```
Completeness    → Not null checks, required fields
Accuracy        → Data format validation, range checks
Consistency     → Cross-field validation, referential integrity
Validity        → Business rule compliance
Uniqueness      → Duplicate detection
Timeliness      → Data freshness checks
Integrity       → Foreign key validation
```

### Current Quality Score: **95.6%** ✅

**Latest Results** (15 Rules Executed):
- ✅ Passed: 12 rules
- ⚠️ Warning: 1 rule (Customer Email 3.07% missing)
- ❌ Failed: 2 rules (Transaction accuracy 20%, Phone 26% missing)

### Quality Reports

**HTML Dashboard** (`reports/quality_dashboard_[timestamp].html`):
- Quality scorecard with pass/warn/fail breakdown
- Trend analysis with matplotlib charts
- Test results table with drill-down
- Exception details (top 1000 violations)

**Executive Summary** (`reports/executive_summary_[date].txt`):
- Daily quality metrics
- Failed/warning rules summary
- Recommended actions

---

## 📚 Documentation

- **[BusinessTerms.md](docs/business_glossary/BusinessTerms.md)** - Business definitions
- **[KPIDefinitions.md](docs/business_glossary/KPIDefinitions.md)** - KPI formulas with SQL

---

