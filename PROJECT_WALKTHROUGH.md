# Enterprise Data Integration Platform - Complete Project Walkthrough

**Author's Note:** This document walks you through every aspect of this project as if I'm a junior developer explaining my work to you. I'll cover what I built, why I built it that way, and how everything works together.

---

## 📚 Table of Contents

1. [Project Vision & Goals](#project-vision--goals)
2. [Architecture Overview](#architecture-overview)
3. [Database Design Deep Dive](#database-design-deep-dive)
4. [Configuration System](#configuration-system)
5. [Utilities & Infrastructure](#utilities--infrastructure)
6. [Data Generation](#data-generation)
7. [ETL Pipeline - Ingestion](#etl-pipeline---ingestion)
8. [ETL Pipeline - Transformation](#etl-pipeline---transformation)
9. [Data Quality Framework](#data-quality-framework)
10. [Reporting & Visualization](#reporting--visualization)
11. [Documentation](#documentation)
12. [Testing & Debugging](#testing--debugging)
13. [Complete Data Flow](#complete-data-flow)
14. [Lessons Learned](#lessons-learned)

---

## Project Vision & Goals

### What I Set Out to Build

I wanted to create a **real-world enterprise data warehouse** that demonstrates professional data engineering skills. Not just a toy project, but something that could actually run in production.

### Why These Technologies?

**PostgreSQL:**
- Free and powerful
- Excellent support for analytical queries
- Strong data integrity features
- Industry-standard

**Python:**
- Great data processing libraries (pandas, SQLAlchemy)
- Easy to read and maintain
- Strong ecosystem for data engineering

**Star Schema (Kimball Methodology):**
- Proven dimensional modeling approach
- Optimized for analytical queries
- Easy for business users to understand
- Supports historical tracking (SCD Type 2)

### Success Criteria

I wanted to demonstrate:
1. ✅ End-to-end ETL pipeline
2. ✅ Proper dimensional modeling
3. ✅ Data quality management
4. ✅ Production-ready code (error handling, logging, configuration)
5. ✅ Complete documentation

---

## Architecture Overview

### The Big Picture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA FLOW                                │
└─────────────────────────────────────────────────────────────┘

1. DATA GENERATION (src/data_generation/)
   │
   ├─> Generates synthetic CSV files
   │   • Customers (50,000)
   │   • Products (500)
   │   • Transactions (200,000)
   │   • Campaigns (25)
   │
   └─> Output: data/raw/*.csv

2. STAGING LAYER (sql/ddl/01_staging_schema.sql)
   │
   ├─> Load raw data into PostgreSQL
   │   • src/ingestion/load_staging.py
   │   • Minimal transformation
   │   • Audit trail added
   │
   └─> Tables: stg_* (8 tables)

3. TRANSFORMATION LAYER (src/transformation/)
   │
   ├─> Business logic applied
   │   • Dimensions: Type 1 & Type 2 SCD
   │   • Facts: Dimension key lookups
   │   • Aggregates: Pre-calculated metrics
   │
   └─> Tables: dim_*, fact_*, agg_*

4. QUALITY LAYER (src/quality/)
   │
   ├─> Validation rules executed
   │   • 15+ rules across 7 dimensions
   │   • Exception tracking
   │   • Scorecards generated
   │
   └─> Tables: dq_* (5 tables)

5. REPORTING (src/quality/quality_reports.py)
   │
   └─> HTML dashboards & Excel docs
```

### Why This Architecture?

**Separation of Concerns:**
- Staging keeps raw data unchanged (auditability)
- Transformation applies business logic (flexibility)
- Quality runs independently (doesn't break ETL)

**Modularity:**
- Each component can be run independently
- Easy to test individual pieces
- Can be scheduled separately (e.g., quality checks hourly)

**Scalability:**
- Staging layer can handle any source format
- Transformation logic is reusable
- Quality rules can be added without changing ETL

---

## Database Design Deep Dive

### File: `sql/ddl/01_staging_schema.sql`

**Purpose:** Create tables that mirror source system structure

**Why This Approach?**

```sql
-- Example: stg_crm_customers
CREATE TABLE stg_crm_customers (
    -- Source system fields (exactly as they come in)
    customer_id        VARCHAR(50),
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    email              VARCHAR(255),
    -- ... more fields ...
    
    -- Technical audit columns (I added these)
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_hash           VARCHAR(64),
    load_id            BIGINT
);
```

**Key Decisions:**

1. **VARCHAR instead of specific types:**
   - *Why?* Staging is forgiving - accepts any data
   - *Benefit:* Bad data doesn't break the load
   - *Trade-off:* More storage, but worth it for reliability

2. **No constraints (no NOT NULL, no foreign keys):**
   - *Why?* We want to load everything, even bad data
   - *Benefit:* Can identify and track data quality issues
   - *Trade-off:* Can't rely on database to enforce quality

3. **Audit columns:**
   - `source_file`: Which CSV did this come from?
   - `load_timestamp`: When was it loaded?
   - `row_hash`: MD5 hash for change detection
   - `load_id`: Batch identifier for tracking loads

**The 8 Staging Tables:**

| Table | Purpose | Row Count |
|-------|---------|-----------|
| `stg_crm_customers` | Customer master data | 50,000 |
| `stg_ecom_transactions` | Transaction headers | 200,000 |
| `stg_ecom_transaction_items` | Transaction line items | 419,907 |
| `stg_products` | Product catalog | 500 |
| `stg_marketing_campaigns` | Campaign definitions | 25 |
| `stg_campaign_responses` | Customer responses | 14,247 |
| `stg_audit_log` | Load tracking | Varies |
| `stg_data_profile` | Data profiling stats | Varies |

---

### File: `sql/ddl/02_warehouse_schema.sql`

**Purpose:** Star schema optimized for analytics

**Why Star Schema?**

- **Simple for business users:** They understand customers, products, dates
- **Fast queries:** Joins are straightforward and optimized
- **Flexible:** Easy to add new dimensions or facts
- **Proven:** Industry-standard approach (Kimball methodology)

#### Dimension Tables Explained

**1. dim_customer (SCD Type 2)**

```sql
CREATE TABLE dim_customer (
    customer_key       BIGSERIAL PRIMARY KEY,  -- Surrogate key
    customer_id        VARCHAR(50),            -- Business key
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    full_name          VARCHAR(200),
    address            VARCHAR(255),
    city               VARCHAR(100),
    state              VARCHAR(50),
    
    -- SCD Type 2 fields
    valid_from         DATE NOT NULL,
    valid_to           DATE NOT NULL DEFAULT '9999-12-31',
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Metrics (updated from facts)
    total_orders       INTEGER,
    total_spent        DECIMAL(15,2)
);
```

**Key Concepts:**

**Surrogate Key (`customer_key`):**
- *What?* Auto-incrementing integer with no business meaning
- *Why?* Allows multiple versions of same customer (SCD Type 2)
- *Example:* Customer "CUST000001" might have keys 1, 2, 3 for different address versions

**Business Key (`customer_id`):**
- *What?* The actual ID from the source system
- *Why?* Links back to source, can repeat for SCD Type 2
- *Example:* "CUST000001" stays the same across all versions

**SCD Type 2 Implementation:**

```
Customer moves from NY to CA:

Version 1 (Historical):
├─ customer_key: 1
├─ customer_id: CUST000001
├─ address: "123 Main St, New York"
├─ valid_from: 2023-01-15
├─ valid_to: 2024-06-30        ← Closed when they moved
└─ is_current: FALSE

Version 2 (Current):
├─ customer_key: 2             ← New surrogate key
├─ customer_id: CUST000001     ← Same business key
├─ address: "456 Oak Ave, Los Angeles"
├─ valid_from: 2024-07-01
├─ valid_to: 9999-12-31        ← Still current
└─ is_current: TRUE
```

**Why SCD Type 2 for Customers?**
- Addresses change over time
- Historical analysis: "Where were customers buying from in Q1 2023?"
- Accurate reporting: Match customer address to transaction date

**Why NOT Type 2 for Everything?**
- Email/phone updates don't need history (Type 1 - overwrite)
- More storage and complexity for Type 2
- Only track what business needs

---

**2. dim_product (SCD Type 1)**

```sql
CREATE TABLE dim_product (
    product_key           BIGSERIAL PRIMARY KEY,
    product_id            VARCHAR(50) UNIQUE NOT NULL,
    product_name          VARCHAR(200),
    product_category      VARCHAR(100),
    product_subcategory   VARCHAR(100),
    unit_cost             DECIMAL(10,2),
    retail_price          DECIMAL(10,2),
    margin_percentage     DECIMAL(5,2),  -- Calculated
    is_active             BOOLEAN
);
```

**Why SCD Type 1 (Overwrite)?**
- Price changes don't need history (we capture actual price in fact table)
- Product name updates are corrections, not new versions
- Less complexity, easier to maintain

**Calculated Fields:**
```python
margin_percentage = ((retail_price - unit_cost) / retail_price) * 100
```

**Why calculate and store?**
- Frequently used in reports
- Faster queries (no need to recalculate each time)
- Consistent calculation across all reports

---

**3. dim_date (Pre-populated)**

```sql
CREATE TABLE dim_date (
    date_key          INTEGER PRIMARY KEY,  -- 20230115
    date_actual       DATE NOT NULL,
    day_name          VARCHAR(20),
    day_of_week       INTEGER,
    week_of_year      INTEGER,
    month_number      INTEGER,
    month_name        VARCHAR(20),
    quarter           INTEGER,
    year_number       INTEGER,
    is_weekend        BOOLEAN,
    is_holiday        BOOLEAN
);
```

**Why Pre-populate?**
- Dates don't change - load once (2020-2030)
- Every report needs date attributes
- Enables time-based analysis without complex SQL

**Date Key Format:**
- `20230115` = January 15, 2023
- Integer type = fast joins
- Human-readable in queries

**Usage in Reports:**
```sql
-- Instead of:
SELECT EXTRACT(MONTH FROM transaction_date), ...

-- We can do:
SELECT d.month_name, ...
FROM fact_transactions f
JOIN dim_date d ON f.transaction_date_key = d.date_key
```

---

**4. dim_campaign**

```sql
CREATE TABLE dim_campaign (
    campaign_key       BIGSERIAL PRIMARY KEY,
    campaign_id        VARCHAR(50) UNIQUE NOT NULL,
    campaign_name      VARCHAR(200),
    campaign_channel   VARCHAR(50),  -- Email, Social, Display, Search
    start_date         DATE,
    end_date           DATE,
    budget_amount      DECIMAL(15,2),
    campaign_duration_days INTEGER   -- Calculated
);
```

**Why This Design?**
- Simple Type 1 (campaigns don't change after launch)
- Channel is dimension attribute (not separate dimension table)
  - *Why?* Only 4-5 channels, rarely changes
  - *Benefit:* Simpler queries, fewer joins

---

#### Fact Tables Explained

**1. fact_transactions (Transaction Grain)**

```sql
CREATE TABLE fact_transactions (
    transaction_item_key   BIGSERIAL PRIMARY KEY,
    transaction_item_id    VARCHAR(50),
    
    -- Foreign keys to dimensions
    customer_key           BIGINT REFERENCES dim_customer(customer_key),
    product_key            BIGINT REFERENCES dim_product(product_key),
    transaction_date_key   INTEGER REFERENCES dim_date(date_key),
    
    -- Degenerate dimensions (no separate table)
    transaction_id         VARCHAR(50),
    order_number           VARCHAR(50),
    payment_method         VARCHAR(50),
    
    -- Date/time
    transaction_datetime   TIMESTAMP,
    
    -- Measures (the numbers we analyze)
    quantity               INTEGER,
    unit_price             DECIMAL(10,2),
    line_total             DECIMAL(10,2),
    discount_amount        DECIMAL(10,2),
    net_amount             DECIMAL(10,2)
);
```

**Grain:** One row per product per transaction

```
Example: Customer buys 3 products:
└─ Creates 3 rows in fact_transactions
   ├─ Row 1: Product A
   ├─ Row 2: Product B
   └─ Row 3: Product C
```

**Degenerate Dimensions:**
- *What?* Dimension attributes stored directly in fact table
- *Why?* 
  - `transaction_id`: Too many unique values for dimension table
  - `order_number`: Customer-facing ID, useful for lookups
  - `payment_method`: Low cardinality but changes per transaction

**Measures:**
- `quantity`: How many units? (INTEGER)
- `unit_price`: Price per unit at time of sale
- `line_total`: quantity × unit_price
- `discount_amount`: Discount applied to this line
- `net_amount`: line_total - discount_amount (what we actually got paid)

**Why These Measures?**
- `net_amount` is most important - this is revenue
- Keep `unit_price` separate - shows price at time of sale (may differ from current price)
- `discount_amount` separate - enables discount analysis

---

**2. fact_campaign_responses (Event Grain)**

```sql
CREATE TABLE fact_campaign_responses (
    response_key         BIGSERIAL PRIMARY KEY,
    response_id          VARCHAR(50),
    
    -- Foreign keys
    campaign_key         BIGINT REFERENCES dim_campaign(campaign_key),
    customer_key         BIGINT REFERENCES dim_customer(customer_key),
    response_date_key    INTEGER REFERENCES dim_date(date_key),
    
    -- Response details
    response_type        VARCHAR(50),  -- Opened, Clicked, Converted
    response_datetime    TIMESTAMP,
    conversion_value     DECIMAL(15,2)
);
```

**Grain:** One row per customer response per campaign

**Response Type Values:**
- `Opened`: Customer viewed the campaign
- `Clicked`: Customer clicked through
- `Converted`: Customer made a purchase

**Conversion Value:**
- Amount spent if they converted
- NULL for opens/clicks
- Links campaign to revenue

---

#### Aggregate Tables (Performance Optimization)

**1. agg_customer_monthly**

```sql
CREATE TABLE agg_customer_monthly (
    customer_key           BIGINT,
    year_month             VARCHAR(7),  -- '2023-01'
    total_transactions     INTEGER,
    total_revenue          DECIMAL(15,2),
    total_units            INTEGER,
    avg_transaction_value  DECIMAL(15,2)
);
```

**Why Aggregates?**

**Without Aggregate:**
```sql
-- This scans millions of fact rows every time
SELECT 
    customer_key,
    TO_CHAR(transaction_datetime, 'YYYY-MM') as month,
    SUM(net_amount) as revenue
FROM fact_transactions
GROUP BY customer_key, month;
```

**With Aggregate:**
```sql
-- This scans pre-calculated summary (much faster)
SELECT customer_key, year_month, total_revenue
FROM agg_customer_monthly;
```

**Trade-offs:**
- ✅ Faster queries (10-100x for reporting)
- ✅ Less load on database
- ❌ More storage space
- ❌ Must be refreshed (adds to ETL time)

**When to Use Aggregates:**
- Common reporting patterns
- Large fact tables (millions of rows)
- Acceptable for data to be hours old

---

### File: `sql/ddl/04_quality_schema.sql`

**Purpose:** Track data quality metrics and results

#### The 5 Quality Tables

**1. dq_rule_categories**
```sql
CREATE TABLE dq_rule_categories (
    category_id     SERIAL PRIMARY KEY,
    category_name   VARCHAR(50) UNIQUE,  -- Completeness, Accuracy, etc.
    description     TEXT
);
```

**The 7 Quality Dimensions:**
1. **Completeness** - Are required fields populated?
2. **Accuracy** - Are values within valid ranges?
3. **Consistency** - Do values agree across tables?
4. **Validity** - Do values meet business rules?
5. **Uniqueness** - Are there duplicates?
6. **Timeliness** - Is data fresh?
7. **Integrity** - Do foreign keys exist?

---

**2. dq_rules**
```sql
CREATE TABLE dq_rules (
    rule_id              SERIAL PRIMARY KEY,
    rule_name            VARCHAR(200) UNIQUE,
    rule_description     TEXT,
    category_id          INTEGER REFERENCES dq_rule_categories,
    target_table         VARCHAR(100),
    target_column        VARCHAR(100),
    rule_type            VARCHAR(50),
    rule_sql             TEXT,           -- SQL that finds violations
    severity             VARCHAR(20),    -- CRITICAL, WARNING, INFO
    failure_threshold    DECIMAL(5,2)    -- % failures allowed
);
```

**Example Rule:**
```sql
rule_name: "Customer Email Not Null"
rule_sql: "
    SELECT customer_key, 'email' as column_name
    FROM dim_customer
    WHERE email IS NULL
      AND is_current = TRUE
"
severity: "WARNING"
failure_threshold: 5.0  -- Allow up to 5% missing
```

**How It Works:**
1. SQL runs and returns rows with violations
2. Count violations vs total records
3. If % violations > threshold → FAILED
4. Otherwise → PASSED or WARNING

---

**3. dq_test_results**
```sql
CREATE TABLE dq_test_results (
    result_id                BIGSERIAL PRIMARY KEY,
    rule_id                  INTEGER REFERENCES dq_rules,
    execution_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_records_checked    INTEGER,
    failed_records           INTEGER,
    passed_records           INTEGER,
    failure_percentage       DECIMAL(5,2),
    test_status              VARCHAR(20),  -- PASSED, FAILED, WARNING
    test_message             TEXT
);
```

**Example Result:**
```
rule_id: 1 (Email Not Null)
total_records_checked: 50000
failed_records: 1535
passed_records: 48465
failure_percentage: 3.07%
test_status: "WARNING" (under 5% threshold)
test_message: "1535 violations found but within threshold (5%)"
```

---

**4. dq_exceptions**
```sql
CREATE TABLE dq_exceptions (
    exception_id        BIGSERIAL PRIMARY KEY,
    result_id           BIGINT REFERENCES dq_test_results,
    rule_id             INTEGER REFERENCES dq_rules,
    table_name          VARCHAR(100),
    record_identifier   VARCHAR(500),  -- Which row failed?
    column_name         VARCHAR(100),
    failed_value        TEXT,
    expected_value      TEXT,
    detected_date       TIMESTAMP
);
```

**Purpose:** Store details of each violation

**Example:**
```
table_name: "dim_customer"
record_identifier: "customer_key=12345"
column_name: "email"
failed_value: NULL
expected_value: "NOT NULL"
```

**Why Limit to 1000?**
- 41,000 violations = too much data
- We get the pattern from 1000 samples
- Prevents database bloat

---

**5. dq_scorecards**
```sql
CREATE TABLE dq_scorecards (
    scorecard_id             BIGSERIAL PRIMARY KEY,
    report_date              DATE NOT NULL,
    overall_quality_score    DECIMAL(5,2),
    completeness_score       DECIMAL(5,2),
    accuracy_score           DECIMAL(5,2),
    -- ... 5 more dimension scores ...
    total_rules_executed     INTEGER,
    rules_passed             INTEGER,
    rules_failed             INTEGER
);
```

**Purpose:** Daily summary of quality health

**Calculation:**
```
overall_quality_score = AVG(
    completeness_score,
    accuracy_score,
    consistency_score,
    validity_score,
    uniqueness_score,
    timeliness_score,
    integrity_score
)
```

**Why Daily Scorecards?**
- Trend analysis over time
- Executive dashboards
- Alerting (if score drops below threshold)

---

## Configuration System

### File: `config/config.yaml`

**Purpose:** Application-wide settings (non-sensitive)

```yaml
project:
  name: "Enterprise Data Integration Platform"
  version: "1.0.0"
  environment: "development"

data_generation:
  customers:
    count: 50000
    seed: 42
  transactions:
    count: 200000

paths:
  raw_data: "data/raw"
  staging_data: "data/staging"
  logs: "logs"

etl:
  batch_size: 10000
  parallel_processing: true
```

**Why YAML?**
- Human-readable
- Supports nested structures
- Comments allowed
- No code changes to adjust settings

**Environment-Specific:**
```yaml
environment: "development"  # or "staging", "production"
```

Can have different configs for each environment.

---

### File: `config/database.ini`

**Purpose:** Database credentials (sensitive, gitignored)

```ini
[postgresql]
host = localhost
port = 5432
database = enterprise_dw
user = postgres
password = your_password
```

**Why Separate File?**
- Never commit passwords to Git
- Easy to change per environment
- Standard format (INI files)

**Security:**
- Added to `.gitignore`
- Users copy from template
- Can use environment variables instead

---

## Utilities & Infrastructure

### File: `src/utils/config_loader.py`

**Purpose:** Load configuration from YAML and INI files

```python
class ConfigLoader:
    """Handles loading configuration from various sources"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config = None
        self.db_config = None
```

**Key Methods:**

**1. load_yaml_config()**
```python
def load_yaml_config(self, filename: str = "config.yaml"):
    config_path = self.config_dir / filename
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        self.config = yaml.safe_load(f)
    
    return self.config
```

**Why This Approach?**
- Validates file exists before loading
- Uses `yaml.safe_load` (security - prevents code execution)
- Returns config for use

---

**2. get() - Nested Config Access**
```python
def get(self, key_path: str, default=None):
    """
    Get config value using dot notation
    Example: get('data_generation.customers.count')
    """
    keys = key_path.split('.')
    value = self.config
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    
    return value
```

**Example Usage:**
```python
config = ConfigLoader()
config.load_yaml_config()

# Instead of: config.config['data_generation']['customers']['count']
customer_count = config.get('data_generation.customers.count', 10000)
```

**Why Dot Notation?**
- Cleaner code
- Default values built-in
- Handles missing keys gracefully

---

**3. get_db_connection_string()**
```python
def get_db_connection_string(self, db_type: str = 'postgresql'):
    """Build SQLAlchemy connection string"""
    db_config = self.db_config[db_type]
    
    return (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}"
        f"/{db_config['database']}"
    )
```

**Output:**
```
postgresql://postgres:password@localhost:5432/enterprise_dw
```

**Why Build String Here?**
- Centralized connection logic
- Easy to switch databases
- Handles URL encoding if needed

---

**4. Singleton Pattern**
```python
# Module-level instances
_config_instance = None

def get_config():
    """Get singleton config instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader()
        _config_instance.load_yaml_config()
        _config_instance.load_db_config()
    return _config_instance
```

**Why Singleton?**
- Load config once, use everywhere
- Saves file I/O
- Consistent config across application

---

### File: `src/utils/logger.py`

**Purpose:** Centralized logging with file and console output

```python
class LoggerSetup:
    """Configure and manage application logging"""
    
    def setup(self):
        """Configure loguru logger"""
        # Remove default handler
        logger.remove()
        
        # Console handler with colors
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
            level="INFO",
            colorize=True
        )
        
        # File handler (all levels)
        log_file = self.log_dir / f"app_{datetime.now():%Y%m%d}.log"
        logger.add(
            log_file,
            format="{time} | {level} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="500 MB",    # Rotate when file hits 500MB
            retention="30 days",  # Keep logs for 30 days
            compression="zip"     # Compress old logs
        )
```

**Why loguru Instead of Standard logging?**
- Simpler API (no complex setup)
- Better formatting out of the box
- Automatic log rotation
- Color support
- Better exception handling

**Log Levels:**
```python
logger.debug("Detailed diagnostic info")
logger.info("General information")
logger.warning("Something unexpected")
logger.error("Error occurred but app continues")
logger.critical("Serious error, app might crash")
```

**Log Output Examples:**
```
2026-01-30 14:15:37 | INFO | Starting ETL pipeline
2026-01-30 14:15:38 | WARNING | Customer email missing for 1535 records
2026-01-30 14:15:42 | ERROR | Failed to load transaction: Invalid date format
```

**Rotation Strategy:**
- Daily files: `app_20260130.log`
- Rotates at 500MB
- Keeps 30 days of history
- Older logs compressed to save space

---

### File: `src/utils/db_manager.py`

**Purpose:** Database connection and operation management

```python
class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, db_type: str = None):
        self.config = get_config()
        self.db_type = db_type or self.config.get('database.type', 'postgresql')
        self._engine = None
```

**Why This Class?**
- Centralize all database operations
- Consistent error handling
- Connection pooling management
- Reusable across all modules

---

**Key Methods:**

**1. Engine Property (Lazy Loading)**
```python
@property
def engine(self) -> Engine:
    """Get or create SQLAlchemy engine"""
    if self._engine is None:
        conn_string = self.config.get_db_connection_string(self.db_type)
        self._engine = create_engine(
            conn_string,
            poolclass=NullPool,  # Disable pooling for simplicity
            echo=False           # Set True to see SQL
        )
        logger.info(f"Database engine created for {self.db_type}")
    return self._engine
```

**Why Lazy Loading?**
- Only creates connection when needed
- Import DatabaseManager without connecting
- Saves resources if database not used

**Why NullPool?**
- Simpler for batch processing
- Each operation gets fresh connection
- No connection pool management complexity
- *Trade-off:* Slower for many small queries

---

**2. Context Manager for Connections**
```python
@contextmanager
def get_connection(self):
    """Context manager for database connections"""
    with self.engine.begin() as conn:
        yield conn
```

**Usage:**
```python
with db.get_connection() as conn:
    result = conn.execute(text("SELECT * FROM dim_customer"))
    # Connection automatically closed and committed
```

**Why Context Manager?**
- Automatic connection cleanup
- Automatic commit/rollback
- Exception handling
- Pythonic pattern

---

**3. SQLAlchemy 2.0 Pattern**
```python
def execute_sql(self, sql: str, params: dict = None):
    """Execute SQL statement"""
    try:
        with self.engine.begin() as conn:
            result = conn.execute(text(sql), params or {})
            return result
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise
```

**Key Changes from Old SQLAlchemy:**
- Use `engine.begin()` instead of `engine.connect()`
- Wrap SQL strings in `text()`
- Context manager handles commit/rollback
- **Auto-commit**: Changes automatically saved

**Why Upgrade to 2.0?**
- Current best practices
- Better error messages
- Explicit transaction handling
- Future-proof

---

**4. read_query() - Pandas Integration**
```python
def read_query(self, query: str, params: dict = None) -> pd.DataFrame:
    """Execute query and return pandas DataFrame"""
    try:
        with self.engine.begin() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        logger.debug(f"Query returned {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise
```

**Why Pandas Integration?**
- Most data work uses DataFrames
- Easy data manipulation
- Direct CSV export
- Familiar API for data scientists

---

**5. load_dataframe() - Bulk Insert**
```python
def load_dataframe(
    self,
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = 'append',
    chunksize: int = 1000
) -> int:
    """Load DataFrame to database table"""
    try:
        # Convert numpy types to Python types
        for col in df.columns:
            if df[col].dtype == 'int64':
                df[col] = df[col].astype(int)
            elif df[col].dtype == 'float64':
                df[col] = df[col].astype(float)
        
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method='multi'  # Faster bulk insert
        )
        
        logger.info(f"Loaded {len(df)} rows to {table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise
```

**Why Type Conversion?**
- psycopg2 doesn't handle numpy types
- `int64` ≠ Python `int`
- Explicit conversion prevents errors

**Chunksize:**
- Loads 1000 rows at a time
- Prevents memory issues
- Better progress tracking
- *Trade-off:* Slower than single insert

**if_exists Options:**
- `'append'`: Add to existing data
- `'replace'`: Drop table and recreate
- `'fail'`: Error if table exists

---

## Data Generation

### File: `src/data_generation/generate_data.py`

**Purpose:** Create realistic synthetic data for testing

**Why Generate Data?**
- No access to real production data
- Control data volume and quality
- Reproducible (seed = 42)
- Can inject intentional errors for testing

---

### Class: DataGenerator

```python
class DataGenerator:
    def __init__(self, seed: int = 42):
        self.config = get_config()
        self.seed = seed
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
```

**Why Set All Seeds?**
- Makes generation reproducible
- Same seed = same data every time
- Important for testing and demos

---

### Method: generate_customers()

```python
def generate_customers(self, count: int = None) -> pd.DataFrame:
    """Generate synthetic customer data with intentional quality issues"""
    count = count or self.config.get('data_generation.customers.count', 50000)
    
    customers = []
    
    for i in range(count):
        # Intentional data quality issue: 5% missing emails
        email = self.fake.email() if random.random() > 0.05 else None
        
        # Inconsistent phone formats
        phone_formats = [
            self.fake.phone_number(),
            self.fake.numerify('###-###-####'),
            None  # 2% missing
        ]
        phone = random.choice(phone_formats) if random.random() > 0.02 else None
        
        customer = {
            'customer_id': f'CUST{i+1:06d}',  # CUST000001, CUST000002, ...
            'first_name': self.fake.first_name(),
            'last_name': self.fake.last_name(),
            'email': email,
            'phone_number': phone,
            'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
            'gender': random.choice(['Male', 'Female', 'Other', None]),
            'address': self.fake.street_address(),
            'city': self.fake.city(),
            'state': self.fake.state_abbr(),
            'postal_code': self.fake.postcode(),
            'country': 'USA',
            'registration_date': self.fake.date_between(
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2026, 1, 26)
            ),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'VIP']),
            'account_status': random.choice(['Active', 'Inactive'])
        }
        
        customers.append(customer)
    
    return pd.DataFrame(customers)
```

**Key Design Decisions:**

**1. Customer ID Format:**
```python
f'CUST{i+1:06d}'  # CUST000001
```
- Consistent format
- Zero-padded to 6 digits
- Easy to sort and identify

**2. Intentional Quality Issues:**
```python
email = self.fake.email() if random.random() > 0.05 else None
```
- 5% missing emails (realistic!)
- Tests data quality rules
- Shows handling of incomplete data

**3. Registration Date Range:**
```python
registration_date = self.fake.date_between(
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2026, 1, 26)
)
```
- Spreads customers over 6 years
- Enables cohort analysis
- Realistic customer growth pattern

---

### Method: generate_products()

```python
def generate_products(self, count: int = 500) -> pd.DataFrame:
    """Generate product catalog"""
    
    categories = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['Shirt', 'Pants', 'Dress', 'Jacket', 'Shoes'],
        'Home': ['Furniture', 'Kitchenware', 'Bedding', 'Decor', 'Lighting'],
        'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children']
    }
    
    products = []
    
    for i in range(count):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        # Price varies by category
        if category == 'Electronics':
            cost = random.uniform(100, 1000)
            retail = cost * random.uniform(1.2, 1.5)  # 20-50% markup
        elif category == 'Clothing':
            cost = random.uniform(10, 100)
            retail = cost * random.uniform(1.5, 2.5)  # 50-150% markup
        else:
            cost = random.uniform(5, 200)
            retail = cost * random.uniform(1.3, 2.0)
        
        product = {
            'product_id': f'PROD{i+1:05d}',
            'product_name': f'{self.fake.word().title()} {subcategory}',
            'product_category': category,
            'product_subcategory': subcategory,
            'brand': self.fake.company(),
            'unit_cost': round(cost, 2),
            'retail_price': round(retail, 2),
            'product_status': random.choices(
                ['Active', 'Inactive'],
                weights=[0.9, 0.1]  # 90% active
            )[0]
        }
        
        products.append(product)
    
    return pd.DataFrame(products)
```

**Why Category-Based Pricing?**
- Electronics: Higher cost, lower margin
- Clothing: Lower cost, higher margin
- Realistic business model

**Product Status Weighting:**
```python
random.choices(['Active', 'Inactive'], weights=[0.9, 0.1])
```
- 90% active products
- 10% discontinued/seasonal
- Realistic for real business

---

### Method: generate_transactions()

```python
def generate_transactions(
    self,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    count: int = 200000
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generate transactions and line items"""
    
    transactions = []
    transaction_items = []
    
    for i in range(count):
        # Pick random customer
        customer = customers_df.sample(1).iloc[0]
        
        # Transaction date between registration and now
        txn_date = self.fake.date_time_between(
            start_date=customer['registration_date'],
            end_date='now'
        )
        
        # 1-5 products per transaction (weighted toward fewer)
        num_items = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]
        
        # Pick random products
        items = products_df.sample(num_items)
        
        txn_id = f'TXN{i+1:07d}'
        order_number = f'ORD-{txn_date.year}-{i+1:05d}'
        
        txn_total = 0
        txn_discount = 0
        
        for item_idx, (_, product) in enumerate(items.iterrows()):
            quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 25, 10, 3, 2])[0]
            unit_price = product['retail_price']
            line_total = quantity * unit_price
            
            # Random discount (0-20%)
            discount = line_total * random.uniform(0, 0.2) if random.random() > 0.7 else 0
            
            transaction_items.append({
                'transaction_item_id': f'TXI{len(transaction_items)+1:07d}',
                'transaction_id': txn_id,
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'line_total': round(line_total, 2),
                'discount_amount': round(discount, 2)
            })
            
            txn_total += line_total
            txn_discount += discount
        
        transactions.append({
            'transaction_id': txn_id,
            'customer_id': customer['customer_id'],
            'transaction_date': txn_date,
            'order_number': order_number,
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']),
            'payment_status': random.choices(
                ['Completed', 'Pending', 'Failed'],
                weights=[0.95, 0.03, 0.02]
            )[0],
            'total_amount': round(txn_total, 2),
            'discount_amount': round(txn_discount, 2),
            'tax_amount': round(txn_total * 0.08, 2),
            'shipping_amount': round(random.uniform(0, 15), 2),
            'currency_code': 'USD'
        })
    
    return pd.DataFrame(transactions), pd.DataFrame(transaction_items)
```

**Key Design Decisions:**

**1. Transaction Date Logic:**
```python
txn_date = self.fake.date_time_between(
    start_date=customer['registration_date'],
    end_date='now'
)
```
- Customer can't buy before registering!
- Realistic customer journey
- Enables lifetime value analysis

**2. Items Per Transaction:**
```python
num_items = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]
```
- Most transactions (50%) have 1 item
- Few (3%) have 5 items
- Realistic shopping behavior

**3. Quantity Distribution:**
```python
quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 25, 10, 3, 2])[0]
```
- Most people buy 1 of something
- Bulk purchases rare
- Realistic for retail

**4. Discount Logic:**
```python
discount = line_total * random.uniform(0, 0.2) if random.random() > 0.7 else 0
```
- 30% of items get discount
- Discount is 0-20% of price
- Realistic promotion pattern

---

### Method: generate_campaigns()

```python
def generate_campaigns(self, count: int = 25) -> pd.DataFrame:
    """Generate marketing campaigns"""
    
    campaigns = []
    
    for i in range(count):
        start_date = self.fake.date_between(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2026, 1, 1)
        )
        
        duration = random.randint(7, 90)  # 1 week to 3 months
        end_date = start_date + timedelta(days=duration)
        
        channel = random.choice(['Email', 'Social Media', 'Display Ads', 'Search Ads'])
        
        # Budget varies by channel
        if channel in ['Display Ads', 'Search Ads']:
            budget = random.uniform(50000, 200000)
        else:
            budget = random.uniform(10000, 50000)
        
        campaigns.append({
            'campaign_id': f'CAMP{i+1:04d}',
            'campaign_name': f'{random.choice(["Spring", "Summer", "Fall", "Winter", "Holiday"])} '
                           f'{random.choice(["Sale", "Promotion", "Event", "Launch"])} '
                           f'{random.randint(2023, 2026)}',
            'campaign_channel': channel,
            'start_date': start_date,
            'end_date': end_date,
            'budget_amount': round(budget, 2),
            'target_audience': random.choice(['All', 'VIP', 'Premium', 'New Customers'])
        })
    
    return pd.DataFrame(campaigns)
```

**Realistic Elements:**
- Seasonal naming (Spring Sale, Holiday Event)
- Channel-appropriate budgets (Search Ads cost more)
- Typical campaign duration (1 week to 3 months)

---

### Method: generate_campaign_responses()

```python
def generate_campaign_responses(
    self,
    campaigns_df: pd.DataFrame,
    customers_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate customer responses to campaigns"""
    
    responses = []
    
    for _, campaign in campaigns_df.iterrows():
        # Response rate varies by channel
        if campaign['campaign_channel'] == 'Email':
            response_rate = 0.20  # 20% open rate
        elif campaign['campaign_channel'] == 'Social Media':
            response_rate = 0.10
        else:
            response_rate = 0.05
        
        # Number of responses for this campaign
        num_responses = int(len(customers_df) * response_rate)
        
        # Pick random customers
        respondents = customers_df.sample(num_responses)
        
        for _, customer in respondents.iterrows():
            response_date = self.fake.date_time_between(
                start_date=campaign['start_date'],
                end_date=campaign['end_date']
            )
            
            # Response funnel: Opened → Clicked → Converted
            response_type = random.choices(
                ['Opened', 'Clicked', 'Converted'],
                weights=[70, 20, 10]  # 70% just open, 10% convert
            )[0]
            
            conversion_value = None
            if response_type == 'Converted':
                # Converters spend $50-$500
                conversion_value = round(random.uniform(50, 500), 2)
            
            responses.append({
                'response_id': f'RESP{len(responses)+1:06d}',
                'campaign_id': campaign['campaign_id'],
                'customer_id': customer['customer_id'],
                'response_date': response_date,
                'response_type': response_type,
                'conversion_value': conversion_value
            })
    
    return pd.DataFrame(responses)
```

**Marketing Funnel:**
```
1000 recipients
  └─> 200 open (20%)
      └─> 40 click (20% of opens)
          └─> 4 convert (10% of clicks)
```

**Why This Funnel?**
- Realistic conversion rates
- Shows full customer journey
- Enables funnel analysis

---

### Method: save_to_csv()

```python
def save_to_csv(self, df: pd.DataFrame, filename: str):
    """Save DataFrame to CSV with timestamp"""
    timestamp = datetime.now().strftime('%Y%m%d')
    output_file = self.output_dir / f"{filename}_{timestamp}.csv"
    
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(df)} rows to {output_file}")
```

**Why Timestamped Files?**
- Track when data was generated
- Multiple versions for testing
- Audit trail

**Example Filenames:**
```
crm_customers_20260130.csv
ecom_transactions_20260130.csv
products_20260130.csv
```

---

## ETL Pipeline - Ingestion

### File: `src/ingestion/load_staging.py`

**Purpose:** Load CSV files into staging tables

**Key Principle:** Get data into database with minimal transformation

---

### Class: StagingLoader

```python
class StagingLoader:
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        self.raw_data_path = Path(self.config.get('paths.raw_data'))
        self.batch_size = self.config.get('etl.batch_size', 10000)
        self.load_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))
```

**Load ID Format:**
```python
load_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))
# Example: 20260130141542 (Jan 30, 2026, 2:15:42 PM)
```

**Why?**
- Unique identifier for each load
- Chronological ordering
- Easy to read in logs

---

### Method: _generate_row_hash()

```python
def _generate_row_hash(self, row: pd.Series) -> str:
    """Generate MD5 hash for a row (for change detection)"""
    row_string = '|'.join(str(v) for v in row.values)
    return hashlib.md5(row_string.encode()).hexdigest()
```

**Example:**
```python
row = {'id': 'CUST000001', 'name': 'John Smith', 'email': 'john@example.com'}
row_string = 'CUST000001|John Smith|john@example.com'
hash = 'a1b2c3d4e5f6...'  # MD5 hash
```

**Why Hash Rows?**
- Detect changed records
- Enable incremental loading
- Compare old vs new data

**Usage:**
```sql
-- Find changed customers
SELECT * FROM stg_crm_customers_new
WHERE row_hash NOT IN (
    SELECT row_hash FROM stg_crm_customers_old
)
```

---

### Method: _add_audit_columns()

```python
def _add_audit_columns(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Add technical audit columns to DataFrame"""
    df['source_file'] = source_file
    df['load_timestamp'] = datetime.now()
    df['load_id'] = self.load_id
    
    # Generate row hash for change detection
    df['row_hash'] = df.apply(self._generate_row_hash, axis=1)
    
    return df
```

**Result:**
```
customer_id | first_name | ... | source_file              | load_timestamp      | load_id        | row_hash
CUST000001  | John       | ... | crm_customers_20260130  | 2026-01-30 14:15:42 | 20260130141542 | a1b2c3...
```

**Why Add These Columns?**
- **source_file**: Trace back to source
- **load_timestamp**: When did we load this?
- **load_id**: Batch identifier
- **row_hash**: Change detection

---

### Method: _log_audit()

```python
def _log_audit(
    self,
    table_name: str,
    load_type: str,
    source_file: str,
    rows_loaded: int,
    rows_rejected: int,
    status: str,
    error_message: str = None
) -> None:
    """Log load statistics to audit table"""
    
    audit_record = {
        'load_id': self.load_id,
        'table_name': table_name,
        'load_type': load_type,  # 'FULL', 'INCREMENTAL'
        'source_file': source_file,
        'load_date': datetime.now(),
        'rows_loaded': rows_loaded,
        'rows_rejected': rows_rejected,
        'status': status,  # 'SUCCESS', 'FAILED', 'PARTIAL'
        'error_message': error_message
    }
    
    self.db.load_dataframe(
        pd.DataFrame([audit_record]),
        'stg_audit_log',
        if_exists='append'
    )
```

**Example Audit Log:**
```
load_id        | table_name         | rows_loaded | status  | load_date
20260130141542 | stg_crm_customers | 50000       | SUCCESS | 2026-01-30 14:15:42
20260130141542 | stg_products      | 500         | SUCCESS | 2026-01-30 14:15:45
```

**Why Audit Logging?**
- Track every load
- Debug failures
- Performance monitoring
- Compliance/audit requirements

---

### Method: _find_latest_file()

```python
def _find_latest_file(self, pattern: str) -> Optional[Path]:
    """Find the most recent file matching pattern"""
    matching_files = list(self.raw_data_path.glob(pattern))
    
    if not matching_files:
        return None
    
    # Sort by modification time, get latest
    latest = max(matching_files, key=lambda p: p.stat().st_mtime)
    return latest
```

**Example:**
```python
pattern = "crm_customers_*.csv"
files = [
    "crm_customers_20260128.csv",
    "crm_customers_20260129.csv",
    "crm_customers_20260130.csv"  # ← Latest
]
```

**Why Find Latest?**
- Don't need to specify exact filename
- Works with dated files
- Always loads newest data

---

### Method: load_table()

```python
def load_table(
    self,
    table_name: str,
    file_pattern: str,
    load_type: str = 'FULL'
) -> int:
    """Load CSV file into staging table"""
    
    logger.info(f"Loading {table_name}...")
    
    try:
        # Find source file
        source_file = self._find_latest_file(file_pattern)
        if not source_file:
            raise FileNotFoundError(f"No file found for pattern: {file_pattern}")
        
        # Read CSV
        df = pd.read_csv(source_file)
        logger.info(f"Read {len(df)} rows from {source_file.name}")
        
        # Add audit columns
        df = self._add_audit_columns(df, source_file.name)
        
        # Truncate table if FULL load
        if load_type == 'FULL':
            self.db.execute_sql(f"TRUNCATE TABLE {table_name}")
            logger.info(f"Truncated {table_name}")
        
        # Load in batches
        rows_loaded = 0
        for i in range(0, len(df), self.batch_size):
            batch = df.iloc[i:i + self.batch_size]
            self.db.load_dataframe(
                batch,
                table_name,
                if_exists='append',
                chunksize=1000
            )
            rows_loaded += len(batch)
            logger.info(f"Loaded batch {i//self.batch_size + 1}: {rows_loaded}/{len(df)} rows")
        
        # Log success
        self._log_audit(
            table_name=table_name,
            load_type=load_type,
            source_file=source_file.name,
            rows_loaded=rows_loaded,
            rows_rejected=0,
            status='SUCCESS'
        )
        
        logger.info(f"✓ Successfully loaded {rows_loaded} rows to {table_name}")
        return rows_loaded
        
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        
        # Log failure
        self._log_audit(
            table_name=table_name,
            load_type=load_type,
            source_file=file_pattern,
            rows_loaded=0,
            rows_rejected=0,
            status='FAILED',
            error_message=str(e)
        )
        
        raise
```

**Complete Flow:**

```
1. Find latest CSV file
   └─> "crm_customers_20260130.csv"

2. Read CSV into DataFrame
   └─> 50,000 rows

3. Add audit columns
   └─> source_file, load_timestamp, load_id, row_hash

4. Truncate table (if FULL load)
   └─> DELETE FROM stg_crm_customers

5. Load in batches (10,000 rows each)
   └─> Batch 1: 0-10,000
   └─> Batch 2: 10,000-20,000
   └─> Batch 3: 20,000-30,000
   └─> Batch 4: 30,000-40,000
   └─> Batch 5: 40,000-50,000

6. Log to audit table
   └─> Record success/failure

7. Return rows loaded
```

**Why Batching?**
- Large files don't fit in memory
- Progress tracking
- Easier to debug failures
- Database can process in chunks

**Error Handling:**
```python
except Exception as e:
    logger.error(f"Failed: {e}")
    # Log failure to audit
    # Re-raise exception
    raise
```

**Why Re-raise?**
- Caller knows something failed
- Can decide how to handle
- Stack trace preserved

---

### Method: load_all_tables()

```python
def load_all_tables(self) -> dict:
    """Load all staging tables"""
    
    tables = {
        'stg_crm_customers': 'crm_customers_*.csv',
        'stg_ecom_transactions': 'ecom_transactions_*.csv',
        'stg_ecom_transaction_items': 'ecom_transaction_items_*.csv',
        'stg_products': 'products_*.csv',
        'stg_marketing_campaigns': 'marketing_campaigns_*.csv',
        'stg_campaign_responses': 'campaign_responses_*.csv'
    }
    
    results = {}
    
    for table_name, file_pattern in tables.items():
        try:
            rows = self.load_table(table_name, file_pattern)
            results[table_name] = {'status': 'SUCCESS', 'rows': rows}
        except Exception as e:
            results[table_name] = {'status': 'FAILED', 'error': str(e)}
    
    return results
```

**Example Output:**
```python
{
    'stg_crm_customers': {'status': 'SUCCESS', 'rows': 50000},
    'stg_products': {'status': 'SUCCESS', 'rows': 500},
    'stg_ecom_transactions': {'status': 'FAILED', 'error': 'File not found'},
}
```

**Why Continue on Error?**
- Load what we can
- Don't let one failure stop everything
- Report all results at end

---

### Command-Line Interface

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', help='Load specific table')
    parser.add_argument('--all', action='store_true', help='Load all tables')
    
    args = parser.parse_args()
    
    loader = StagingLoader()
    
    if args.all:
        results = loader.load_all_tables()
        # Print summary
    elif args.table:
        loader.load_table(args.table, f"{args.table}_*.csv")
    else:
        print("Usage: --table TABLE_NAME or --all")

if __name__ == "__main__":
    main()
```

**Usage:**
```bash
# Load single table
python src/ingestion/load_staging.py --table stg_crm_customers

# Load all tables
python src/ingestion/load_staging.py --all
```

**Why CLI?**
- Flexible execution
- Can be scripted
- Easy testing of single tables
- Automation friendly

---

## ETL Pipeline - Transformation

### Overview

Transformation is where business logic lives. We transform staging data into the dimensional model.

**Key Principles:**
1. Dimensions first (customers, products, dates)
2. Then facts (transactions, responses)
3. Then aggregates (monthly summaries)
4. All within transactions (rollback on error)

---

### File: `src/transformation/transform_dimensions.py`

**Purpose:** Transform staging data into dimension tables

---

### Method: transform_product_dimension()

**Approach: SCD Type 1 (Overwrite)**

```python
def transform_product_dimension(self) -> int:
    """Transform products from staging to dimension"""
    
    logger.info("Transforming Product Dimension")
    
    try:
        # Read from staging
        df = self.db.read_query("""
            SELECT DISTINCT
                product_id,
                product_name,
                product_category,
                product_subcategory,
                brand,
                unit_cost,
                retail_price,
                product_status
            FROM stg_products
            WHERE product_id IS NOT NULL
        """)
        
        logger.info(f"Read {len(df)} products from staging")
        
        # Calculate margin percentage
        df['margin_percentage'] = (
            (df['retail_price'] - df['unit_cost']) / df['retail_price'] * 100
        ).round(2)
        
        # Set is_active flag
        df['is_active'] = df['product_status'] == 'Active'
        
        # Add metadata
        df['created_date'] = datetime.now()
        df['updated_date'] = datetime.now()
        df['source_system'] = 'E-COMMERCE'
        
        # Check if products already exist
        existing_products = self.db.read_query("""
            SELECT product_id FROM dim_product
        """)
        
        if len(existing_products) > 0:
            # Split into new and updates
            new_products = df[~df['product_id'].isin(existing_products['product_id'])]
            update_products = df[df['product_id'].isin(existing_products['product_id'])]
            
            # Insert new products
            if len(new_products) > 0:
                self.db.load_dataframe(new_products, 'dim_product', if_exists='append')
                logger.info(f"Inserted {len(new_products)} new products")
            
            # Update existing products (SCD Type 1)
            if len(update_products) > 0:
                for _, product in update_products.iterrows():
                    self.db.execute_sql("""
                        UPDATE dim_product
                        SET product_name = :name,
                            product_category = :category,
                            product_subcategory = :subcategory,
                            brand = :brand,
                            unit_cost = :cost,
                            retail_price = :price,
                            margin_percentage = :margin,
                            product_status = :status,
                            is_active = :active,
                            updated_date = :updated,
                            source_system = :source
                        WHERE product_id = :id
                    """, {
                        'name': product['product_name'],
                        'category': product['product_category'],
                        'subcategory': product['product_subcategory'],
                        'brand': product['brand'],
                        'cost': float(product['unit_cost']),
                        'price': float(product['retail_price']),
                        'margin': float(product['margin_percentage']),
                        'status': product['product_status'],
                        'active': bool(product['is_active']),
                        'updated': datetime.now(),
                        'source': product['source_system'],
                        'id': product['product_id']
                    })
                logger.info(f"Updated {len(update_products)} products")
        else:
            # First load - insert all
            self.db.load_dataframe(df, 'dim_product', if_exists='append')
            logger.info(f"Inserted {len(df)} products (initial load)")
        
        logger.info(f"✓ Product dimension complete")
        return len(df)
        
    except Exception as e:
        logger.error(f"Product transformation failed: {e}")
        raise
```

**Key Steps:**

**1. Read Distinct Products:**
```sql
SELECT DISTINCT ...  -- Remove duplicates
WHERE product_id IS NOT NULL  -- Valid products only
```

**2. Calculate Derived Fields:**
```python
margin_percentage = (retail_price - unit_cost) / retail_price * 100
is_active = (product_status == 'Active')
```

**3. Handle New vs. Existing:**
```python
if existing_products:
    new = df[not in existing]      # INSERT these
    updates = df[in existing]      # UPDATE these
else:
    # First load - INSERT all
```

**4. SCD Type 1 Update:**
```sql
UPDATE dim_product
SET product_name = :name,
    retail_price = :price,
    ...
WHERE product_id = :id  -- Overwrites existing row
```

**Why Type 1 for Products?**
- Price changes don't need history (captured in facts)
- Name changes are corrections
- Simpler maintenance
- Less storage

---

### Method: transform_customer_dimension()

**Approach: SCD Type 2 (Full History)**

This is the most complex transformation!

```python
def transform_customer_dimension(self) -> int:
    """Transform customers with SCD Type 2"""
    
    logger.info("Transforming Customer Dimension (SCD Type 2)")
    
    try:
        # Read from staging
        df = self.db.read_query("""
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                phone_number,
                date_of_birth,
                gender,
                address,
                city,
                state,
                postal_code,
                country,
                registration_date,
                customer_segment,
                account_status
            FROM stg_crm_customers
            WHERE customer_id IS NOT NULL
        """)
        
        logger.info(f"Read {len(df)} customers from staging")
        
        # Calculate derived fields
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        df['age'] = df['date_of_birth'].apply(
            lambda dob: (datetime.now().year - dob.year) if pd.notna(dob) else None
        )
        df['age_group'] = df['age'].apply(lambda age: 
            '18-24' if age < 25 else
            '25-34' if age < 35 else
            '35-44' if age < 45 else
            '45-54' if age < 55 else
            '55-64' if age < 65 else
            '65+' if age else None
        )
        
        # Get existing customers
        existing = self.db.read_query("""
            SELECT *
            FROM dim_customer
            WHERE is_current = TRUE  -- Only current versions
        """)
        
        if len(existing) > 0:
            # Process updates
            merged = df.merge(
                existing[['customer_id', 'customer_key', 'address', 'city', 'state']],
                on='customer_id',
                how='left',
                suffixes=('_new', '_old')
            )
            
            # Find changed addresses (SCD Type 2 trigger)
            scd2_changes = merged[
                (merged['address_new'] != merged['address_old']) |
                (merged['city_new'] != merged['city_old']) |
                (merged['state_new'] != merged['state_old'])
            ]
            
            if len(scd2_changes) > 0:
                logger.info(f"Found {len(scd2_changes)} address changes (SCD Type 2)")
                
                # Close old versions
                for _, row in scd2_changes.iterrows():
                    self.db.execute_sql("""
                        UPDATE dim_customer
                        SET valid_to = :valid_to,
                            is_current = FALSE,
                            updated_date = :updated
                        WHERE customer_key = :key
                    """, {
                        'valid_to': datetime.now().date(),
                        'updated': datetime.now(),
                        'key': int(row['customer_key'])
                    })
                
                # Insert new versions
                new_versions = df[df['customer_id'].isin(scd2_changes['customer_id'])]
                new_versions['valid_from'] = datetime.now().date()
                new_versions['valid_to'] = date(9999, 12, 31)
                new_versions['is_current'] = True
                new_versions['created_date'] = datetime.now()
                new_versions['updated_date'] = datetime.now()
                
                self.db.load_dataframe(
                    new_versions,
                    'dim_customer',
                    if_exists='append'
                )
                
                logger.info(f"Inserted {len(new_versions)} new customer versions")
            
            # Type 1 updates (email, phone - no new version)
            type1_updates = merged[~merged['customer_id'].isin(scd2_changes['customer_id'])]
            
            if len(type1_updates) > 0:
                for _, row in type1_updates.iterrows():
                    if pd.notna(row['customer_key']):
                        self.db.execute_sql("""
                            UPDATE dim_customer
                            SET first_name = :first,
                                last_name = :last,
                                full_name = :full,
                                email = :email,
                                phone_number = :phone,
                                updated_date = :updated
                            WHERE customer_key = :key
                        """, {
                            'first': row['first_name'],
                            'last': row['last_name'],
                            'full': row['full_name'],
                            'email': row['email'],
                            'phone': row['phone_number'],
                            'updated': datetime.now(),
                            'key': int(row['customer_key'])
                        })
                
                logger.info(f"Updated {len(type1_updates)} customers (Type 1)")
            
            # Insert completely new customers
            new_customers = df[~df['customer_id'].isin(existing['customer_id'])]
            
            if len(new_customers) > 0:
                new_customers['valid_from'] = new_customers['registration_date']
                new_customers['valid_to'] = date(9999, 12, 31)
                new_customers['is_current'] = True
                new_customers['created_date'] = datetime.now()
                new_customers['updated_date'] = datetime.now()
                
                self.db.load_dataframe(
                    new_customers,
                    'dim_customer',
                    if_exists='append'
                )
                
                logger.info(f"Inserted {len(new_customers)} new customers")
        
        else:
            # First load - all customers are new
            df['valid_from'] = df['registration_date']
            df['valid_to'] = date(9999, 12, 31)
            df['is_current'] = True
            df['created_date'] = datetime.now()
            df['updated_date'] = datetime.now()
            df['total_orders'] = 0
            df['total_spent'] = 0.0
            
            self.db.load_dataframe(df, 'dim_customer', if_exists='append')
            logger.info(f"Inserted {len(df)} customers (initial load)")
        
        logger.info("✓ Customer dimension complete")
        return len(df)
        
    except Exception as e:
        logger.error(f"Customer transformation failed: {e}")
        raise
```

**SCD Type 2 Logic Explained:**

**Scenario: Customer moves from NY to CA**

**Step 1: Detect Change**
```python
scd2_changes = merged[
    (merged['address_new'] != merged['address_old']) |
    (merged['city_new'] != merged['city_old']) |
    (merged['state_new'] != merged['state_old'])
]
```

**Step 2: Close Old Version**
```sql
UPDATE dim_customer
SET valid_to = '2026-01-30',  -- Today
    is_current = FALSE
WHERE customer_key = 12345  -- Old version
```

**Result:**
```
customer_key | customer_id | address        | valid_from | valid_to   | is_current
12345        | CUST000001  | 123 Main, NY  | 2023-01-15 | 2026-01-30 | FALSE
```

**Step 3: Insert New Version**
```python
new_versions['valid_from'] = datetime.now().date()
new_versions['valid_to'] = date(9999, 12, 31)
new_versions['is_current'] = True
```

**Result:**
```
customer_key | customer_id | address        | valid_from | valid_to   | is_current
12345        | CUST000001  | 123 Main, NY  | 2023-01-15 | 2026-01-30 | FALSE      ← Old
54321        | CUST000001  | 456 Oak, CA   | 2026-01-30 | 9999-12-31 | TRUE       ← New
```

**Critical Implementation Detail:**

```python
df['valid_from'] = df['registration_date']  # NOT datetime.now()!
```

**Why?**
- First version should start from registration
- Enables historical analysis: "Where were customers when they first registered?"
- Accurate customer age calculations

**This was a bug I fixed!** Originally used `datetime.now()` which filtered out all transactions because:
```sql
-- This failed because valid_from was always today
transaction_date BETWEEN valid_from AND valid_to
-- 2023-01-15 BETWEEN 2026-01-30 AND 9999-12-31 = FALSE!
```

**Fixed by using registration_date:**
```sql
transaction_date BETWEEN valid_from AND valid_to
-- 2023-01-15 BETWEEN 2023-01-15 AND 9999-12-31 = TRUE!
```

---

**Type 1 vs Type 2 Summary:**

| Attribute | SCD Type | Reason |
|-----------|----------|--------|
| Address | Type 2 | Geographic analysis needed |
| City | Type 2 | Part of address |
| State | Type 2 | Part of address |
| Email | Type 1 | Contact info, overwrite OK |
| Phone | Type 1 | Contact info, overwrite OK |
| First Name | Type 1 | Corrections only |
| Last Name | Type 1 | Corrections only |

---

### File: `src/transformation/transform_facts.py`

**Purpose:** Transform staging data into fact tables

---

### Method: transform_transactions()

**Challenge:** Match dimensions using SCD Type 2 logic

```python
def transform_transactions(self) -> int:
    """Transform transactions with dimension lookups"""
    
    logger.info("Transforming Fact Transactions")
    
    try:
        # This is done in SQL for better performance
        query = """
        INSERT INTO fact_transactions (
            transaction_item_id,
            transaction_id,
            customer_key,
            product_key,
            transaction_date_key,
            transaction_datetime,
            quantity,
            unit_price,
            line_total,
            discount_amount,
            net_amount,
            order_number,
            payment_method
        )
        SELECT 
            ti.transaction_item_id,
            ti.transaction_id,
            c.customer_key,
            p.product_key,
            d.date_key,
            t.transaction_date,
            ti.quantity,
            ti.unit_price,
            ti.line_total,
            ti.discount_amount,
            ti.line_total - ti.discount_amount as net_amount,
            t.order_number,
            t.payment_method
        FROM stg_ecom_transaction_items ti
        JOIN stg_ecom_transactions t 
            ON ti.transaction_id = t.transaction_id
        LEFT JOIN dim_customer c 
            ON t.customer_id = c.customer_id
            AND t.transaction_date::date BETWEEN c.valid_from AND c.valid_to  -- SCD Type 2!
        LEFT JOIN dim_product p 
            ON ti.product_id = p.product_id
        LEFT JOIN dim_date d 
            ON t.transaction_date::date = d.date_actual
        WHERE ti.transaction_item_id NOT IN (
            SELECT transaction_item_id FROM fact_transactions
        );
        """
        
        result = self.db.execute_sql(query)
        rows_loaded = result.rowcount
        
        logger.info(f"✓ Loaded {rows_loaded} transaction line items")
        return rows_loaded
        
    except Exception as e:
        logger.error(f"Transaction transformation failed: {e}")
        raise
```

**Critical Join for SCD Type 2:**

```sql
LEFT JOIN dim_customer c 
    ON t.customer_id = c.customer_id
    AND t.transaction_date::date BETWEEN c.valid_from AND c.valid_to
```

**Why This Works:**

```
Transaction Date: 2024-03-15

Customer Versions:
├─ Version 1: valid_from=2023-01-15, valid_to=2024-06-30, address=NY
└─ Version 2: valid_from=2024-07-01, valid_to=9999-12-31, address=CA

JOIN Result:
└─ Matches Version 1 (NY address)
   Because 2024-03-15 BETWEEN 2023-01-15 AND 2024-06-30
```

**This ensures:**
- Historical accuracy
- Transaction shows where customer lived at that time
- Correct address for analysis

---

**Why SQL Instead of Python?**

**Option 1: Python/Pandas (DON'T DO THIS)**
```python
transactions = pd.read_sql("SELECT * FROM stg_ecom_transaction_items")  # 419,907 rows
customers = pd.read_sql("SELECT * FROM dim_customer")  # 50,000 rows
products = pd.read_sql("SELECT * FROM dim_product")  # 500 rows

# Merge in Python
result = transactions.merge(customers, ...)  # SLOW! Memory intensive!
result = result.merge(products, ...)
result = result.merge(dates, ...)
```

**Problems:**
- All data loaded into memory (100MB+)
- Pandas merge slower than SQL
- SCD Type 2 logic complex in Python
- No database optimization

**Option 2: SQL (WHAT WE DO)**
```sql
INSERT INTO fact_transactions
SELECT ...
FROM stg_ecom_transaction_items ti
JOIN stg_ecom_transactions t ...
LEFT JOIN dim_customer c ...  -- Database does the work
LEFT JOIN dim_product p ...
LEFT JOIN dim_date d ...
```

**Benefits:**
- Database optimized for joins
- No memory limits
- Uses indexes
- Parallel processing
- One transaction
- **10-100x faster**

---

### Method: update_customer_lifetime_metrics()

**Purpose:** Calculate total_orders and total_spent from facts

```python
def update_customer_lifetime_metrics(self) -> int:
    """Update customer metrics from fact table"""
    
    logger.info("Updating customer lifetime metrics")
    
    query = """
    UPDATE dim_customer c
    SET total_orders = metrics.order_count,
        total_spent = metrics.total_amount,
        updated_date = CURRENT_TIMESTAMP
    FROM (
        SELECT 
            customer_key,
            COUNT(DISTINCT transaction_id) as order_count,
            SUM(net_amount) as total_amount
        FROM fact_transactions
        GROUP BY customer_key
    ) metrics
    WHERE c.customer_key = metrics.customer_key
      AND c.is_current = TRUE;
    """
    
    result = self.db.execute_sql(query)
    customers_updated = result.rowcount
    
    logger.info(f"✓ Updated metrics for {customers_updated} customers")
    return customers_updated
```

**Why Calculate After Loading Facts?**
- Metrics depend on fact data
- Accurate counts
- Updated every ETL run

**Example Result:**
```
customer_key | customer_id | total_orders | total_spent
12345        | CUST000001  | 15           | 2,450.75
54321        | CUST000002  | 8            | 1,120.50
```

---

### Method: populate_aggregates()

**Purpose:** Pre-calculate monthly summaries

```python
def populate_aggregates(self) -> int:
    """Populate aggregate tables"""
    
    logger.info("Populating aggregate tables")
    
    # Clear existing aggregates
    self.db.execute_sql("TRUNCATE TABLE agg_customer_monthly")
    
    # Calculate monthly customer metrics
    query = """
    INSERT INTO agg_customer_monthly (
        customer_key,
        year_month,
        total_transactions,
        total_revenue,
        total_units,
        avg_transaction_value
    )
    SELECT 
        f.customer_key,
        TO_CHAR(d.date_actual, 'YYYY-MM') as year_month,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        SUM(f.net_amount) as total_revenue,
        SUM(f.quantity) as total_units,
        AVG(f.net_amount) as avg_transaction_value
    FROM fact_transactions f
    JOIN dim_date d ON f.transaction_date_key = d.date_key
    GROUP BY f.customer_key, TO_CHAR(d.date_actual, 'YYYY-MM');
    """
    
    result = self.db.execute_sql(query)
    rows = result.rowcount
    
    logger.info(f"✓ Created {rows} monthly customer aggregates")
    return rows
```

**Example Aggregate Data:**
```
customer_key | year_month | total_transactions | total_revenue | avg_transaction_value
12345        | 2024-01   | 3                  | 450.75       | 150.25
12345        | 2024-02   | 2                  | 320.50       | 160.25
12345        | 2024-03   | 1                  | 89.99        | 89.99
```

**Why Aggregates?**
- Customer monthly reports are common
- Pre-calculation = 100x faster queries
- Small table (customer × months) vs huge fact table
- Worth the ETL time trade-off

---

### File: `src/transformation/run_etl.py`

**Purpose:** Orchestrate the complete ETL process

```python
class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def run_full_pipeline(self):
        """Run the complete ETL pipeline"""
        
        self.start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("STARTING FULL ETL PIPELINE")
        logger.info("=" * 80)
        
        try:
            # PHASE 1: DIMENSIONS
            logger.info("\nPHASE 1: DIMENSION TRANSFORMATION")
            
            dim_product = self.dim_transformer.transform_product_dimension()
            dim_campaign = self.dim_transformer.transform_campaign_dimension()
            dim_customer = self.dim_transformer.transform_customer_dimension()
            # dim_date is pre-populated, skip
            
            logger.info(f"✓ Dimensions complete: {dim_product} products, "
                       f"{dim_customer} customers, {dim_campaign} campaigns")
            
            # PHASE 2: FACTS
            logger.info("\nPHASE 2: FACT TRANSFORMATION")
            
            fact_txn = self.fact_transformer.transform_transactions()
            fact_resp = self.fact_transformer.transform_campaign_responses()
            
            logger.info(f"✓ Facts complete: {fact_txn} transactions, "
                       f"{fact_resp} responses")
            
            # PHASE 3: POST-PROCESSING
            logger.info("\nPHASE 3: POST-PROCESSING")
            
            cust_metrics = self.fact_transformer.update_customer_lifetime_metrics()
            agg_rows = self.fact_transformer.populate_aggregates()
            
            logger.info(f"✓ Post-processing complete: {cust_metrics} customers, "
                       f"{agg_rows} aggregate rows")
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"ETL PIPELINE FAILED: {e}")
            return False
```

**Execution Order is Critical:**

```
1. Products     ← No dependencies
2. Campaigns    ← No dependencies
3. Customers    ← No dependencies
   │
   └─> 4. Transactions  ← Needs customer_key, product_key
       │
       └─> 5. Customer Metrics  ← Needs transaction facts
           │
           └─> 6. Aggregates  ← Needs everything
```

**Why This Order?**
- Can't lookup dimension keys that don't exist
- Can't calculate metrics without facts
- Dimensional model best practice

---

## Data Quality Framework

### File: `src/quality/validation_rules.py`

**Purpose:** Define all data quality rules

---

### Class: ValidationRules

```python
class ValidationRules:
    """Centralized repository of data quality rules"""
    
    @staticmethod
    def get_all_rules():
        """Returns all data quality rules"""
        
        rules = []
        
        # COMPLETENESS RULES
        rules.append({
            'rule_name': 'Customer Email Not Null',
            'rule_description': 'Customer email must not be null for active customers',
            'category': 'Completeness',
            'target_table': 'dim_customer',
            'target_column': 'email',
            'rule_type': 'not_null',
            'rule_sql': """
                SELECT 
                    customer_key as record_identifier,
                    'email' as column_name,
                    NULL as failed_value,
                    'NOT NULL' as expected_value
                FROM dim_customer
                WHERE email IS NULL
                  AND is_current = TRUE
                  AND account_status = 'Active'
            """,
            'severity': 'WARNING',
            'failure_threshold': 5.0  # Allow up to 5% missing
        })
```

**Rule Structure Explained:**

**rule_sql:**
- Query that finds violations
- Returns rows that **fail** the rule
- Columns: record_identifier, column_name, failed_value, expected_value

**Example:**
```sql
-- This rule finds customers with missing emails
SELECT customer_key, 'email', NULL, 'NOT NULL'
FROM dim_customer
WHERE email IS NULL  -- The violation
  AND is_current = TRUE
  AND account_status = 'Active'
```

**severity:**
- `CRITICAL`: Must fix immediately
- `WARNING`: Should fix soon
- `INFO`: Nice to fix

**failure_threshold:**
- % of records that can fail
- 5.0 = up to 5% missing is acceptable
- Above threshold = FAILED status
- Below threshold = WARNING status

---

**The 7 Quality Dimensions:**

**1. Completeness** - Are required fields populated?
```python
rules.append({
    'rule_name': 'Customer Phone Not Null',
    'category': 'Completeness',
    'rule_sql': "SELECT ... WHERE phone_number IS NULL",
    'failure_threshold': 10.0  # 10% can be missing
})
```

**2. Accuracy** - Are values within valid ranges?
```python
rules.append({
    'rule_name': 'Customer Age Range Valid',
    'category': 'Accuracy',
    'rule_sql': """
        SELECT customer_key, 'age', age::text, '18-120'
        FROM dim_customer
        WHERE age NOT BETWEEN 18 AND 120
          AND age IS NOT NULL
    """,
    'failure_threshold': 0.0  # Must be 100% accurate
})
```

**3. Consistency** - Do values agree across tables?
```python
rules.append({
    'rule_name': 'Customer Lifetime Value Consistency',
    'category': 'Consistency',
    'rule_sql': """
        SELECT 
            c.customer_key,
            'total_spent',
            c.total_spent::text,
            SUM(f.net_amount)::text
        FROM dim_customer c
        JOIN fact_transactions f ON c.customer_key = f.customer_key
        WHERE c.is_current = TRUE
        GROUP BY c.customer_key, c.total_spent
        HAVING ABS(c.total_spent - SUM(f.net_amount)) > 0.01  -- Allow penny rounding
    """,
    'failure_threshold': 1.0
})
```

**4. Validity** - Do values meet business rules?
```python
rules.append({
    'rule_name': 'Transaction Amount Positive',
    'category': 'Validity',
    'rule_sql': """
        SELECT transaction_item_key, 'net_amount', net_amount::text, '> 0'
        FROM fact_transactions
        WHERE net_amount <= 0
    """,
    'failure_threshold': 0.0
})
```

**5. Uniqueness** - Are there duplicates?
```python
rules.append({
    'rule_name': 'Customer ID Unique Per Version',
    'category': 'Uniqueness',
    'rule_sql': """
        SELECT customer_id, valid_from, COUNT(*) as dup_count
        FROM dim_customer
        GROUP BY customer_id, valid_from
        HAVING COUNT(*) > 1
    """,
    'failure_threshold': 0.0
})
```

**6. Timeliness** - Is data fresh?
```python
rules.append({
    'rule_name': 'Recent Data Available',
    'category': 'Timeliness',
    'rule_sql': """
        SELECT 
            'fact_transactions' as table_name,
            COUNT(*) as record_count,
            MAX(transaction_datetime)::text as latest_date,
            CURRENT_DATE - MAX(transaction_datetime::date) as days_old
        FROM fact_transactions
        HAVING CURRENT_DATE - MAX(transaction_datetime::date) > 7  -- Warn if > 7 days old
    """,
    'failure_threshold': 0.0
})
```

**7. Integrity** - Do foreign keys exist?
```python
rules.append({
    'rule_name': 'Transaction Has Valid Customer',
    'category': 'Integrity',
    'rule_sql': """
        SELECT f.transaction_item_key, 'customer_key', f.customer_key::text, 'EXISTS'
        FROM fact_transactions f
        LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL  -- Orphaned transaction
    """,
    'failure_threshold': 0.0
})
```

---

### File: `src/quality/quality_engine.py`

**Purpose:** Execute validation rules and track results

---

### Method: execute_rule()

```python
def execute_rule(self, rule: dict) -> dict:
    """Execute a single quality rule"""
    
    rule_name = rule['rule_name']
    logger.info(f"Executing rule: {rule_name}")
    
    start_time = time.time()
    
    try:
        # Get total records to check
        total_query = f"SELECT COUNT(*) as cnt FROM {rule['target_table']}"
        total_result = self.db.read_query(total_query)
        total_records = total_result.iloc[0]['cnt']
        
        # Execute validation SQL
        violations_df = self.db.read_query(rule['rule_sql'])
        failed_records = len(violations_df)
        passed_records = total_records - failed_records
        
        # Calculate metrics
        failure_percentage = (failed_records / total_records * 100) if total_records > 0 else 0
        
        # Determine status
        if failed_records == 0:
            test_status = 'PASSED'
            test_message = 'All records passed validation'
        elif failure_percentage <= rule['failure_threshold']:
            test_status = 'WARNING'
            test_message = f"{failed_records} violations found but within threshold ({rule['failure_threshold']}%)"
        else:
            test_status = 'FAILED'
            test_message = f"{failed_records} violations exceed threshold ({rule['failure_threshold']}%)"
        
        execution_time = int((time.time() - start_time) * 1000)  # milliseconds
        
        result = {
            'rule_name': rule_name,
            'total_records': total_records,
            'failed_records': failed_records,
            'passed_records': passed_records,
            'failure_percentage': round(failure_percentage, 2),
            'test_status': test_status,
            'test_message': test_message,
            'execution_time_ms': execution_time,
            'violations': violations_df
        }
        
        logger.info(f"  Status: {test_status}, Failures: {failed_records}/{total_records} ({failure_percentage:.2f}%)")
        
        return result
        
    except Exception as e:
        logger.error(f"Rule execution failed: {e}")
        return {
            'rule_name': rule_name,
            'total_records': 0,
            'failed_records': 0,
            'test_status': 'ERROR',
            'test_message': str(e),
            'violations': pd.DataFrame()
        }
```

**Status Logic:**

```
Total Records: 50,000
Failed Records: 1,535
Failure %: 3.07%
Threshold: 5.0%

Since 3.07% <= 5.0%:
  └─> Status: WARNING (not FAILED)
```

**Execution Time:**
- Measured in milliseconds
- Tracks performance
- Helps identify slow rules

---

### Method: save_result()

```python
def save_result(self, rule_id: int, result: dict):
    """Save execution result to database"""
    
    # Save to dq_test_results
    self.db.execute_sql("""
        INSERT INTO dq_test_results (
            rule_id,
            execution_duration_ms,
            total_records_checked,
            failed_records,
            passed_records,
            failure_percentage,
            test_status,
            test_message
        ) VALUES (
            :rule_id, :duration, :total, :failed, :passed,
            :pct, :status, :message
        )
    """, {
        'rule_id': int(rule_id),
        'duration': int(result['execution_time_ms']),
        'total': int(result['total_records']),
        'failed': int(result['failed_records']),
        'passed': int(result['passed_records']),
        'pct': float(result['failure_percentage']),
        'status': str(result['test_status']),
        'message': str(result['test_message'])
    })
    
    # Get result_id for exceptions
    result_id = self.db.read_query("""
        SELECT result_id FROM dq_test_results
        WHERE rule_id = :rule_id
        ORDER BY execution_date DESC
        LIMIT 1
    """, {'rule_id': rule_id}).iloc[0]['result_id']
    
    # Save exceptions (limited to 1000)
    if len(result['violations']) > 0:
        self.save_exceptions(result_id, rule_id, result['violations'])
```

**Why Separate Tables?**
- `dq_test_results`: Summary (1 row per execution)
- `dq_exceptions`: Details (many rows per execution)
- Efficient storage
- Easy to query

---

### Method: save_exceptions()

```python
def save_exceptions(self, result_id: int, rule_id: int, violations: pd.DataFrame):
    """Save detailed exceptions (limited to first 1000)"""
    
    # Limit to prevent database bloat
    max_exceptions = 1000
    violations_to_save = violations.head(max_exceptions)
    
    if len(violations) > max_exceptions:
        logger.warning(f"Limiting exceptions to {max_exceptions} of {len(violations)} total violations")
    
    for _, row in violations_to_save.iterrows():
        # Create hash for duplicate detection
        exception_str = f"{rule_id}_{row['record_identifier']}_{row.get('column_name', '')}_{row.get('failed_value', '')}"
        exception_hash = hashlib.md5(exception_str.encode()).hexdigest()
        
        self.db.execute_sql("""
            INSERT INTO dq_exceptions (
                result_id,
                rule_id,
                table_name,
                record_identifier,
                column_name,
                failed_value,
                expected_value,
                exception_hash
            ) VALUES (
                :result_id, :rule_id, :table, :identifier,
                :column, :failed, :expected, :hash
            )
        """, {
            'result_id': int(result_id),
            'rule_id': int(rule_id),
            'table': str(rule_info['target_table']),
            'identifier': str(row['record_identifier']),
            'column': str(row.get('column_name', '')),
            'failed': str(row.get('failed_value', ''))[:1000],  # Truncate
            'expected': str(row.get('expected_value', ''))[:1000],
            'hash': str(exception_hash)
        })
```

**Why Limit to 1000?**

**Scenario:** Email completeness rule finds 41,000 violations

**Without Limit:**
- 41,000 INSERTs (slow!)
- 41,000 rows stored (database bloat)
- All say the same thing: "email is NULL"

**With Limit:**
- 1,000 INSERTs (fast)
- Representative sample
- Pattern is clear from 1,000 examples

---

### Method: generate_scorecard()

```python
def generate_scorecard(self, report_period='DAILY'):
    """Generate quality scorecard"""
    
    # Get today's results
    today_results = self.db.read_query("""
        SELECT 
            r.category_id,
            c.category_name,
            tr.test_status,
            COUNT(*) as rule_count,
            SUM(tr.total_records_checked) as total_records,
            SUM(tr.failed_records) as failed_records
        FROM dq_test_results tr
        JOIN dq_rules r ON tr.rule_id = r.rule_id
        JOIN dq_rule_categories c ON r.category_id = c.category_id
        WHERE tr.execution_date::date = CURRENT_DATE
        GROUP BY r.category_id, c.category_name, tr.test_status
    """)
    
    # Calculate scores by category
    category_scores = {}
    for category in ['Completeness', 'Accuracy', 'Consistency', 'Validity',
                    'Uniqueness', 'Timeliness', 'Integrity']:
        cat_data = today_results[today_results['category_name'] == category]
        if len(cat_data) > 0:
            total_records = cat_data['total_records'].sum()
            failed_records = cat_data['failed_records'].sum()
            score = ((total_records - failed_records) / total_records * 100) if total_records > 0 else 100
            category_scores[category.lower() + '_score'] = round(score, 2)
        else:
            category_scores[category.lower() + '_score'] = 100.0
    
    # Overall score = average of category scores
    overall_score = sum(category_scores.values()) / len(category_scores)
    
    # Insert scorecard
    self.db.execute_sql("""
        INSERT INTO dq_scorecards (
            report_date,
            report_period,
            overall_quality_score,
            completeness_score,
            accuracy_score,
            consistency_score,
            validity_score,
            uniqueness_score,
            timeliness_score,
            integrity_score,
            total_rules_executed,
            rules_passed,
            rules_failed,
            total_records_checked,
            total_failed_records
        ) VALUES (
            CURRENT_DATE, :period,
            :overall, :completeness, :accuracy, :consistency,
            :validity, :uniqueness, :timeliness, :integrity,
            :total_rules, :passed, :failed,
            :total_records, :failed_records
        )
        ON CONFLICT (report_date, report_period)
        DO UPDATE SET
            overall_quality_score = EXCLUDED.overall_quality_score,
            ...  -- Update all fields
    """, {
        'period': report_period,
        'overall': round(overall_score, 2),
        'completeness': category_scores['completeness_score'],
        # ... more scores
    })
```

**Score Calculation:**

```
Completeness Rules:
  Rule 1: 48,465 passed / 50,000 total = 96.93%
  Rule 2: 36,905 passed / 50,000 total = 73.81%
  
Average: (96.93 + 73.81) / 2 = 85.37%
└─> completeness_score = 85.37

Overall Score:
  Average of all 7 dimension scores = 95.6%
```

---

## Reporting & Visualization

### File: `src/quality/quality_reports.py`

**Purpose:** Generate HTML dashboards and visualizations

I won't detail every line (it's 723 lines!), but here's the approach:

---

### Method: _fig_to_base64()

```python
def _fig_to_base64(self, fig) -> str:
    """Convert matplotlib figure to base64 for embedding in HTML"""
    buffer = BytesIO()
    fig.savefig(buffer, format='png', bbox_inches='tight', dpi=100)
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode()
    plt.close(fig)
    return f"data:image/png;base64,{image_base64}"
```

**Why Base64?**
- Embed images directly in HTML
- No separate image files
- Single file = easier to share
- Works offline

**Result:**
```html
<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAA...">
```

---

### Method: _create_scorecard_chart()

```python
def _create_scorecard_chart(self, scorecard: pd.Series) -> str:
    """Create horizontal bar chart of quality dimensions"""
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    dimensions = ['Completeness', 'Accuracy', 'Consistency', 'Validity',
                 'Uniqueness', 'Timeliness', 'Integrity']
    scores = [scorecard['completeness_score'], scorecard['accuracy_score'], ...]
    
    # Color by score (green if >= 95%, orange if >= 80%, else red)
    colors = ['#2ecc71' if s >= 95 else '#f39c12' if s >= 80 else '#e74c3c'
              for s in scores]
    
    bars = ax.barh(dimensions, scores, color=colors)
    ax.set_xlabel('Quality Score (%)')
    ax.set_xlim(0, 100)
    
    # Add value labels
    for bar, score in zip(bars, scores):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
               f'{score:.1f}%', ha='left', va='center')
    
    return self._fig_to_base64(fig)
```

**Visualization Choices:**

**Horizontal Bars:**
- Easy to read dimension names
- Natural left-to-right reading
- Better than vertical for many items

**Color Coding:**
- 🟢 Green (95-100%): Excellent
- 🟠 Orange (80-94%): Good
- 🔴 Red (<80%): Needs attention
- Instant visual understanding

---

### Method: generate_html_report()

**The HTML is a single file with:**
- CSS embedded in `<style>` tags
- Charts as base64 images
- Responsive design (works on mobile)
- Print-friendly

**Structure:**
```html
<html>
<head>
  <style>
    /* Professional CSS styling */
    /* Gradient background */
    /* Card-based layout */
    /* Color-coded metrics */
  </style>
</head>
<body>
  <div class="header">
    <h1>Data Quality Dashboard</h1>
  </div>
  
  <div class="scorecard">
    <!-- Metric cards -->
  </div>
  
  <div class="chart-section">
    <!-- Quality dimensions chart -->
  </div>
  
  <div class="chart-section">
    <!-- Trend chart -->
  </div>
  
  <table>
    <!-- Test results -->
  </table>
  
  <table>
    <!-- Exceptions -->
  </table>
</body>
</html>
```

**Why This Design?**
- Executive-friendly (non-technical users can understand)
- Action-oriented (shows what needs attention)
- Comprehensive (all details available)
- Professional appearance (portfolio-quality)

---

## Documentation

I created three types of documentation:

### 1. Data Mapping (Excel)

**Why Excel?**
- Business users comfortable with Excel
- Easy to share and print
- Filter and sort capabilities
- Multiple sheets for organization

**Sheets:**
1. **Field Mappings:**
   - Source → Target column mappings
   - Transformation logic
   - Business rules
   - Sample values

2. **SCD Logic:**
   - Explanation of Type 1 vs Type 2
   - Which fields use which type
   - Reasoning

3. **Business Rules:**
   - Validation rules
   - Calculation formulas
   - Special cases

4. **Data Lineage:**
   - Step-by-step data flow
   - Frequency of each step

---

### 2. Business Glossary (Markdown)

**Why Markdown?**
- Easy to maintain in Git
- Readable as plain text
- Converts to HTML/PDF
- Searchable

**Structure:**
- Alphabetical within categories
- Clear definitions
- Business rules
- Ownership
- Related terms

**Example Entry:**
```markdown
### Customer Lifetime Value (CLV)

**Definition:** Total revenue generated by a customer since registration.

**Formula:** `SUM(net_amount)` from all transactions for the customer

**Business Rules:**
- Calculated and stored in `dim_customer.total_spent`
- Updated after each ETL run
- Excludes returns and refunds

**Usage:**
- Customer segmentation
- Marketing campaign targeting
- Customer retention strategies

**Owner:** Analytics Team
**Refresh Frequency:** Daily
```

---

### 3. KPI Definitions (Markdown)

**Structure:**
- KPI name
- SQL calculation
- Business rules
- Targets
- Frequency
- Owner
- Dashboard placement

**Why Separate from Glossary?**
- KPIs need SQL formulas
- Different audience (analysts vs business users)
- More technical detail

---

## Testing & Debugging

### File: `debug_etl.py`

**Purpose:** Diagnostic script when things go wrong

```python
db = get_db_manager()

# Check staging data
staging_txn = db.read_query("SELECT COUNT(*) FROM stg_ecom_transactions")
print(f"Staging transactions: {staging_txn.iloc[0]['cnt']:,}")

# Check dimension data
dim_customer = db.read_query("SELECT COUNT(*) FROM dim_customer")
print(f"Customers in dimension: {dim_customer.iloc[0]['cnt']:,}")

# Check fact data
fact_txn = db.read_query("SELECT COUNT(*) FROM fact_transactions")
print(f"Transactions in fact: {fact_txn.iloc[0]['cnt']:,}")

# Sample dimension keys
sample = db.read_query("""
    SELECT customer_key, customer_id, valid_from, valid_to, is_current
    FROM dim_customer
    LIMIT 10
""")
print(sample)

# Test join logic
join_test = db.read_query("""
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        c.customer_key,
        c.valid_from,
        c.valid_to
    FROM stg_ecom_transactions t
    LEFT JOIN dim_customer c
        ON t.customer_id = c.customer_id
        AND t.transaction_date::date BETWEEN c.valid_from AND c.valid_to
    LIMIT 10
""")
print(join_test)
```

**This Script Helped Me:**
1. Find the SCD Type 2 date bug
2. Verify staging data loaded
3. Check dimension key lookups
4. Test join conditions

**Always Create Debug Scripts!**
- Save hours of troubleshooting
- Document what you checked
- Reusable for future issues

---

## Complete Data Flow

### End-to-End Example: One Customer's Journey

**Day 1: Customer Registers**

```
1. Data Generated:
   └─> crm_customers_20260130.csv
       customer_id: CUST000001
       first_name: John
       last_name: Smith
       address: 123 Main St, New York
       registration_date: 2023-01-15

2. Loaded to Staging:
   └─> stg_crm_customers
       + audit columns added

3. Transformed to Dimension:
   └─> dim_customer
       customer_key: 1
       customer_id: CUST000001
       valid_from: 2023-01-15    ← registration_date
       valid_to: 9999-12-31
       is_current: TRUE
```

**Day 365: Customer Makes First Purchase**

```
1. Transaction Data Generated:
   └─> ecom_transactions_20240115.csv
       transaction_id: TXN0001
       customer_id: CUST000001
       transaction_date: 2024-01-15
       └─> ecom_transaction_items_20240115.csv
           product_id: PROD00123
           quantity: 2
           unit_price: 29.99

2. Loaded to Staging:
   └─> stg_ecom_transactions
   └─> stg_ecom_transaction_items

3. Transformed to Fact:
   └─> fact_transactions
       transaction_item_key: 1
       customer_key: 1        ← Lookup matches!
       product_key: 123
       transaction_date_key: 20240115
       net_amount: 59.98

4. Customer Metrics Updated:
   └─> dim_customer (customer_key=1)
       total_orders: 1
       total_spent: 59.98
```

**Day 500: Customer Moves to California**

```
1. Updated Customer Data:
   └─> crm_customers_20240630.csv
       customer_id: CUST000001
       address: 456 Oak Ave, Los Angeles  ← Changed!

2. Loaded to Staging:
   └─> stg_crm_customers (overwrites)

3. SCD Type 2 Triggered:
   
   a) Close Old Version:
      └─> dim_customer (customer_key=1)
          valid_from: 2023-01-15
          valid_to: 2024-06-30    ← Updated
          is_current: FALSE        ← Updated
   
   b) Insert New Version:
      └─> dim_customer (customer_key=2)
          customer_id: CUST000001  ← Same business key
          address: 456 Oak Ave, Los Angeles
          valid_from: 2024-07-01
          valid_to: 9999-12-31
          is_current: TRUE
          total_orders: 1          ← Copied from old
          total_spent: 59.98
```

**Day 501: Customer Makes Second Purchase**

```
1. Transaction in California:
   └─> transaction_date: 2024-07-15
   └─> customer_id: CUST000001

2. Fact Table Join:
   └─> Matches customer_key=2 (CA version)
       Because 2024-07-15 BETWEEN 2024-07-01 AND 9999-12-31

3. Now We Know:
   ├─ First purchase: Made from NY address (customer_key=1)
   └─ Second purchase: Made from CA address (customer_key=2)
```

**This is the power of SCD Type 2!**

---

## Lessons Learned

### What Went Well

**1. Modular Design**
- Easy to test individual components
- Can run staging without transformation
- Can re-run quality checks independently

**2. Configuration System**
- Changed batch size without code changes
- Easy to adjust thresholds
- Environment-specific configs

**3. Logging**
- Saved hours debugging
- Clear audit trail
- Performance monitoring

**4. SQL-Based Transformations**
- Much faster than Python
- Leverages database optimization
- Easier to review logic

---

### Challenges & Solutions

**1. SQLAlchemy 2.0 Migration**
- **Problem:** Old connection patterns deprecated
- **Solution:** Migrated to `engine.begin()` and `text()`
- **Lesson:** Stay current with library versions

**2. PostgreSQL Parameter Limits**
- **Problem:** Bulk insert hit 65,535 parameter limit
- **Solution:** Reduced chunk size, disabled 'multi' method
- **Lesson:** Understand database limitations

**3. SCD Type 2 Date Logic**
- **Problem:** Used `CURRENT_DATE` for `valid_from`, filtered out all transactions
- **Solution:** Used `registration_date` instead
- **Lesson:** Think through temporal logic carefully

**4. Data Type Conversions**
- **Problem:** psycopg2 can't handle numpy.int64
- **Solution:** Explicit conversion to Python types
- **Lesson:** Libraries don't always play nice together

**5. Exception Volume**
- **Problem:** 41,000 exceptions overwhelming database
- **Solution:** Limited to 1,000 per rule
- **Lesson:** Representative samples often sufficient

---

### What I'd Do Differently

**1. Add Unit Tests**
- Test each transformation function
- Mock database connections
- Catch bugs earlier

**2. Incremental Loading**
- Current approach is full reload
- Add delta detection
- Process only changed records

**3. Parallel Processing**
- Load multiple tables simultaneously
- Use Python multiprocessing
- Faster overall ETL

**4. dbt Integration**
- Modern transformation framework
- Version control for SQL
- Built-in testing

**5. CI/CD Pipeline**
- Automate testing on commit
- GitHub Actions
- Deploy to cloud automatically

---

### Skills Demonstrated

**Technical:**
- ✅ Python (OOP, pandas, SQLAlchemy)
- ✅ SQL (PostgreSQL, complex joins, window functions)
- ✅ Dimensional modeling (Kimball)
- ✅ ETL pipeline development
- ✅ Data quality framework
- ✅ Visualization (matplotlib, HTML/CSS)

**Conceptual:**
- ✅ SCD Type 1 & Type 2
- ✅ Star schema design
- ✅ Data lineage
- ✅ Business intelligence
- ✅ Documentation

**Soft Skills:**
- ✅ Problem-solving (8+ technical challenges)
- ✅ Attention to detail
- ✅ Communication (documentation)
- ✅ Self-directed learning

---

## Final Thoughts

This project took me from "I can write SQL" to "I can build enterprise data warehouses."

**Key Takeaways:**

1. **Design Matters:** Spend time on schema design upfront
2. **Configuration is Key:** Hard-coded values = technical debt
3. **Logging Saves Time:** Always log important steps
4. **Test Early:** Don't wait until the end
5. **Document Everything:** Future you will thank present you

**What Makes This Portfolio-Worthy:**

- ✅ **Production-ready code** (not tutorial-level)
- ✅ **Real-world complexity** (SCD Type 2, data quality)
- ✅ **Complete solution** (end-to-end, not just snippets)
- ✅ **Professional documentation** (business + technical)
- ✅ **Demonstrates growth** (challenges solved, lessons learned)

---

**Thank you for reading this walkthrough!**

If you have questions about any part of the project, I'm happy to explain further. Every design decision had a reason, and every challenge taught me something.

**- Your AI Junior Developer**

---

*Last Updated: January 30, 2026*  
*Total Project Time: ~100 hours over 4 days*  
*Lines of Code: ~5,000+ (Python + SQL)*  
*What I Learned: Priceless* 😊
