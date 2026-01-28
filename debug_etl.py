"""
Debug ETL - Check why transactions aren't loading
"""

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent / 'src'))

from utils.db_manager import get_db_manager
import pandas as pd

db = get_db_manager()

print("=" * 80)
print("DEBUGGING ETL ISSUE")
print("=" * 80)

# Check staging data
print("\n1. Staging Data Check:")
staging_txn = db.read_query("SELECT COUNT(*) as cnt FROM stg_ecom_transactions")
staging_items = db.read_query("SELECT COUNT(*) as cnt FROM stg_ecom_transaction_items")
print(f"   Staging transactions: {staging_txn.iloc[0]['cnt']:,}")
print(f"   Staging transaction items: {staging_items.iloc[0]['cnt']:,}")

# Check dimension data
print("\n2. Dimension Data Check:")
dim_customer = db.read_query("SELECT COUNT(*) as cnt FROM dim_customer")
dim_product = db.read_query("SELECT COUNT(*) as cnt FROM dim_product")
print(f"   Customers in dimension: {dim_customer.iloc[0]['cnt']:,}")
print(f"   Products in dimension: {dim_product.iloc[0]['cnt']:,}")

# Sample customer IDs from staging
print("\n3. Sample Customer IDs from Staging:")
sample_cust_staging = db.read_query("""
    SELECT DISTINCT customer_id 
    FROM stg_ecom_transactions 
    LIMIT 5
""")
print(sample_cust_staging)

# Sample customer IDs from dimension
print("\n4. Sample Customer IDs from Dimension:")
sample_cust_dim = db.read_query("""
    SELECT customer_id, valid_from, valid_to, is_current
    FROM dim_customer 
    LIMIT 5
""")
print(sample_cust_dim)

# Sample product IDs
print("\n5. Sample Product IDs from Staging:")
sample_prod_staging = db.read_query("""
    SELECT DISTINCT product_id 
    FROM stg_ecom_transaction_items 
    LIMIT 5
""")
print(sample_prod_staging)

print("\n6. Sample Product IDs from Dimension:")
sample_prod_dim = db.read_query("""
    SELECT product_id 
    FROM dim_product 
    LIMIT 5
""")
print(sample_prod_dim)

# Sample transaction dates
print("\n7. Sample Transaction Dates:")
sample_dates = db.read_query("""
    SELECT 
        MIN(transaction_date) as min_date,
        MAX(transaction_date) as max_date
    FROM stg_ecom_transactions
""")
print(sample_dates)

# Check if any joins would work
print("\n8. Test Join (should show matches):")
test_join = db.read_query("""
    SELECT COUNT(*) as match_count
    FROM stg_ecom_transactions t
    INNER JOIN dim_customer c ON t.customer_id = c.customer_id
    WHERE c.is_current = TRUE
    LIMIT 1
""")
print(f"   Transactions matching customers (ignoring dates): {test_join.iloc[0]['match_count']:,}")

# Check date range join
print("\n9. Test Date Range Join:")
test_date_join = db.read_query("""
    SELECT COUNT(*) as match_count
    FROM stg_ecom_transactions t
    INNER JOIN dim_customer c 
        ON t.customer_id = c.customer_id
        AND t.transaction_date::date BETWEEN c.valid_from AND c.valid_to
    LIMIT 1
""")
print(f"   Transactions matching with date range: {test_date_join.iloc[0]['match_count']:,}")

# Show specific example
print("\n10. Specific Example (one transaction):")
example = db.read_query("""
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        c.customer_id as dim_customer_id,
        c.valid_from,
        c.valid_to,
        c.is_current
    FROM stg_ecom_transactions t
    LEFT JOIN dim_customer c ON t.customer_id = c.customer_id
    LIMIT 1
""")
print(example)

print("\n" + "=" * 80)
print("DEBUG COMPLETE")
print("=" * 80)