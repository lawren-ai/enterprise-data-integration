"""
Data Quality Validation Rules
Defines all quality rules for the data warehouse
"""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger(__name__)


class ValidationRules:
    """Centralized repository of data quality rules"""
    
    @staticmethod
    def get_all_rules():
        """
        Returns all data quality rules
        
        Rule Structure:
        {
            'rule_name': str,
            'rule_description': str,
            'category': str,  # Completeness, Accuracy, Consistency, etc.
            'target_table': str,
            'target_column': str (optional),
            'rule_type': str,  # not_null, unique, range, regex, custom_sql
            'rule_sql': str,  # SQL that returns violating records
            'severity': str,  # CRITICAL, WARNING, INFO
            'failure_threshold': float  # % of failures allowed (0-100)
        }
        """
        
        rules = []
        
        # =====================================================
        # COMPLETENESS RULES
        # =====================================================
        
        # Customer completeness checks
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
        
        rules.append({
            'rule_name': 'Customer Phone Not Null',
            'rule_description': 'Customer phone should be captured',
            'category': 'Completeness',
            'target_table': 'dim_customer',
            'target_column': 'phone_number',
            'rule_type': 'not_null',
            'rule_sql': """
                SELECT 
                    customer_key as record_identifier,
                    'phone_number' as column_name,
                    NULL as failed_value,
                    'NOT NULL' as expected_value
                FROM dim_customer
                WHERE phone_number IS NULL
                  AND is_current = TRUE
            """,
            'severity': 'INFO',
            'failure_threshold': 10.0
        })
        
        rules.append({
            'rule_name': 'Product Category Not Null',
            'rule_description': 'All products must have a category',
            'category': 'Completeness',
            'target_table': 'dim_product',
            'target_column': 'product_category',
            'rule_type': 'not_null',
            'rule_sql': """
                SELECT 
                    product_key as record_identifier,
                    'product_category' as column_name,
                    NULL as failed_value,
                    'NOT NULL' as expected_value
                FROM dim_product
                WHERE product_category IS NULL
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        # =====================================================
        # ACCURACY RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Customer Age Valid Range',
            'rule_description': 'Customer age must be between 18 and 120',
            'category': 'Accuracy',
            'target_table': 'dim_customer',
            'target_column': 'age',
            'rule_type': 'range',
            'rule_sql': """
                SELECT 
                    customer_key as record_identifier,
                    'age' as column_name,
                    age::text as failed_value,
                    '18-120' as expected_value
                FROM dim_customer
                WHERE age < 18 OR age > 120
                  AND is_current = TRUE
            """,
            'severity': 'WARNING',
            'failure_threshold': 1.0
        })
        
        rules.append({
            'rule_name': 'Transaction Amount Positive',
            'rule_description': 'Transaction net amount must be positive',
            'category': 'Accuracy',
            'target_table': 'fact_transactions',
            'target_column': 'net_amount',
            'rule_type': 'range',
            'rule_sql': """
                SELECT 
                    transaction_key as record_identifier,
                    'net_amount' as column_name,
                    net_amount::text as failed_value,
                    '> 0' as expected_value
                FROM fact_transactions
                WHERE net_amount <= 0
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.1
        })
        
        rules.append({
            'rule_name': 'Product Price Valid',
            'rule_description': 'Product retail price must be greater than cost',
            'category': 'Accuracy',
            'target_table': 'dim_product',
            'target_column': 'retail_price',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    product_key as record_identifier,
                    'retail_price' as column_name,
                    retail_price::text as failed_value,
                    '> unit_cost' as expected_value
                FROM dim_product
                WHERE retail_price <= unit_cost
            """,
            'severity': 'WARNING',
            'failure_threshold': 2.0
        })
        
        # =====================================================
        # UNIQUENESS RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Customer ID Unique',
            'rule_description': 'Customer ID must be unique per is_current flag',
            'category': 'Uniqueness',
            'target_table': 'dim_customer',
            'target_column': 'customer_id',
            'rule_type': 'unique',
            'rule_sql': """
                SELECT 
                    MIN(customer_key) as record_identifier,
                    'customer_id' as column_name,
                    customer_id as failed_value,
                    'UNIQUE' as expected_value
                FROM dim_customer
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        rules.append({
            'rule_name': 'Product ID Unique',
            'rule_description': 'Product ID must be unique',
            'category': 'Uniqueness',
            'target_table': 'dim_product',
            'target_column': 'product_id',
            'rule_type': 'unique',
            'rule_sql': """
                SELECT 
                    MIN(product_key) as record_identifier,
                    'product_id' as column_name,
                    product_id as failed_value,
                    'UNIQUE' as expected_value
                FROM dim_product
                GROUP BY product_id
                HAVING COUNT(*) > 1
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        # =====================================================
        # CONSISTENCY RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Customer Lifetime Value Consistency',
            'rule_description': 'Customer lifetime_value should equal sum of transactions',
            'category': 'Consistency',
            'target_table': 'dim_customer',
            'target_column': 'lifetime_value',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    c.customer_key as record_identifier,
                    'lifetime_value' as column_name,
                    c.lifetime_value::text as failed_value,
                    COALESCE(SUM(f.net_amount), 0)::text as expected_value
                FROM dim_customer c
                LEFT JOIN fact_transactions f ON c.customer_key = f.customer_key
                WHERE c.is_current = TRUE
                GROUP BY c.customer_key, c.lifetime_value
                HAVING ABS(c.lifetime_value - COALESCE(SUM(f.net_amount), 0)) > 0.01
            """,
            'severity': 'WARNING',
            'failure_threshold': 1.0
        })
        
        rules.append({
            'rule_name': 'Transaction Line Total Accuracy',
            'rule_description': 'Line total should equal quantity * unit_price - discount',
            'category': 'Consistency',
            'target_table': 'fact_transactions',
            'target_column': 'line_total',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    transaction_key as record_identifier,
                    'line_total' as column_name,
                    line_total::text as failed_value,
                    (quantity * unit_price)::text as expected_value
                FROM fact_transactions
                WHERE ABS(line_total - (quantity * unit_price)) > 0.01
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.1
        })
        
        # =====================================================
        # INTEGRITY RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Transaction Customer FK Valid',
            'rule_description': 'All transactions must have valid customer reference',
            'category': 'Integrity',
            'target_table': 'fact_transactions',
            'target_column': 'customer_key',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    transaction_key as record_identifier,
                    'customer_key' as column_name,
                    customer_key::text as failed_value,
                    'VALID FK' as expected_value
                FROM fact_transactions f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_customer c 
                    WHERE c.customer_key = f.customer_key
                )
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        rules.append({
            'rule_name': 'Transaction Product FK Valid',
            'rule_description': 'All transactions must have valid product reference',
            'category': 'Integrity',
            'target_table': 'fact_transactions',
            'target_column': 'product_key',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    transaction_key as record_identifier,
                    'product_key' as column_name,
                    product_key::text as failed_value,
                    'VALID FK' as expected_value
                FROM fact_transactions f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_product p 
                    WHERE p.product_key = f.product_key
                )
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        # =====================================================
        # VALIDITY RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Email Format Valid',
            'rule_description': 'Email addresses must follow standard format',
            'category': 'Validity',
            'target_table': 'dim_customer',
            'target_column': 'email',
            'rule_type': 'regex',
            'rule_sql': """
                SELECT 
                    customer_key as record_identifier,
                    'email' as column_name,
                    email as failed_value,
                    'valid email format' as expected_value
                FROM dim_customer
                WHERE email IS NOT NULL
                  AND email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                  AND is_current = TRUE
            """,
            'severity': 'WARNING',
            'failure_threshold': 2.0
        })
        
        rules.append({
            'rule_name': 'Transaction Date Valid',
            'rule_description': 'Transaction dates must not be in the future',
            'category': 'Validity',
            'target_table': 'fact_transactions',
            'target_column': 'transaction_date_key',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    transaction_key as record_identifier,
                    'transaction_date_key' as column_name,
                    transaction_date_key::text as failed_value,
                    '<= TODAY' as expected_value
                FROM fact_transactions f
                JOIN dim_date d ON f.transaction_date_key = d.date_key
                WHERE d.date_actual > CURRENT_DATE
            """,
            'severity': 'CRITICAL',
            'failure_threshold': 0.0
        })
        
        # =====================================================
        # TIMELINESS RULES
        # =====================================================
        
        rules.append({
            'rule_name': 'Recent Data Available',
            'rule_description': 'Transactions should exist within last 7 days',
            'category': 'Timeliness',
            'target_table': 'fact_transactions',
            'target_column': 'created_date',
            'rule_type': 'custom_sql',
            'rule_sql': """
                SELECT 
                    0 as record_identifier,
                    'transaction_date' as column_name,
                    MAX(d.date_actual)::text as failed_value,
                    (CURRENT_DATE - INTERVAL '7 days')::text as expected_value
                FROM fact_transactions f
                JOIN dim_date d ON f.transaction_date_key = d.date_key
                HAVING MAX(d.date_actual) < CURRENT_DATE - INTERVAL '7 days'
            """,
            'severity': 'WARNING',
            'failure_threshold': 0.0
        })
        
        logger.info(f"Loaded {len(rules)} data quality rules")
        return rules