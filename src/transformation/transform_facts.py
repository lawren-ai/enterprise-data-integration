"""
Fact Table Transformation Module
Transforms staging data into warehouse fact tables
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager

logger = get_logger(__name__)


class FactTransformer:
    """Handles transformation of staging data into fact tables"""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        logger.info("FactTransformer initialized")
    
    def transform_transaction_facts(self) -> int:
        """
        Transform transactions into fact table
        Joins with dimensions to get surrogate keys
        """
        logger.info("=" * 60)
        logger.info("Transforming Transaction Facts")
        logger.info("=" * 60)
        
        try:
            # Read transaction data with items
            transactions_df = self.db.read_query("""
                SELECT 
                    t.transaction_id,
                    t.customer_id,
                    t.transaction_date,
                    t.order_number,
                    t.payment_method,
                    t.payment_status,
                    t.tax_amount,
                    t.shipping_amount,
                    t.currency_code,
                    ti.transaction_item_id,
                    ti.product_id,
                    ti.quantity,
                    ti.unit_price,
                    ti.line_total,
                    ti.discount_amount
                FROM stg_ecom_transactions t
                INNER JOIN stg_ecom_transaction_items ti 
                    ON t.transaction_id = ti.transaction_id
                WHERE t.transaction_id IS NOT NULL
                  AND ti.product_id IS NOT NULL
                  AND t.customer_id IS NOT NULL
            """)
            
            logger.info(f"Read {len(transactions_df)} transaction items from staging")
            
            # Get dimension keys
            logger.info("Looking up dimension keys...")
            
            # For SCD Type 2, we need to join on customer_id AND date range
            # Use SQL to do the join directly - more reliable than pandas date comparison
            transactions_with_keys_df = self.db.read_query("""
                SELECT 
                    t.transaction_id,
                    t.order_number,
                    t.transaction_date,
                    t.payment_method,
                    t.payment_status,
                    t.tax_amount,
                    t.shipping_amount,
                    t.currency_code,
                    ti.quantity,
                    ti.unit_price,
                    ti.line_total,
                    ti.discount_amount,
                    ti.line_total - ti.discount_amount as net_amount,
                    c.customer_key,
                    p.product_key,
                    TO_CHAR(t.transaction_date, 'YYYYMMDD')::INTEGER as transaction_date_key
                FROM stg_ecom_transactions t
                INNER JOIN stg_ecom_transaction_items ti 
                    ON t.transaction_id = ti.transaction_id
                INNER JOIN dim_customer c 
                    ON t.customer_id = c.customer_id
                    AND t.transaction_date::date BETWEEN c.valid_from AND c.valid_to
                INNER JOIN dim_product p 
                    ON ti.product_id = p.product_id
                WHERE t.transaction_id IS NOT NULL
                  AND ti.product_id IS NOT NULL
                  AND t.customer_id IS NOT NULL
            """)
            
            logger.info(f"After dimension lookup: {len(transactions_with_keys_df)} valid transaction items")
            
            # Prepare fact table
            fact_df = transactions_with_keys_df[[
                'customer_key',
                'product_key',
                'transaction_date_key',
                'transaction_id',
                'order_number',
                'quantity',
                'unit_price',
                'line_total',
                'discount_amount',
                'tax_amount',
                'shipping_amount',
                'net_amount',
                'payment_method',
                'payment_status',
                'currency_code'
            ]].copy()
            
            # Ensure proper data types
            fact_df['customer_key'] = fact_df['customer_key'].astype('int64')
            fact_df['product_key'] = fact_df['product_key'].astype('int64')
            fact_df['transaction_date_key'] = fact_df['transaction_date_key'].astype('int32')
            
            # Add metadata
            fact_df['created_date'] = datetime.now()
            fact_df['source_system'] = 'E-COMMERCE'
            
            # Check for duplicates before loading
            existing_transactions = self.db.read_query("""
                SELECT DISTINCT transaction_id 
                FROM fact_transactions
            """)
            
            if len(existing_transactions) > 0:
                fact_df = fact_df[
                    ~fact_df['transaction_id'].isin(existing_transactions['transaction_id'])
                ]
                logger.info(f"Filtered out {len(transactions_df) - len(fact_df)} duplicate transactions")
            
            if len(fact_df) > 0:
                # Load to fact table
                self.db.load_dataframe(
                    fact_df,
                    'fact_transactions',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"✓ Loaded {len(fact_df)} transaction facts")
            else:
                logger.info("No new transactions to load")
            
            return len(fact_df)
            
        except Exception as e:
            logger.error(f"Transaction fact transformation failed: {e}")
            raise
    
    def transform_campaign_response_facts(self) -> int:
        """
        Transform campaign responses into fact table
        """
        logger.info("=" * 60)
        logger.info("Transforming Campaign Response Facts")
        logger.info("=" * 60)
        
        try:
            # Use SQL join to handle SCD Type 2 properly
            responses_with_keys_df = self.db.read_query("""
                SELECT 
                    r.response_id,
                    r.response_type,
                    r.conversion_value,
                    CASE WHEN r.response_type = 'opened' THEN TRUE ELSE FALSE END as is_opened,
                    CASE WHEN r.response_type = 'clicked' THEN TRUE ELSE FALSE END as is_clicked,
                    CASE WHEN r.response_type = 'converted' THEN TRUE ELSE FALSE END as is_converted,
                    c.customer_key,
                    cam.campaign_key,
                    TO_CHAR(r.response_date, 'YYYYMMDD')::INTEGER as response_date_key
                FROM stg_campaign_responses r
                INNER JOIN dim_customer c 
                    ON r.customer_id = c.customer_id
                    AND r.response_date::date BETWEEN c.valid_from AND c.valid_to
                INNER JOIN dim_campaign cam 
                    ON r.campaign_id = cam.campaign_id
                WHERE r.response_id IS NOT NULL
                  AND r.campaign_id IS NOT NULL
                  AND r.customer_id IS NOT NULL
            """)
            
            logger.info(f"After dimension lookup: {len(responses_with_keys_df)} valid responses")
            
            # Prepare fact table
            fact_df = responses_with_keys_df[[
                'customer_key',
                'campaign_key',
                'response_date_key',
                'response_id',
                'response_type',
                'conversion_value',
                'is_opened',
                'is_clicked',
                'is_converted'
            ]].copy()
            
            # Ensure proper data types
            fact_df['customer_key'] = fact_df['customer_key'].astype('int64')
            fact_df['campaign_key'] = fact_df['campaign_key'].astype('int64')
            fact_df['response_date_key'] = fact_df['response_date_key'].astype('int32')
            
            # Add metadata
            fact_df['created_date'] = datetime.now()
            fact_df['source_system'] = 'MARKETING'
            
            # Check for duplicates
            existing_responses = self.db.read_query("""
                SELECT DISTINCT response_id 
                FROM fact_campaign_responses
            """)
            
            if len(existing_responses) > 0:
                fact_df = fact_df[
                    ~fact_df['response_id'].isin(existing_responses['response_id'])
                ]
            
            if len(fact_df) > 0:
                # Load to fact table
                self.db.load_dataframe(
                    fact_df,
                    'fact_campaign_responses',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"✓ Loaded {len(fact_df)} campaign response facts")
            else:
                logger.info("No new campaign responses to load")
            
            return len(fact_df)
            
        except Exception as e:
            logger.error(f"Campaign response fact transformation failed: {e}")
            raise
    
    def transform_all_facts(self) -> dict:
        """Transform all fact tables"""
        logger.info("=" * 60)
        logger.info("FACT TRANSFORMATION PIPELINE")
        logger.info("=" * 60)
        
        results = {}
        
        try:
            # Transactions
            results['transactions'] = self.transform_transaction_facts()
            
            # Campaign Responses
            results['campaign_responses'] = self.transform_campaign_response_facts()
            
            logger.info("=" * 60)
            logger.info("✓ All facts transformed successfully!")
            logger.info(f"Transaction facts: {results['transactions']:,}")
            logger.info(f"Campaign response facts: {results['campaign_responses']:,}")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            logger.error(f"Fact transformation pipeline failed: {e}")
            raise


def main():
    """Main execution"""
    transformer = FactTransformer()
    transformer.transform_all_facts()


if __name__ == "__main__":
    main()