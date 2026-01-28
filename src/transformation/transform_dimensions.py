"""
Dimension Transformation Module
Transforms staging data into warehouse dimensions
Implements SCD Type 2 for customer dimension
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Tuple, List
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager

logger = get_logger(__name__)


class DimensionTransformer:
    """Handles transformation of staging data into dimension tables"""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        logger.info("DimensionTransformer initialized")
    
    def transform_product_dimension(self) -> int:
        """
        Transform products from staging to dimension
        SCD Type 1 - Overwrites changes
        """
        logger.info("=" * 60)
        logger.info("Transforming Product Dimension")
        logger.info("=" * 60)
        
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
                # Update existing products
                new_products = df[~df['product_id'].isin(existing_products['product_id'])]
                update_products = df[df['product_id'].isin(existing_products['product_id'])]
                
                if len(new_products) > 0:
                    self.db.load_dataframe(
                        new_products,
                        'dim_product',
                        if_exists='append',
                        chunksize=1000
                    )
                    logger.info(f"Inserted {len(new_products)} new products")
                
                # For updates, we'll use SQL UPDATE (SCD Type 1)
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
                                updated_date = :updated
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
                            'id': product['product_id']
                        })
                    logger.info(f"Updated {len(update_products)} existing products")
            else:
                # First load - insert all
                self.db.load_dataframe(
                    df,
                    'dim_product',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(df)} products (initial load)")
            
            logger.info("✓ Product dimension transformation completed")
            return len(df)
            
        except Exception as e:
            logger.error(f"Product dimension transformation failed: {e}")
            raise
    
    def transform_campaign_dimension(self) -> int:
        """
        Transform campaigns from staging to dimension
        SCD Type 1 - Overwrites changes
        """
        logger.info("=" * 60)
        logger.info("Transforming Campaign Dimension")
        logger.info("=" * 60)
        
        try:
            df = self.db.read_query("""
                SELECT DISTINCT
                    campaign_id,
                    campaign_name,
                    campaign_type,
                    channel,
                    start_date,
                    end_date,
                    budget,
                    target_audience,
                    campaign_status
                FROM stg_marketing_campaigns
                WHERE campaign_id IS NOT NULL
            """)
            
            logger.info(f"Read {len(df)} campaigns from staging")
            
            # Calculate duration
            df['duration_days'] = (
                pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])
            ).dt.days
            
            # Add metadata
            df['created_date'] = datetime.now()
            df['updated_date'] = datetime.now()
            df['source_system'] = 'MARKETING'
            
            # Check existing
            existing_campaigns = self.db.read_query("""
                SELECT campaign_id FROM dim_campaign
            """)
            
            if len(existing_campaigns) > 0:
                new_campaigns = df[~df['campaign_id'].isin(existing_campaigns['campaign_id'])]
                
                if len(new_campaigns) > 0:
                    self.db.load_dataframe(
                        new_campaigns,
                        'dim_campaign',
                        if_exists='append',
                        chunksize=1000
                    )
                    logger.info(f"Inserted {len(new_campaigns)} new campaigns")
                else:
                    logger.info("No new campaigns to insert")
            else:
                self.db.load_dataframe(
                    df,
                    'dim_campaign',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(df)} campaigns (initial load)")
            
            logger.info("✓ Campaign dimension transformation completed")
            return len(df)
            
        except Exception as e:
            logger.error(f"Campaign dimension transformation failed: {e}")
            raise
    
    def transform_customer_dimension_scd2(self) -> Tuple[int, int, int]:
        """
        Transform customers with SCD Type 2 logic
        Tracks historical changes for address-related fields
        
        Returns:
            Tuple of (new_records, updated_records, unchanged_records)
        """
        logger.info("=" * 60)
        logger.info("Transforming Customer Dimension (SCD Type 2)")
        logger.info("=" * 60)
        
        try:
            # Read staging data
            staging_df = self.db.read_query("""
                SELECT DISTINCT ON (customer_id)
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
                ORDER BY customer_id, load_timestamp DESC
            """)
            
            logger.info(f"Read {len(staging_df)} customers from staging")
            
            # Data transformations
            staging_df['full_name'] = (
                staging_df['first_name'] + ' ' + staging_df['last_name']
            )
            
            # Calculate age
            staging_df['date_of_birth'] = pd.to_datetime(staging_df['date_of_birth'])
            today = pd.Timestamp.now()
            staging_df['age'] = (
                (today - staging_df['date_of_birth']).dt.days // 365
            )
            
            # Age groups
            staging_df['age_group'] = pd.cut(
                staging_df['age'],
                bins=[0, 25, 35, 45, 55, 65, 100],
                labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
            ).astype(str)
            
            # Read existing dimension
            existing_df = self.db.read_query("""
                SELECT 
                    customer_key,
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone_number,
                    address,
                    city,
                    state,
                    postal_code,
                    customer_segment,
                    is_current,
                    valid_from,
                    valid_to
                FROM dim_customer
                WHERE is_current = TRUE
            """)
            
            # SCD Type 2 tracked columns
            scd2_columns = ['address', 'city', 'state', 'postal_code', 'customer_segment']
            
            # Type 1 columns (always update)
            type1_columns = ['email', 'phone_number']
            
            new_customers = []
            updated_customers = []
            unchanged_count = 0
            
            for _, staged_row in staging_df.iterrows():
                customer_id = staged_row['customer_id']
                
                # Check if customer exists
                existing = existing_df[existing_df['customer_id'] == customer_id]
                
                if len(existing) == 0:
                    # New customer - insert
                    new_record = staged_row.to_dict()
                    new_record['valid_from'] = staged_row['registration_date']  # Use registration date, not today!
                    new_record['valid_to'] = date(9999, 12, 31)
                    new_record['is_current'] = True
                    new_record['created_date'] = datetime.now()
                    new_record['updated_date'] = datetime.now()
                    new_record['source_system'] = 'CRM'
                    new_record['lifetime_value'] = 0.0
                    new_record['total_orders'] = 0
                    new_record['total_spent'] = 0.0
                    
                    new_customers.append(new_record)
                else:
                    existing_row = existing.iloc[0]
                    
                    # Check if SCD Type 2 columns changed
                    scd2_changed = False
                    for col in scd2_columns:
                        if staged_row[col] != existing_row[col]:
                            scd2_changed = True
                            break
                    
                    if scd2_changed:
                        # Expire old record
                        self.db.execute_sql("""
                            UPDATE dim_customer
                            SET valid_to = :today,
                                is_current = FALSE,
                                updated_date = :updated
                            WHERE customer_key = :key
                        """, {
                            'today': date.today(),
                            'updated': datetime.now(),
                            'key': int(existing_row['customer_key'])
                        })
                        
                        # Insert new version
                        new_version = staged_row.to_dict()
                        new_version['valid_from'] = date.today()  # SCD change starts today
                        new_version['valid_to'] = date(9999, 12, 31)
                        new_version['is_current'] = True
                        new_version['created_date'] = datetime.now()
                        new_version['updated_date'] = datetime.now()
                        new_version['source_system'] = 'CRM'
                        new_version['lifetime_value'] = 0.0
                        new_version['total_orders'] = 0
                        new_version['total_spent'] = 0.0
                        
                        updated_customers.append(new_version)
                    else:
                        # Check Type 1 columns and update in place
                        type1_changed = False
                        for col in type1_columns:
                            if col in staged_row and staged_row[col] != existing_row[col]:
                                type1_changed = True
                        
                        if type1_changed:
                            self.db.execute_sql("""
                                UPDATE dim_customer
                                SET email = :email,
                                    phone_number = :phone,
                                    updated_date = :updated
                                WHERE customer_key = :key
                            """, {
                                'email': staged_row['email'],
                                'phone': staged_row['phone_number'],
                                'updated': datetime.now(),
                                'key': int(existing_row['customer_key'])
                            })
                        
                        unchanged_count += 1
            
            # Insert new customers
            if new_customers:
                new_df = pd.DataFrame(new_customers)
                self.db.load_dataframe(
                    new_df,
                    'dim_customer',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(new_customers)} new customers")
            
            # Insert updated versions
            if updated_customers:
                updated_df = pd.DataFrame(updated_customers)
                self.db.load_dataframe(
                    updated_df,
                    'dim_customer',
                    if_exists='append',
                    chunksize=1000
                )
                logger.info(f"Inserted {len(updated_customers)} updated customer versions")
            
            logger.info(f"Unchanged customers: {unchanged_count}")
            logger.info("✓ Customer dimension transformation completed")
            
            return len(new_customers), len(updated_customers), unchanged_count
            
        except Exception as e:
            logger.error(f"Customer dimension transformation failed: {e}")
            raise
    
    def transform_all_dimensions(self) -> dict:
        """Transform all dimensions"""
        logger.info("=" * 60)
        logger.info("DIMENSION TRANSFORMATION PIPELINE")
        logger.info("=" * 60)
        
        results = {}
        
        try:
            # Products
            results['products'] = self.transform_product_dimension()
            
            # Campaigns
            results['campaigns'] = self.transform_campaign_dimension()
            
            # Customers (SCD Type 2)
            new_cust, upd_cust, unch_cust = self.transform_customer_dimension_scd2()
            results['customers_new'] = new_cust
            results['customers_updated'] = upd_cust
            results['customers_unchanged'] = unch_cust
            
            logger.info("=" * 60)
            logger.info("✓ All dimensions transformed successfully!")
            logger.info(f"Products: {results['products']}")
            logger.info(f"Campaigns: {results['campaigns']}")
            logger.info(f"Customers - New: {results['customers_new']}, "
                       f"Updated: {results['customers_updated']}, "
                       f"Unchanged: {results['customers_unchanged']}")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            logger.error(f"Dimension transformation pipeline failed: {e}")
            raise


def main():
    """Main execution"""
    transformer = DimensionTransformer()
    transformer.transform_all_dimensions()


if __name__ == "__main__":
    main()