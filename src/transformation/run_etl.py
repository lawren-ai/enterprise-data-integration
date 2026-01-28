"""
ETL Pipeline Orchestrator
Coordinates the full ETL process from staging to warehouse
"""

import argparse
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager
from transformation.transform_dimensions import DimensionTransformer
from transformation.transform_facts import FactTransformer

logger = get_logger(__name__)


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        self.dim_transformer = DimensionTransformer()
        self.fact_transformer = FactTransformer()
        
        self.start_time = None
        self.end_time = None
        
        logger.info("ETLOrchestrator initialized")
    
    def run_full_pipeline(self):
        """
        Run the complete ETL pipeline
        Order: Dimensions first, then Facts
        """
        self.start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("ENTERPRISE DATA WAREHOUSE - FULL ETL PIPELINE")
        logger.info(f"Started at: {self.start_time}")
        logger.info("=" * 80)
        
        try:
            # Phase 1: Transform Dimensions
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 1: DIMENSION TRANSFORMATION")
            logger.info("=" * 80)
            
            dim_results = self.dim_transformer.transform_all_dimensions()
            
            # Phase 2: Transform Facts
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 2: FACT TRANSFORMATION")
            logger.info("=" * 80)
            
            fact_results = self.fact_transformer.transform_all_facts()
            
            # Phase 3: Update Customer Metrics
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 3: UPDATE CUSTOMER METRICS")
            logger.info("=" * 80)
            
            self.update_customer_metrics()
            
            # Phase 4: Build Aggregates (Optional)
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 4: BUILD AGGREGATE TABLES")
            logger.info("=" * 80)
            
            self.build_aggregates()
            
            # Completion
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            logger.info("\n" + "=" * 80)
            logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 80)
            logger.info(f"Start time: {self.start_time}")
            logger.info(f"End time: {self.end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("\nSummary:")
            logger.info(f"  Products: {dim_results['products']}")
            logger.info(f"  Campaigns: {dim_results['campaigns']}")
            logger.info(f"  Customers (New): {dim_results['customers_new']}")
            logger.info(f"  Customers (Updated): {dim_results['customers_updated']}")
            logger.info(f"  Transaction Facts: {fact_results['transactions']:,}")
            logger.info(f"  Campaign Response Facts: {fact_results['campaign_responses']:,}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise
    
    def update_customer_metrics(self):
        """
        Update customer lifetime metrics in dimension
        Updates: lifetime_value, total_orders, total_spent
        """
        logger.info("Updating customer lifetime metrics...")
        
        try:
            # Calculate metrics from fact table
            update_sql = """
                UPDATE dim_customer c
                SET 
                    lifetime_value = COALESCE(metrics.total_revenue, 0),
                    total_orders = COALESCE(metrics.order_count, 0),
                    total_spent = COALESCE(metrics.total_revenue, 0),
                    updated_date = CURRENT_TIMESTAMP
                FROM (
                    SELECT 
                        customer_key,
                        COUNT(DISTINCT transaction_id) as order_count,
                        SUM(net_amount) as total_revenue
                    FROM fact_transactions
                    GROUP BY customer_key
                ) metrics
                WHERE c.customer_key = metrics.customer_key
                  AND c.is_current = TRUE;
            """
            
            self.db.execute_sql(update_sql)
            logger.info("✓ Customer metrics updated")
            
        except Exception as e:
            logger.error(f"Failed to update customer metrics: {e}")
            raise
    
    def build_aggregates(self):
        """Build aggregate tables for performance"""
        logger.info("Building aggregate tables...")
        
        try:
            # Customer Monthly Aggregates
            logger.info("Building customer monthly aggregates...")
            
            self.db.execute_sql("TRUNCATE TABLE agg_customer_monthly")
            
            agg_sql = """
                INSERT INTO agg_customer_monthly (
                    customer_key,
                    year_month,
                    total_transactions,
                    total_quantity,
                    total_amount,
                    total_discount,
                    avg_transaction_value,
                    month_start_date,
                    month_end_date,
                    created_date
                )
                SELECT 
                    f.customer_key,
                    EXTRACT(YEAR FROM d.date_actual) * 100 + EXTRACT(MONTH FROM d.date_actual) AS year_month,
                    COUNT(DISTINCT f.transaction_id) as total_transactions,
                    SUM(f.quantity) as total_quantity,
                    SUM(f.net_amount) as total_amount,
                    SUM(f.discount_amount) as total_discount,
                    AVG(f.net_amount) as avg_transaction_value,
                    DATE_TRUNC('month', d.date_actual)::DATE as month_start_date,
                    (DATE_TRUNC('month', d.date_actual) + INTERVAL '1 month' - INTERVAL '1 day')::DATE as month_end_date,
                    CURRENT_TIMESTAMP
                FROM fact_transactions f
                JOIN dim_date d ON f.transaction_date_key = d.date_key
                GROUP BY 
                    f.customer_key,
                    EXTRACT(YEAR FROM d.date_actual),
                    EXTRACT(MONTH FROM d.date_actual),
                    DATE_TRUNC('month', d.date_actual);
            """
            
            self.db.execute_sql(agg_sql)
            logger.info("✓ Customer monthly aggregates built")
            
            # Product Daily Aggregates
            logger.info("Building product daily aggregates...")
            
            self.db.execute_sql("TRUNCATE TABLE agg_product_daily")
            
            prod_agg_sql = """
                INSERT INTO agg_product_daily (
                    product_key,
                    date_key,
                    units_sold,
                    total_revenue,
                    total_cost,
                    total_profit,
                    unique_customers,
                    created_date
                )
                SELECT 
                    f.product_key,
                    f.transaction_date_key,
                    SUM(f.quantity) as units_sold,
                    SUM(f.net_amount) as total_revenue,
                    SUM(f.quantity * p.unit_cost) as total_cost,
                    SUM(f.net_amount - (f.quantity * p.unit_cost)) as total_profit,
                    COUNT(DISTINCT f.customer_key) as unique_customers,
                    CURRENT_TIMESTAMP
                FROM fact_transactions f
                JOIN dim_product p ON f.product_key = p.product_key
                GROUP BY 
                    f.product_key,
                    f.transaction_date_key;
            """
            
            self.db.execute_sql(prod_agg_sql)
            logger.info("✓ Product daily aggregates built")
            
            # Verify aggregates
            cust_agg_count = self.db.get_table_row_count('agg_customer_monthly')
            prod_agg_count = self.db.get_table_row_count('agg_product_daily')
            
            logger.info(f"Customer monthly records: {cust_agg_count:,}")
            logger.info(f"Product daily records: {prod_agg_count:,}")
            
        except Exception as e:
            logger.error(f"Failed to build aggregates: {e}")
            raise
    
    def run_incremental_pipeline(self):
        """
        Run incremental ETL (for delta loads)
        This would check watermarks and process only new/changed data
        """
        logger.info("=" * 80)
        logger.info("INCREMENTAL ETL PIPELINE (Not Yet Implemented)")
        logger.info("=" * 80)
        logger.info("For now, use --full to run complete ETL")
        logger.info("Incremental logic would:")
        logger.info("  1. Check watermark tables for last load time")
        logger.info("  2. Extract only new/changed records from staging")
        logger.info("  3. Process deltas through transformation")
        logger.info("  4. Update watermarks")
    
    def validate_pipeline(self):
        """
        Validate the ETL results
        """
        logger.info("=" * 80)
        logger.info("ETL VALIDATION")
        logger.info("=" * 80)
        
        try:
            # Count records in each table
            counts = {
                'dim_customer': self.db.get_table_row_count('dim_customer'),
                'dim_product': self.db.get_table_row_count('dim_product'),
                'dim_campaign': self.db.get_table_row_count('dim_campaign'),
                'dim_date': self.db.get_table_row_count('dim_date'),
                'fact_transactions': self.db.get_table_row_count('fact_transactions'),
                'fact_campaign_responses': self.db.get_table_row_count('fact_campaign_responses'),
            }
            
            logger.info("Record counts:")
            for table, count in counts.items():
                logger.info(f"  {table}: {count:,}")
            
            # Check for referential integrity
            logger.info("\nReferential Integrity Checks:")
            
            orphan_checks = [
                ("fact_transactions", "customer_key", "dim_customer", "customer_key"),
                ("fact_transactions", "product_key", "dim_product", "product_key"),
                ("fact_transactions", "transaction_date_key", "dim_date", "date_key"),
                ("fact_campaign_responses", "customer_key", "dim_customer", "customer_key"),
                ("fact_campaign_responses", "campaign_key", "dim_campaign", "campaign_key"),
            ]
            
            all_valid = True
            for fact_table, fk_column, dim_table, dim_key_column in orphan_checks:
                orphans = self.db.read_query(f"""
                    SELECT COUNT(*) as cnt
                    FROM {fact_table} f
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {dim_table} d 
                        WHERE d.{dim_key_column} = f.{fk_column}
                    )
                """)
                
                orphan_count = orphans.iloc[0]['cnt']
                if orphan_count > 0:
                    logger.warning(f"  ✗ {fact_table}.{fk_column}: {orphan_count} orphaned records")
                    all_valid = False
                else:
                    logger.info(f"  ✓ {fact_table}.{fk_column}: No orphans")
            
            if all_valid:
                logger.info("\n✓ All validation checks passed!")
            else:
                logger.warning("\n⚠ Some validation checks failed")
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")


def main():
    """Main execution with command line arguments"""
    parser = argparse.ArgumentParser(
        description='Run ETL Pipeline - Transform staging data to warehouse'
    )
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'validate'],
        default='full',
        help='ETL mode: full load, incremental, or validate only'
    )
    
    args = parser.parse_args()
    
    orchestrator = ETLOrchestrator()
    
    if args.mode == 'full':
        orchestrator.run_full_pipeline()
    elif args.mode == 'incremental':
        orchestrator.run_incremental_pipeline()
    elif args.mode == 'validate':
        orchestrator.validate_pipeline()


if __name__ == "__main__":
    main()