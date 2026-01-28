"""
Staging Layer Loader
Loads raw CSV files into staging tables
"""

import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Optional
import argparse

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager

logger = get_logger(__name__)


class StagingLoader:
    """Handles loading raw data files into staging tables"""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        self.raw_data_path = Path(self.config.get('paths.raw_data'))
        self.batch_size = self.config.get('etl.batch_size', 10000)
        self.load_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))
        
        logger.info(f"StagingLoader initialized with load_id: {self.load_id}")
    
    def _generate_row_hash(self, row: pd.Series) -> str:
        """Generate MD5 hash for a row (for change detection)"""
        row_string = '|'.join(str(v) for v in row.values)
        return hashlib.md5(row_string.encode()).hexdigest()
    
    def _add_audit_columns(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Add technical audit columns to DataFrame"""
        df['source_file'] = source_file
        df['load_timestamp'] = datetime.now()
        df['load_id'] = self.load_id
        
        # Generate row hash for change detection
        df['row_hash'] = df.apply(self._generate_row_hash, axis=1)
        
        return df
    
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
        """Log load activity to audit table"""
        audit_record = {
            'load_id': self.load_id,
            'table_name': table_name,
            'load_type': load_type,
            'source_file': source_file,
            'rows_loaded': rows_loaded,
            'rows_rejected': rows_rejected,
            'load_start_time': datetime.now(),
            'load_end_time': datetime.now(),
            'load_status': status,
            'error_message': error_message,
            'loaded_by': 'system'
        }
        
        audit_df = pd.DataFrame([audit_record])
        self.db.load_dataframe(audit_df, 'stg_audit_log', if_exists='append')
    
    def _find_latest_file(self, pattern: str) -> Optional[Path]:
        """Find the most recent file matching pattern"""
        files = list(self.raw_data_path.glob(pattern))
        
        if not files:
            logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
        # Sort by modification time, get latest
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Found latest file: {latest_file.name}")
        return latest_file
    
    def load_customers(self, file_path: Optional[Path] = None) -> int:
        """Load customer data into staging"""
        logger.info("Loading customers to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('crm_customers_*.csv')
            if file_path is None:
                raise FileNotFoundError("No customer file found")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            original_count = len(df)
            logger.info(f"Read {original_count} rows from {file_path.name}")
            
            # Add audit columns
            df = self._add_audit_columns(df, file_path.name)
            
            # Load to staging
            self.db.load_dataframe(
                df, 
                'stg_crm_customers', 
                if_exists='append',
                chunksize=self.batch_size
            )
            
            # Log audit
            self._log_audit(
                'stg_crm_customers',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} customer records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load customers: {e}")
            self._log_audit(
                'stg_crm_customers',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_products(self, file_path: Optional[Path] = None) -> int:
        """Load product data into staging"""
        logger.info("Loading products to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('products_*.csv')
            if file_path is None:
                raise FileNotFoundError("No product file found")
        
        try:
            df = pd.read_csv(file_path)
            df = self._add_audit_columns(df, file_path.name)
            
            self.db.load_dataframe(
                df,
                'stg_products',
                if_exists='append',
                chunksize=self.batch_size
            )
            
            self._log_audit(
                'stg_products',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} product records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load products: {e}")
            self._log_audit(
                'stg_products',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_transactions(self, file_path: Optional[Path] = None) -> int:
        """Load transaction header data into staging"""
        logger.info("Loading transactions to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('ecom_transactions_*.csv')
            if file_path is None:
                raise FileNotFoundError("No transaction file found")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['transaction_date'])
            df = self._add_audit_columns(df, file_path.name)
            
            self.db.load_dataframe(
                df,
                'stg_ecom_transactions',
                if_exists='append',
                chunksize=self.batch_size
            )
            
            self._log_audit(
                'stg_ecom_transactions',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} transaction records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load transactions: {e}")
            self._log_audit(
                'stg_ecom_transactions',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_transaction_items(self, file_path: Optional[Path] = None) -> int:
        """Load transaction line items into staging"""
        logger.info("Loading transaction items to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('ecom_transaction_items_*.csv')
            if file_path is None:
                raise FileNotFoundError("No transaction items file found")
        
        try:
            df = pd.read_csv(file_path)
            df = self._add_audit_columns(df, file_path.name)
            
            self.db.load_dataframe(
                df,
                'stg_ecom_transaction_items',
                if_exists='append',
                chunksize=self.batch_size
            )
            
            self._log_audit(
                'stg_ecom_transaction_items',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} transaction item records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load transaction items: {e}")
            self._log_audit(
                'stg_ecom_transaction_items',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_campaigns(self, file_path: Optional[Path] = None) -> int:
        """Load campaign data into staging"""
        logger.info("Loading campaigns to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('marketing_campaigns_*.csv')
            if file_path is None:
                raise FileNotFoundError("No campaign file found")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['start_date', 'end_date'])
            df = self._add_audit_columns(df, file_path.name)
            
            self.db.load_dataframe(
                df,
                'stg_marketing_campaigns',
                if_exists='append',
                chunksize=self.batch_size
            )
            
            self._log_audit(
                'stg_marketing_campaigns',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} campaign records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load campaigns: {e}")
            self._log_audit(
                'stg_marketing_campaigns',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_campaign_responses(self, file_path: Optional[Path] = None) -> int:
        """Load campaign response data into staging"""
        logger.info("Loading campaign responses to staging...")
        
        if file_path is None:
            file_path = self._find_latest_file('campaign_responses_*.csv')
            if file_path is None:
                raise FileNotFoundError("No campaign responses file found")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['response_date'])
            df = self._add_audit_columns(df, file_path.name)
            
            self.db.load_dataframe(
                df,
                'stg_campaign_responses',
                if_exists='append',
                chunksize=self.batch_size
            )
            
            self._log_audit(
                'stg_campaign_responses',
                'FULL',
                file_path.name,
                len(df),
                0,
                'SUCCESS'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} campaign response records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load campaign responses: {e}")
            self._log_audit(
                'stg_campaign_responses',
                'FULL',
                file_path.name if file_path else 'N/A',
                0,
                0,
                'FAILED',
                str(e)
            )
            raise
    
    def load_all(self) -> dict:
        """Load all tables to staging"""
        logger.info("=" * 60)
        logger.info("Starting full staging load pipeline")
        logger.info("=" * 60)
        
        results = {}
        
        try:
            results['customers'] = self.load_customers()
            results['products'] = self.load_products()
            results['transactions'] = self.load_transactions()
            results['transaction_items'] = self.load_transaction_items()
            results['campaigns'] = self.load_campaigns()
            results['campaign_responses'] = self.load_campaign_responses()
            
            total_rows = sum(results.values())
            logger.info("=" * 60)
            logger.info(f"✓ Staging load completed successfully!")
            logger.info(f"Total rows loaded: {total_rows:,}")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            logger.error(f"Staging load failed: {e}")
            raise


def main():
    """Main execution with command line arguments"""
    parser = argparse.ArgumentParser(description='Load data to staging tables')
    parser.add_argument(
        '--table',
        choices=['customers', 'products', 'transactions', 'items', 'campaigns', 'responses', 'all'],
        default='all',
        help='Specific table to load (default: all)'
    )
    
    args = parser.parse_args()
    
    loader = StagingLoader()
    
    if args.table == 'all':
        loader.load_all()
    elif args.table == 'customers':
        loader.load_customers()
    elif args.table == 'products':
        loader.load_products()
    elif args.table == 'transactions':
        loader.load_transactions()
    elif args.table == 'items':
        loader.load_transaction_items()
    elif args.table == 'campaigns':
        loader.load_campaigns()
    elif args.table == 'responses':
        loader.load_campaign_responses()


if __name__ == "__main__":
    main()