"""
Data Mapping Documentation Generator
Creates Excel templates with source-to-target field mappings
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager

logger = get_logger(__name__)


class MappingDocGenerator:
    """Generate data mapping documentation"""
    
    def __init__(self, output_dir: str = "docs/mapping"):
        self.config = get_config()
        self.db = get_db_manager()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"MappingDocGenerator initialized - Output: {self.output_dir}")
    
    def create_customer_dimension_mapping(self) -> Path:
        """Create customer dimension mapping document"""
        logger.info("Creating Customer Dimension mapping...")
        
        # Define mapping
        mapping_data = [
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'customer_id',
                'Target Table': 'dim_customer',
                'Target Column': 'customer_id',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping - no transformation',
                'Business Rule': 'Unique identifier from source CRM system',
                'SCD Type': 'Type 2',
                'Nullable': 'NOT NULL',
                'Sample Value': 'CUST000001'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'first_name',
                'Target Table': 'dim_customer',
                'Target Column': 'first_name',
                'Data Type': 'VARCHAR(100)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Customer first name',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': 'John'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'last_name',
                'Target Table': 'dim_customer',
                'Target Column': 'last_name',
                'Data Type': 'VARCHAR(100)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Customer last name',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': 'Smith'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'first_name || \' \' || last_name',
                'Target Table': 'dim_customer',
                'Target Column': 'full_name',
                'Data Type': 'VARCHAR(200)',
                'Transformation Logic': 'CONCAT(first_name, \' \', last_name)',
                'Business Rule': 'Full name for display purposes',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': 'John Smith'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'email',
                'Target Table': 'dim_customer',
                'Target Column': 'email',
                'Data Type': 'VARCHAR(255)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Primary contact email',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': 'john.smith@example.com'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'phone_number',
                'Target Table': 'dim_customer',
                'Target Column': 'phone_number',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping - keep original format',
                'Business Rule': 'Primary contact phone',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '555-123-4567'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'date_of_birth',
                'Target Table': 'dim_customer',
                'Target Column': 'date_of_birth',
                'Data Type': 'DATE',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Date of birth for age calculation',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '1985-06-15'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'EXTRACT(YEAR FROM AGE(date_of_birth))',
                'Target Table': 'dim_customer',
                'Target Column': 'age',
                'Data Type': 'INTEGER',
                'Transformation Logic': 'Calculate age from date_of_birth: EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth))',
                'Business Rule': 'Current age in years',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '39'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'CASE WHEN age...',
                'Target Table': 'dim_customer',
                'Target Column': 'age_group',
                'Data Type': 'VARCHAR(20)',
                'Transformation Logic': 'CASE WHEN age < 25 THEN \'18-24\' WHEN age < 35 THEN \'25-34\' WHEN age < 45 THEN \'35-44\' WHEN age < 55 THEN \'45-54\' WHEN age < 65 THEN \'55-64\' ELSE \'65+\' END',
                'Business Rule': 'Age segmentation for marketing',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '35-44'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'address',
                'Target Table': 'dim_customer',
                'Target Column': 'address',
                'Data Type': 'VARCHAR(255)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Street address - triggers SCD Type 2',
                'SCD Type': 'Type 2',
                'Nullable': 'NULL',
                'Sample Value': '123 Main St'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'city',
                'Target Table': 'dim_customer',
                'Target Column': 'city',
                'Data Type': 'VARCHAR(100)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'City - triggers SCD Type 2',
                'SCD Type': 'Type 2',
                'Nullable': 'NULL',
                'Sample Value': 'New York'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'state',
                'Target Table': 'dim_customer',
                'Target Column': 'state',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'State abbreviation - triggers SCD Type 2',
                'SCD Type': 'Type 2',
                'Nullable': 'NULL',
                'Sample Value': 'NY'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'registration_date',
                'Target Table': 'dim_customer',
                'Target Column': 'registration_date',
                'Data Type': 'DATE',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Date customer registered - used as valid_from for initial record',
                'SCD Type': 'Type 1',
                'Nullable': 'NOT NULL',
                'Sample Value': '2023-01-15'
            },
            {
                'Source System': 'CRM',
                'Source Table': 'stg_crm_customers',
                'Source Column': 'registration_date',
                'Target Table': 'dim_customer',
                'Target Column': 'valid_from',
                'Data Type': 'DATE',
                'Transformation Logic': 'For initial load: registration_date. For updates: CURRENT_DATE',
                'Business Rule': 'SCD Type 2 - Start date of this version',
                'SCD Type': 'Type 2',
                'Nullable': 'NOT NULL',
                'Sample Value': '2023-01-15'
            },
            {
                'Source System': 'System',
                'Source Table': 'Derived',
                'Source Column': '9999-12-31',
                'Target Table': 'dim_customer',
                'Target Column': 'valid_to',
                'Data Type': 'DATE',
                'Transformation Logic': 'Current record: 9999-12-31. Historical records: valid_from of next version',
                'Business Rule': 'SCD Type 2 - End date of this version',
                'SCD Type': 'Type 2',
                'Nullable': 'NOT NULL',
                'Sample Value': '9999-12-31'
            },
            {
                'Source System': 'System',
                'Source Table': 'Derived',
                'Source Column': 'valid_to = 9999-12-31',
                'Target Table': 'dim_customer',
                'Target Column': 'is_current',
                'Data Type': 'BOOLEAN',
                'Transformation Logic': 'TRUE if valid_to = 9999-12-31, else FALSE',
                'Business Rule': 'SCD Type 2 - Flag for current version',
                'SCD Type': 'Type 2',
                'Nullable': 'NOT NULL',
                'Sample Value': 'TRUE'
            },
            {
                'Source System': 'System',
                'Source Table': 'Derived from fact_transactions',
                'Source Column': 'COUNT(DISTINCT transaction_id)',
                'Target Table': 'dim_customer',
                'Target Column': 'total_orders',
                'Data Type': 'INTEGER',
                'Transformation Logic': 'Updated post-load: COUNT(DISTINCT transaction_id) FROM fact_transactions',
                'Business Rule': 'Total number of orders placed by customer',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '15'
            },
            {
                'Source System': 'System',
                'Source Table': 'Derived from fact_transactions',
                'Source Column': 'SUM(net_amount)',
                'Target Table': 'dim_customer',
                'Target Column': 'total_spent',
                'Data Type': 'DECIMAL(15,2)',
                'Transformation Logic': 'Updated post-load: SUM(net_amount) FROM fact_transactions',
                'Business Rule': 'Customer lifetime value',
                'SCD Type': 'Type 1',
                'Nullable': 'NULL',
                'Sample Value': '2450.75'
            },
        ]
        
        df = pd.DataFrame(mapping_data)
        
        # Create Excel with multiple sheets
        output_file = self.output_dir / 'CustomerDimensionMapping.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Field Mapping
            df.to_excel(writer, sheet_name='Field Mappings', index=False)
            
            # Sheet 2: SCD Logic
            scd_info = pd.DataFrame([
                {
                    'SCD Type': 'Type 1',
                    'Description': 'Overwrite - latest value replaces old value',
                    'Fields': 'first_name, last_name, email, phone_number, age, age_group, account_status',
                    'Reason': 'These attributes don\'t require historical tracking'
                },
                {
                    'SCD Type': 'Type 2',
                    'Description': 'Historical tracking - new row created for changes',
                    'Fields': 'customer_id, address, city, state, postal_code',
                    'Reason': 'Address changes need to be tracked for historical transaction analysis'
                },
                {
                    'SCD Type': 'Metadata',
                    'Description': 'System-generated fields for SCD Type 2',
                    'Fields': 'valid_from, valid_to, is_current',
                    'Reason': 'Required for temporal queries and current record identification'
                }
            ])
            scd_info.to_excel(writer, sheet_name='SCD Logic', index=False)
            
            # Sheet 3: Business Rules
            rules = pd.DataFrame([
                {'Rule': 'Unique Customer', 'Logic': 'customer_id must be unique per version (valid_from)', 'Impact': 'Primary key constraint'},
                {'Rule': 'Address Changes', 'Logic': 'Changes to address/city/state trigger new SCD Type 2 version', 'Impact': 'New row inserted, previous row updated'},
                {'Rule': 'Valid From Date', 'Logic': 'Initial load: registration_date. Updates: CURRENT_DATE', 'Impact': 'Ensures temporal accuracy'},
                {'Rule': 'Current Flag', 'Logic': 'Only one is_current=TRUE per customer_id', 'Impact': 'Easy filtering for current data'},
                {'Rule': 'Lifetime Metrics', 'Logic': 'total_orders and total_spent updated post-load from fact tables', 'Impact': 'Pre-calculated for performance'},
                {'Rule': 'Age Calculation', 'Logic': 'Derived from date_of_birth using AGE() function', 'Impact': 'Always current age, not stored age'},
            ])
            rules.to_excel(writer, sheet_name='Business Rules', index=False)
            
            # Sheet 4: Data Lineage
            lineage = pd.DataFrame([
                {'Step': '1', 'Process': 'Extraction', 'Description': 'CRM data extracted to CSV files', 'Frequency': 'Daily'},
                {'Step': '2', 'Process': 'Staging Load', 'Description': 'CSV files loaded to stg_crm_customers', 'Frequency': 'Daily'},
                {'Step': '3', 'Process': 'Transformation', 'Description': 'SCD Type 2 logic applied, derived fields calculated', 'Frequency': 'Daily'},
                {'Step': '4', 'Process': 'Dimension Load', 'Description': 'Upsert to dim_customer table', 'Frequency': 'Daily'},
                {'Step': '5', 'Process': 'Post-Load Update', 'Description': 'Update lifetime metrics from fact_transactions', 'Frequency': 'Daily'},
            ])
            lineage.to_excel(writer, sheet_name='Data Lineage', index=False)
        
        logger.info(f"✓ Customer dimension mapping created: {output_file}")
        return output_file
    
    def create_transaction_fact_mapping(self) -> Path:
        """Create transaction fact table mapping"""
        logger.info("Creating Transaction Fact mapping...")
        
        mapping_data = [
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'transaction_item_id',
                'Target Table': 'fact_transactions',
                'Target Column': 'transaction_item_id',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Unique identifier for each line item',
                'Grain': 'Transaction Line Item',
                'Sample Value': 'TXI000001'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'transaction_id',
                'Target Table': 'fact_transactions',
                'Target Column': 'transaction_id',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping - Degenerate dimension',
                'Business Rule': 'Transaction header ID',
                'Grain': 'Transaction Line Item',
                'Sample Value': 'TXN000001'
            },
            {
                'Source System': 'E-Commerce + Lookup',
                'Source Table': 'stg_ecom_transactions.customer_id → dim_customer',
                'Source Column': 'JOIN on customer_id',
                'Target Table': 'fact_transactions',
                'Target Column': 'customer_key',
                'Data Type': 'BIGINT',
                'Transformation Logic': 'SQL JOIN: Find customer_key WHERE customer_id matches AND transaction_date BETWEEN valid_from AND valid_to',
                'Business Rule': 'Foreign key to customer dimension (SCD Type 2 aware)',
                'Grain': 'Transaction Line Item',
                'Sample Value': '12345'
            },
            {
                'Source System': 'E-Commerce + Lookup',
                'Source Table': 'stg_ecom_transaction_items.product_id → dim_product',
                'Source Column': 'JOIN on product_id',
                'Target Table': 'fact_transactions',
                'Target Column': 'product_key',
                'Data Type': 'BIGINT',
                'Transformation Logic': 'SQL JOIN: Find product_key WHERE product_id matches',
                'Business Rule': 'Foreign key to product dimension',
                'Grain': 'Transaction Line Item',
                'Sample Value': '567'
            },
            {
                'Source System': 'E-Commerce + Lookup',
                'Source Table': 'stg_ecom_transactions.transaction_date → dim_date',
                'Source Column': 'JOIN on date',
                'Target Table': 'fact_transactions',
                'Target Column': 'transaction_date_key',
                'Data Type': 'INTEGER',
                'Transformation Logic': 'SQL JOIN: Find date_key WHERE date_actual = transaction_date::date',
                'Business Rule': 'Foreign key to date dimension',
                'Grain': 'Transaction Line Item',
                'Sample Value': '20230115'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transactions',
                'Source Column': 'transaction_date',
                'Target Table': 'fact_transactions',
                'Target Column': 'transaction_datetime',
                'Data Type': 'TIMESTAMP',
                'Transformation Logic': 'Direct mapping from header table',
                'Business Rule': 'Full timestamp for detailed analysis',
                'Grain': 'Transaction Line Item',
                'Sample Value': '2023-01-15 14:30:25'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'quantity',
                'Target Table': 'fact_transactions',
                'Target Column': 'quantity',
                'Data Type': 'INTEGER',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Number of units purchased',
                'Grain': 'Transaction Line Item',
                'Sample Value': '2'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'unit_price',
                'Target Table': 'fact_transactions',
                'Target Column': 'unit_price',
                'Data Type': 'DECIMAL(10,2)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Price per unit at time of sale',
                'Grain': 'Transaction Line Item',
                'Sample Value': '29.99'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'line_total',
                'Target Table': 'fact_transactions',
                'Target Column': 'line_total',
                'Data Type': 'DECIMAL(10,2)',
                'Transformation Logic': 'Direct mapping (should equal quantity * unit_price)',
                'Business Rule': 'Total for this line item before discounts',
                'Grain': 'Transaction Line Item',
                'Sample Value': '59.98'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transaction_items',
                'Source Column': 'discount_amount',
                'Target Table': 'fact_transactions',
                'Target Column': 'discount_amount',
                'Data Type': 'DECIMAL(10,2)',
                'Transformation Logic': 'Direct mapping',
                'Business Rule': 'Discount applied to this line item',
                'Grain': 'Transaction Line Item',
                'Sample Value': '5.00'
            },
            {
                'Source System': 'Calculated',
                'Source Table': 'line_total - discount_amount',
                'Source Column': 'Derived',
                'Target Table': 'fact_transactions',
                'Target Column': 'net_amount',
                'Data Type': 'DECIMAL(10,2)',
                'Transformation Logic': 'line_total - discount_amount',
                'Business Rule': 'Final amount for this line item (used for revenue calculations)',
                'Grain': 'Transaction Line Item',
                'Sample Value': '54.98'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transactions',
                'Source Column': 'order_number',
                'Target Table': 'fact_transactions',
                'Target Column': 'order_number',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping - Degenerate dimension',
                'Business Rule': 'Customer-facing order number',
                'Grain': 'Transaction Line Item',
                'Sample Value': 'ORD-2023-0001'
            },
            {
                'Source System': 'E-Commerce',
                'Source Table': 'stg_ecom_transactions',
                'Source Column': 'payment_method',
                'Target Table': 'fact_transactions',
                'Target Column': 'payment_method',
                'Data Type': 'VARCHAR(50)',
                'Transformation Logic': 'Direct mapping - Degenerate dimension',
                'Business Rule': 'Payment method used',
                'Grain': 'Transaction Line Item',
                'Sample Value': 'Credit Card'
            },
        ]
        
        df = pd.DataFrame(mapping_data)
        
        output_file = self.output_dir / 'TransactionFactMapping.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Field Mapping
            df.to_excel(writer, sheet_name='Field Mappings', index=False)
            
            # Sheet 2: Grain Definition
            grain = pd.DataFrame([
                {'Aspect': 'Grain', 'Definition': 'One row per transaction line item', 'Example': 'If order has 3 products, 3 rows in fact table'},
                {'Aspect': 'Fact Type', 'Definition': 'Transaction Fact (Accumulating Snapshot)', 'Example': 'Records business events as they occur'},
                {'Aspect': 'Additivity', 'Definition': 'All measures are fully additive', 'Example': 'quantity, line_total, discount_amount, net_amount can be summed across all dimensions'},
                {'Aspect': 'Dimensions', 'Definition': 'Customer (SCD Type 2), Product, Date, Transaction (degenerate)', 'Example': 'Multi-dimensional analysis supported'},
            ])
            grain.to_excel(writer, sheet_name='Grain Definition', index=False)
            
            # Sheet 3: Join Logic
            joins = pd.DataFrame([
                {
                    'Join': 'Customer Lookup',
                    'Logic': 'LEFT JOIN dim_customer ON stg.customer_id = dim.customer_id AND stg.transaction_date BETWEEN dim.valid_from AND dim.valid_to AND dim.is_current = TRUE',
                    'Purpose': 'Get correct customer version based on transaction date (SCD Type 2)',
                    'Null Handling': 'Allow NULL if customer not found (orphaned transaction)'
                },
                {
                    'Join': 'Product Lookup',
                    'Logic': 'LEFT JOIN dim_product ON stg.product_id = dim.product_id',
                    'Purpose': 'Get product surrogate key (SCD Type 1)',
                    'Null Handling': 'Allow NULL if product not found'
                },
                {
                    'Join': 'Date Lookup',
                    'Logic': 'LEFT JOIN dim_date ON stg.transaction_date::date = dim.date_actual',
                    'Purpose': 'Get date dimension surrogate key',
                    'Null Handling': 'Should not be NULL (date dimension is pre-populated)'
                },
            ])
            joins.to_excel(writer, sheet_name='Join Logic', index=False)
            
            # Sheet 4: Measure Calculations
            measures = pd.DataFrame([
                {'Measure': 'Total Revenue', 'SQL': 'SUM(net_amount)', 'Description': 'Total revenue after discounts'},
                {'Measure': 'Total Units Sold', 'SQL': 'SUM(quantity)', 'Description': 'Total quantity sold'},
                {'Measure': 'Average Order Value', 'SQL': 'AVG(net_amount)', 'Description': 'Average revenue per line item'},
                {'Measure': 'Total Discounts', 'SQL': 'SUM(discount_amount)', 'Description': 'Total discounts given'},
                {'Measure': 'Discount Percentage', 'SQL': 'SUM(discount_amount) / SUM(line_total) * 100', 'Description': 'Percentage of revenue discounted'},
                {'Measure': 'Transaction Count', 'SQL': 'COUNT(DISTINCT transaction_id)', 'Description': 'Number of unique transactions'},
                {'Measure': 'Line Item Count', 'SQL': 'COUNT(*)', 'Description': 'Total number of line items'},
            ])
            measures.to_excel(writer, sheet_name='Measure Calculations', index=False)
        
        logger.info(f"✓ Transaction fact mapping created: {output_file}")
        return output_file
    
    def create_mapping_template(self) -> Path:
        """Create blank mapping template for future use"""
        logger.info("Creating mapping template...")
        
        template_data = pd.DataFrame({
            'Source System': [''],
            'Source Table': [''],
            'Source Column': [''],
            'Target Table': [''],
            'Target Column': [''],
            'Data Type': [''],
            'Transformation Logic': [''],
            'Business Rule': [''],
            'SCD Type': [''],
            'Nullable': [''],
            'Sample Value': ['']
        })
        
        output_file = self.output_dir / 'MappingTemplate.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            template_data.to_excel(writer, sheet_name='Field Mappings', index=False)
            
            # Instructions sheet
            instructions = pd.DataFrame([
                {'Column': 'Source System', 'Description': 'Name of source system (e.g., CRM, ERP, E-Commerce)'},
                {'Column': 'Source Table', 'Description': 'Staging table name'},
                {'Column': 'Source Column', 'Description': 'Source column name or expression'},
                {'Column': 'Target Table', 'Description': 'Warehouse table name (dimension or fact)'},
                {'Column': 'Target Column', 'Description': 'Target column name'},
                {'Column': 'Data Type', 'Description': 'PostgreSQL data type (e.g., VARCHAR(50), INTEGER, DECIMAL(10,2))'},
                {'Column': 'Transformation Logic', 'Description': 'SQL logic or description of transformation'},
                {'Column': 'Business Rule', 'Description': 'Business meaning and rules'},
                {'Column': 'SCD Type', 'Description': 'Type 1 (overwrite), Type 2 (history), or N/A'},
                {'Column': 'Nullable', 'Description': 'NULL or NOT NULL'},
                {'Column': 'Sample Value', 'Description': 'Example value for reference'},
            ])
            instructions.to_excel(writer, sheet_name='Instructions', index=False)
        
        logger.info(f"✓ Mapping template created: {output_file}")
        return output_file


def main():
    """Main execution"""
    generator = MappingDocGenerator()
    
    # Generate mapping documents
    customer_file = generator.create_customer_dimension_mapping()
    logger.info(f"✓ Customer mapping: {customer_file}")
    
    transaction_file = generator.create_transaction_fact_mapping()
    logger.info(f"✓ Transaction mapping: {transaction_file}")
    
    template_file = generator.create_mapping_template()
    logger.info(f"✓ Template: {template_file}")
    
    logger.info("✓ All mapping documentation generated successfully")


if __name__ == "__main__":
    main()
