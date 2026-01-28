"""
Synthetic Data Generator
Generates realistic customer, transaction, and campaign data
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from pathlib import Path
from typing import Tuple

import sys
sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)


class DataGenerator:
    """Generate synthetic business data for the data warehouse project"""
    
    def __init__(self, seed: int = 42):
        self.config = get_config()
        self.seed = seed
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
        
        self.output_dir = Path(self.config.get('paths.raw_data'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"DataGenerator initialized with seed {seed}")
    
    def generate_customers(self, count: int = None) -> pd.DataFrame:
        """
        Generate synthetic customer data
        
        Intentionally includes data quality issues:
        - Some missing emails
        - Duplicate customers
        - Inconsistent phone formats
        """
        count = count or self.config.get('data_generation.customers.count', 50000)
        logger.info(f"Generating {count} customers...")
        
        start_date = datetime.strptime(
            self.config.get('data_generation.customers.start_date', '2020-01-01'), 
            '%Y-%m-%d'
        )
        end_date = datetime.strptime(
            self.config.get('data_generation.customers.end_date', '2026-01-26'),
            '%Y-%m-%d'
        )
        
        customers = []
        
        for i in range(count):
            # Generate customer data
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            
            # Intentional data quality issues (5% missing emails)
            email = self.fake.email() if random.random() > 0.05 else None
            
            # Inconsistent phone formats
            phone_formats = [
                self.fake.phone_number(),
                self.fake.phone_number().replace('x', ''),
                self.fake.numerify('###-###-####'),
                None  # 2% missing
            ]
            phone = random.choice(phone_formats) if random.random() > 0.02 else None
            
            registration_date = self.fake.date_between(
                start_date=start_date,
                end_date=end_date
            )
            
            customer = {
                'customer_id': f'CUST{i+1:06d}',
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone_number': phone,
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'gender': random.choice(['Male', 'Female', 'Other', None]),
                'address': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state_abbr(),
                'postal_code': self.fake.postcode(),
                'country': 'USA',
                'registration_date': registration_date,
                'customer_segment': random.choice([
                    'Premium', 'Standard', 'Basic', 'VIP'
                ]),
                'account_status': random.choice([
                    'Active', 'Active', 'Active', 'Inactive', 'Suspended'
                ])  # Weighted toward Active
            }
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        
        # Add intentional duplicates (1%)
        duplicate_count = int(count * 0.01)
        duplicates = df.sample(n=duplicate_count, random_state=self.seed)
        df = pd.concat([df, duplicates], ignore_index=True)
        
        logger.info(f"Generated {len(df)} customer records (includes {duplicate_count} duplicates)")
        return df
    
    def generate_products(self, count: int = 500) -> pd.DataFrame:
        """Generate synthetic product catalog"""
        logger.info(f"Generating {count} products...")
        
        categories = {
            'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
            'Clothing': ['Men', 'Women', 'Kids', 'Accessories'],
            'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding'],
            'Sports': ['Equipment', 'Apparel', 'Accessories', 'Footwear'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children']
        }
        
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 
                  'Generic', 'Premium', 'Value']
        
        products = []
        
        for i in range(count):
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            
            unit_cost = round(random.uniform(5, 500), 2)
            markup = random.uniform(1.2, 2.5)  # 20% to 150% markup
            retail_price = round(unit_cost * markup, 2)
            
            product = {
                'product_id': f'PROD{i+1:05d}',
                'product_name': f'{self.fake.catch_phrase()} {subcategory}',
                'product_category': category,
                'product_subcategory': subcategory,
                'brand': random.choice(brands),
                'unit_cost': unit_cost,
                'retail_price': retail_price,
                'product_status': random.choice(['Active', 'Active', 'Active', 'Discontinued'])
            }
            products.append(product)
        
        df = pd.DataFrame(products)
        logger.info(f"Generated {len(df)} products")
        return df
    
    def generate_transactions(
        self, 
        customers: pd.DataFrame, 
        products: pd.DataFrame,
        count: int = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate synthetic transactions with line items
        
        Returns:
            Tuple of (transactions_df, transaction_items_df)
        """
        count = count or self.config.get('data_generation.transactions.count', 200000)
        logger.info(f"Generating {count} transactions...")
        
        start_date = datetime.strptime(
            self.config.get('data_generation.customers.start_date', '2020-01-01'),
            '%Y-%m-%d'
        )
        end_date = datetime.strptime(
            self.config.get('data_generation.customers.end_date', '2026-01-26'),
            '%Y-%m-%d'
        )
        
        # Get active customers only
        active_customers = customers[
            customers['account_status'].isin(['Active', 'VIP'])
        ]['customer_id'].tolist()
        
        active_products = products[
            products['product_status'] == 'Active'
        ]
        
        transactions = []
        transaction_items = []
        
        for i in range(count):
            customer_id = random.choice(active_customers)
            transaction_date = self.fake.date_time_between(
                start_date=start_date,
                end_date=end_date
            )
            
            # Number of items in transaction (1-5, weighted toward 1-2)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
            
            transaction_id = f'TXN{i+1:08d}'
            order_number = f'ORD{i+1:08d}'
            
            # Select products for this transaction
            selected_products = active_products.sample(
                n=min(num_items, len(active_products)),
                random_state=i
            )
            
            subtotal = 0
            items = []
            
            for idx, product in selected_products.iterrows():
                quantity = random.randint(1, 3)
                unit_price = product['retail_price']
                
                # Random discount (20% chance)
                discount = 0
                if random.random() < 0.2:
                    discount = round(unit_price * quantity * random.uniform(0.05, 0.2), 2)
                
                line_total = round((unit_price * quantity) - discount, 2)
                subtotal += line_total
                
                item = {
                    'transaction_item_id': f'{transaction_id}_ITEM{len(items)+1}',
                    'transaction_id': transaction_id,
                    'product_id': product['product_id'],
                    'product_name': product['product_name'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'line_total': line_total,
                    'discount_amount': discount
                }
                items.append(item)
            
            # Calculate totals
            tax_rate = 0.08
            tax_amount = round(subtotal * tax_rate, 2)
            shipping_amount = round(random.uniform(0, 20), 2) if subtotal < 100 else 0
            total_amount = subtotal + tax_amount + shipping_amount
            
            transaction = {
                'transaction_id': transaction_id,
                'customer_id': customer_id,
                'transaction_date': transaction_date,
                'order_number': order_number,
                'payment_method': random.choice([
                    'Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay'
                ]),
                'payment_status': random.choice([
                    'Completed', 'Completed', 'Completed', 'Pending', 'Failed'
                ]),
                'total_amount': round(total_amount, 2),
                'tax_amount': tax_amount,
                'shipping_amount': shipping_amount,
                'discount_amount': sum(item['discount_amount'] for item in items),
                'currency_code': 'USD'
            }
            
            transactions.append(transaction)
            transaction_items.extend(items)
        
        transactions_df = pd.DataFrame(transactions)
        items_df = pd.DataFrame(transaction_items)
        
        logger.info(f"Generated {len(transactions_df)} transactions with {len(items_df)} line items")
        return transactions_df, items_df
    
    def generate_campaigns(self, count: int = 25) -> pd.DataFrame:
        """Generate marketing campaign data"""
        logger.info(f"Generating {count} campaigns...")
        
        campaign_types = ['Email', 'Social Media', 'Display Ads', 'Search Ads', 'Influencer']
        channels = ['Email', 'Facebook', 'Instagram', 'Google', 'LinkedIn', 'Twitter']
        audiences = ['All Customers', 'Premium', 'New Customers', 'Inactive', 'High Value']
        
        campaigns = []
        
        for i in range(count):
            start_date = self.fake.date_between(start_date='-2y', end_date='today')
            duration = random.randint(7, 90)
            end_date = start_date + timedelta(days=duration)
            
            campaign = {
                'campaign_id': f'CAMP{i+1:04d}',
                'campaign_name': f'{self.fake.catch_phrase()} Campaign',
                'campaign_type': random.choice(campaign_types),
                'channel': random.choice(channels),
                'start_date': start_date,
                'end_date': end_date,
                'budget': round(random.uniform(1000, 50000), 2),
                'target_audience': random.choice(audiences),
                'campaign_status': random.choice([
                    'Completed', 'Completed', 'Active', 'Planned'
                ])
            }
            campaigns.append(campaign)
        
        df = pd.DataFrame(campaigns)
        logger.info(f"Generated {len(df)} campaigns")
        return df
    
    def generate_campaign_responses(
        self,
        campaigns: pd.DataFrame,
        customers: pd.DataFrame
    ) -> pd.DataFrame:
        """Generate campaign response data"""
        logger.info("Generating campaign responses...")
        
        response_rate = self.config.get('data_generation.campaigns.response_rate', 0.15)
        
        responses = []
        
        for _, campaign in campaigns.iterrows():
            if campaign['campaign_status'] not in ['Completed', 'Active']:
                continue
            
            # Sample customers who received the campaign
            num_recipients = random.randint(1000, 10000)
            num_responders = int(num_recipients * response_rate)
            
            responding_customers = customers.sample(
                n=min(num_responders, len(customers))
            )
            
            for idx, customer in responding_customers.iterrows():
                response_date = self.fake.date_between(
                    start_date=campaign['start_date'],
                    end_date=min(campaign['end_date'], datetime.now().date())
                )
                
                # Response funnel
                response_types = ['opened', 'clicked', 'converted']
                response_type = random.choice(response_types)
                
                conversion_value = 0
                if response_type == 'converted':
                    conversion_value = round(random.uniform(20, 500), 2)
                
                response = {
                    'response_id': f"RESP{len(responses)+1:08d}",
                    'campaign_id': campaign['campaign_id'],
                    'customer_id': customer['customer_id'],
                    'response_date': response_date,
                    'response_type': response_type,
                    'conversion_value': conversion_value
                }
                responses.append(response)
        
        df = pd.DataFrame(responses)
        logger.info(f"Generated {len(df)} campaign responses")
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """Save DataFrame to CSV with timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d')
        filepath = self.output_dir / f"{filename}_{timestamp}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} rows to {filepath}")
    
    def generate_all(self) -> None:
        """Generate all datasets and save to CSV"""
        logger.info("Starting full data generation pipeline...")
        
        # Generate master data
        customers = self.generate_customers()
        products = self.generate_products()
        campaigns = self.generate_campaigns()
        
        # Generate transactional data
        transactions, transaction_items = self.generate_transactions(customers, products)
        campaign_responses = self.generate_campaign_responses(campaigns, customers)
        
        # Save all to CSV
        self.save_to_csv(customers, 'crm_customers')
        self.save_to_csv(products, 'products')
        self.save_to_csv(transactions, 'ecom_transactions')
        self.save_to_csv(transaction_items, 'ecom_transaction_items')
        self.save_to_csv(campaigns, 'marketing_campaigns')
        self.save_to_csv(campaign_responses, 'campaign_responses')
        
        logger.info("✓ Data generation completed successfully!")


def main():
    """Main execution function"""
    generator = DataGenerator()
    generator.generate_all()


if __name__ == "__main__":
    main()