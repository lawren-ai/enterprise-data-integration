"""
Database Setup Script
Creates database and initializes schema
Run this FIRST before any other scripts
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import configparser
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from utils.logger import get_logger

logger = get_logger(__name__)


def read_db_config():
    """Read database configuration"""
    config = configparser.ConfigParser()
    config_path = Path('config/database.ini')
    
    if not config_path.exists():
        logger.error("database.ini not found. Please copy database.ini.template and configure it.")
        sys.exit(1)
    
    config.read(config_path)
    return config['postgresql']


def create_database():
    """Create the enterprise_dw database"""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP - Step 1: Create Database")
    logger.info("=" * 60)
    
    db_config = read_db_config()
    
    # Connect to PostgreSQL server (not to a specific database)
    try:
        # Connect to 'postgres' maintenance database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database='postgres'  # Connect to default postgres database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        logger.info("Connected to PostgreSQL server")
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = 'enterprise_dw'"
        )
        exists = cursor.fetchone()
        
        if exists:
            logger.warning("Database 'enterprise_dw' already exists")
            response = input("Do you want to drop and recreate it? (yes/no): ")
            
            if response.lower() == 'yes':
                logger.info("Terminating existing connections...")
                cursor.execute("""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = 'enterprise_dw'
                      AND pid <> pg_backend_pid()
                """)
                
                logger.info("Dropping database...")
                cursor.execute("DROP DATABASE enterprise_dw")
                logger.info("✓ Database dropped")
            else:
                logger.info("Using existing database")
                cursor.close()
                conn.close()
                return True
        
        # Create database
        logger.info("Creating database 'enterprise_dw'...")
        cursor.execute("CREATE DATABASE enterprise_dw")
        logger.info("✓ Database created successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False


def create_schemas():
    """Create database schemas"""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP - Step 2: Create Schemas")
    logger.info("=" * 60)
    
    db_config = read_db_config()
    
    try:
        # Connect to the enterprise_dw database
        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/enterprise_dw"
        )
        engine = create_engine(conn_string)
        
        # Create schemas
        schemas = ['staging', 'warehouse', 'quality']
        
        with engine.begin() as conn:  # Using begin() for auto-commit
            for schema in schemas:
                logger.info(f"Creating schema: {schema}")
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                logger.info(f"✓ Schema '{schema}' created")
        
        logger.info("✓ All schemas created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create schemas: {e}")
        return False


def execute_ddl_file(file_path: Path, description: str):
    """Execute a DDL SQL file"""
    logger.info(f"Executing {description}...")
    
    db_config = read_db_config()
    
    try:
        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/enterprise_dw"
        )
        engine = create_engine(conn_string)
        
        # Read SQL file
        with open(file_path, 'r') as f:
            sql = f.read()
        
        # Split by statement and execute
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        with engine.begin() as conn:  # Using begin() for auto-commit
            for i, stmt in enumerate(statements, 1):
                if stmt.strip():
                    try:
                        conn.execute(text(stmt))
                    except Exception as e:
                        logger.warning(f"Statement {i} warning: {e}")
                        # Continue with other statements
        
        logger.info(f"✓ {description} completed")
        return True
        
    except Exception as e:
        logger.error(f"Failed to execute {description}: {e}")
        return False


def initialize_schema():
    """Initialize database schema (staging and warehouse tables)"""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP - Step 3: Initialize Schema")
    logger.info("=" * 60)
    
    sql_dir = Path('sql/ddl')
    
    # Execute DDL files in order
    ddl_files = [
        (sql_dir / '01_staging_schema.sql', 'Staging schema'),
        (sql_dir / '02_warehouse_schema.sql', 'Warehouse schema'),
    ]
    
    for file_path, description in ddl_files:
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        
        if not execute_ddl_file(file_path, description):
            return False
    
    logger.info("✓ Schema initialization completed")
    return True


def populate_date_dimension():
    """Populate the date dimension with calendar data"""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP - Step 4: Populate Date Dimension")
    logger.info("=" * 60)
    
    db_config = read_db_config()
    
    try:
        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/enterprise_dw"
        )
        engine = create_engine(conn_string)
        
        # Generate date dimension data year by year to avoid statement length issues
        start_year = 2020
        end_year = 2030
        total_rows = 0
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Populating dates for year {year}...")
            
            sql = f"""
            INSERT INTO dim_date (
                date_key,
                date_actual,
                day_of_week,
                day_name,
                day_of_month,
                day_of_year,
                week_of_year,
                week_start_date,
                week_end_date,
                month_number,
                month_name,
                month_abbr,
                month_start_date,
                month_end_date,
                quarter_number,
                quarter_name,
                quarter_start_date,
                quarter_end_date,
                year_number,
                fiscal_year,
                fiscal_quarter,
                fiscal_month,
                is_weekend,
                is_holiday,
                holiday_name,
                is_business_day
            )
            SELECT
                TO_CHAR(date_actual, 'YYYYMMDD')::INTEGER AS date_key,
                date_actual,
                EXTRACT(DOW FROM date_actual)::INTEGER AS day_of_week,
                TO_CHAR(date_actual, 'Day') AS day_name,
                EXTRACT(DAY FROM date_actual)::INTEGER AS day_of_month,
                EXTRACT(DOY FROM date_actual)::INTEGER AS day_of_year,
                EXTRACT(WEEK FROM date_actual)::INTEGER AS week_of_year,
                DATE_TRUNC('week', date_actual)::DATE AS week_start_date,
                (DATE_TRUNC('week', date_actual) + INTERVAL '6 days')::DATE AS week_end_date,
                EXTRACT(MONTH FROM date_actual)::INTEGER AS month_number,
                TO_CHAR(date_actual, 'Month') AS month_name,
                TO_CHAR(date_actual, 'Mon') AS month_abbr,
                DATE_TRUNC('month', date_actual)::DATE AS month_start_date,
                (DATE_TRUNC('month', date_actual) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end_date,
                EXTRACT(QUARTER FROM date_actual)::INTEGER AS quarter_number,
                'Q' || EXTRACT(QUARTER FROM date_actual)::TEXT AS quarter_name,
                DATE_TRUNC('quarter', date_actual)::DATE AS quarter_start_date,
                (DATE_TRUNC('quarter', date_actual) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS quarter_end_date,
                EXTRACT(YEAR FROM date_actual)::INTEGER AS year_number,
                EXTRACT(YEAR FROM date_actual)::INTEGER AS fiscal_year,
                EXTRACT(QUARTER FROM date_actual)::INTEGER AS fiscal_quarter,
                EXTRACT(MONTH FROM date_actual)::INTEGER AS fiscal_month,
                CASE WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                FALSE AS is_holiday,
                NULL AS holiday_name,
                CASE WHEN EXTRACT(DOW FROM date_actual) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_business_day
            FROM (
                SELECT date_actual
                FROM GENERATE_SERIES('{year}-01-01'::DATE, '{year}-12-31'::DATE, '1 day'::INTERVAL) AS date_actual
            ) dates
            ON CONFLICT (date_key) DO NOTHING;
            """
            
            with engine.begin() as conn:
                result = conn.execute(text(sql))
                rows = result.rowcount if result.rowcount != -1 else 0
                total_rows += rows
        
        logger.info(f"✓ Date dimension populated with {total_rows:,} records")
        return True
        
    except Exception as e:
        logger.error(f"Failed to populate date dimension: {e}")
        return False


def verify_setup():
    """Verify database setup"""
    logger.info("=" * 60)
    logger.info("DATABASE SETUP - Step 5: Verification")
    logger.info("=" * 60)
    
    db_config = read_db_config()
    
    try:
        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/enterprise_dw"
        )
        engine = create_engine(conn_string)
        
        with engine.connect() as conn:
            # Check staging tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name LIKE 'stg_%'
                ORDER BY table_name
            """))
            staging_tables = [row[0] for row in result]
            
            logger.info(f"Staging tables created: {len(staging_tables)}")
            for table in staging_tables:
                logger.info(f"  - {table}")
            
            # Check warehouse tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND (table_name LIKE 'dim_%' OR table_name LIKE 'fact_%')
                ORDER BY table_name
            """))
            warehouse_tables = [row[0] for row in result]
            
            logger.info(f"Warehouse tables created: {len(warehouse_tables)}")
            for table in warehouse_tables:
                logger.info(f"  - {table}")
            
            # Check date dimension
            result = conn.execute(text("SELECT COUNT(*) FROM dim_date"))
            date_count = result.fetchone()[0]
            logger.info(f"Date dimension records: {date_count:,}")
            
        logger.info("✓ Database verification completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False


def main():
    """Main setup function"""
    logger.info("=" * 60)
    logger.info("ENTERPRISE DATA WAREHOUSE - DATABASE SETUP")
    logger.info("=" * 60)
    
    # Step 1: Create database
    if not create_database():
        logger.error("Database creation failed. Exiting.")
        sys.exit(1)
    
    # Step 2: Create schemas
    if not create_schemas():
        logger.error("Schema creation failed. Exiting.")
        sys.exit(1)
    
    # Step 3: Initialize schema (create tables)
    if not initialize_schema():
        logger.error("Schema initialization failed. Exiting.")
        sys.exit(1)
    
    # Step 4: Populate date dimension
    if not populate_date_dimension():
        logger.error("Date dimension population failed. Exiting.")
        sys.exit(1)
    
    # Step 5: Verify setup
    if not verify_setup():
        logger.error("Verification failed.")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("✓ DATABASE SETUP COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Run: python src/data_generation/generate_data.py")
    logger.info("2. Run: python src/ingestion/load_staging.py --all")
    logger.info("")


if __name__ == "__main__":
    main()