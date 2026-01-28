"""
Database Manager Utility
Handles database connections and common operations
"""

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
import pandas as pd
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

from .config_loader import get_config
from .logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, db_type: str = None):
        self.config = get_config()
        self.db_type = db_type or self.config.get('database.type', 'postgresql')
        self._engine = None
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine"""
        if self._engine is None:
            conn_string = self.config.get_db_connection_string(self.db_type)
            self._engine = create_engine(
                conn_string,
                poolclass=NullPool,  # Disable connection pooling for simplicity
                echo=False  # Set to True for SQL debugging
            )
            logger.info(f"Database engine created for {self.db_type}")
        return self._engine
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        with self.engine.begin() as conn:
            yield conn
    
    def execute_sql(self, sql: str, params: Dict[str, Any] = None) -> Any:
        """Execute SQL statement"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(sql), params or {})
                logger.debug(f"Executed SQL: {sql[:100]}...")
                return result
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise
    
    def execute_sql_file(self, file_path: str) -> None:
        """Execute SQL from a file"""
        logger.info(f"Executing SQL file: {file_path}")
        
        with open(file_path, 'r') as f:
            sql = f.read()
        
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        with self.engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        
        logger.info(f"Successfully executed {len(statements)} statements")
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """Check if table exists"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names(schema=schema)
    
    def get_table_row_count(self, table_name: str, schema: str = None) -> int:
        """Get row count for a table"""
        full_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT COUNT(*) as cnt FROM {full_name}"
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()
    
    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        schema: str = None,
        if_exists: str = 'append',
        chunksize: int = 1000
    ) -> int:
        """
        Load pandas DataFrame to database table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Database schema
            if_exists: 'fail', 'replace', 'append'
            chunksize: Rows per batch
        
        Returns:
            Number of rows loaded
        """
        try:
            rows_loaded = df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method=None  # Use default method to avoid parameter limits
            )
            
            logger.info(
                f"Loaded {len(df)} rows to {schema}.{table_name if schema else table_name}"
            )
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load DataFrame: {e}")
            raise
    
    def read_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            logger.debug(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def read_table(self, table_name: str, schema: str = None) -> pd.DataFrame:
        """Read entire table into DataFrame"""
        full_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT * FROM {full_name}"
        return self.read_query(query)
    
    def truncate_table(self, table_name: str, schema: str = None) -> None:
        """Truncate a table"""
        full_name = f"{schema}.{table_name}" if schema else table_name
        self.execute_sql(f"TRUNCATE TABLE {full_name}")
        logger.info(f"Truncated table: {full_name}")
    
    def drop_table(self, table_name: str, schema: str = None, if_exists: bool = True) -> None:
        """Drop a table"""
        full_name = f"{schema}.{table_name}" if schema else table_name
        if_exists_clause = "IF EXISTS" if if_exists else ""
        self.execute_sql(f"DROP TABLE {if_exists_clause} {full_name}")
        logger.info(f"Dropped table: {full_name}")
    
    def get_column_names(self, table_name: str, schema: str = None) -> List[str]:
        """Get list of column names for a table"""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema=schema)
        return [col['name'] for col in columns]
    
    def vacuum_analyze(self, table_name: str = None, schema: str = None) -> None:
        """Run VACUUM and ANALYZE (PostgreSQL specific)"""
        if self.db_type != 'postgresql':
            logger.warning("VACUUM ANALYZE only supported on PostgreSQL")
            return
        
        if table_name:
            full_name = f"{schema}.{table_name}" if schema else table_name
            self.execute_sql(f"VACUUM ANALYZE {full_name}")
            logger.info(f"VACUUM ANALYZE completed for {full_name}")
        else:
            self.execute_sql("VACUUM ANALYZE")
            logger.info("VACUUM ANALYZE completed for database")
    
    def close(self):
        """Close database connection"""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connection closed")


# Singleton instance
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """Get singleton DatabaseManager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager