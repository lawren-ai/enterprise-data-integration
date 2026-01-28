"""
Configuration Loader Utility
Loads configuration from YAML and INI files
"""

import yaml
import configparser
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    """Handles loading configuration from various sources"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config = None
        self.db_config = None
    
    def load_yaml_config(self, filename: str = "config.yaml") -> Dict[str, Any]:
        """Load YAML configuration file"""
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        return self.config
    
    def load_db_config(self, filename: str = "database.ini") -> configparser.ConfigParser:
        """Load database configuration from INI file"""
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            raise FileNotFoundError(
                f"Database config not found: {config_path}\n"
                f"Please copy database.ini.template to database.ini and configure it."
            )
        
        parser = configparser.ConfigParser()
        parser.read(config_path)
        self.db_config = parser
        
        return parser
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation
        
        Example:
            config.get('database.host')
            config.get('etl.batch_size', 10000)
        """
        if self.config is None:
            self.load_yaml_config()
        
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_db_connection_string(self, db_type: str = "postgresql") -> str:
        """
        Generate database connection string
        
        Args:
            db_type: 'postgresql' or 'sqlserver'
        
        Returns:
            SQLAlchemy connection string
        """
        if self.db_config is None:
            self.load_db_config()
        
        if db_type == "postgresql":
            user = self.db_config['postgresql']['user']
            password = self.db_config['postgresql']['password']
            host = self.db_config['postgresql']['host']
            port = self.db_config['postgresql']['port']
            database = self.db_config['postgresql']['database']
            
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        elif db_type == "sqlserver":
            driver = self.db_config['sqlserver']['driver']
            server = self.db_config['sqlserver']['server']
            database = self.db_config['sqlserver']['database']
            user = self.db_config['sqlserver']['user']
            password = self.db_config['sqlserver']['password']
            
            return (
                f"mssql+pyodbc://{user}:{password}@{server}/{database}"
                f"?driver={driver.replace(' ', '+')}"
            )
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def validate_config(self) -> bool:
        """Validate that all required configuration is present"""
        required_keys = [
            'project.name',
            'database.type',
            'etl.batch_size',
            'paths.raw_data'
        ]
        
        missing_keys = []
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
        
        return True


# Singleton instance
_config_loader = None

def get_config() -> ConfigLoader:
    """Get singleton ConfigLoader instance"""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
        _config_loader.load_yaml_config()
        _config_loader.load_db_config()
        _config_loader.validate_config()
    return _config_loader