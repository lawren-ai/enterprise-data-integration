"""
Logging Utility
Centralized logging configuration with file and console output
"""

import sys
from pathlib import Path
from loguru import logger
from datetime import datetime


class LoggerSetup:
    """Configure and manage application logging"""
    
    def __init__(self, log_dir: str = "logs", level: str = "INFO"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.level = level
        self._configured = False
    
    def setup(self):
        """Configure loguru logger with file and console handlers"""
        if self._configured:
            return
        
        # Remove default handler
        logger.remove()
        
        # Console handler with colors
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
            level=self.level,
            colorize=True
        )
        
        # General log file (all levels)
        log_file = self.log_dir / f"app_{datetime.now():%Y%m%d}.log"
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="500 MB",
            retention="30 days",
            compression="zip"
        )
        
        # Error log file (errors only)
        error_file = self.log_dir / f"error_{datetime.now():%Y%m%d}.log"
        logger.add(
            error_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="ERROR",
            rotation="100 MB",
            retention="90 days",
            compression="zip"
        )
        
        self._configured = True
        logger.info("Logger initialized successfully")
    
    def get_logger(self, name: str = None):
        """Get a logger instance"""
        if not self._configured:
            self.setup()
        
        if name:
            return logger.bind(name=name)
        return logger


# Singleton instance
_logger_setup = None

def get_logger(name: str = None):
    """Get application logger"""
    global _logger_setup
    if _logger_setup is None:
        _logger_setup = LoggerSetup()
        _logger_setup.setup()
    
    if name:
        return logger.bind(name=name)
    return logger