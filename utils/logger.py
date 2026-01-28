"""
Logging utilities for the pipeline.
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Union


def setup_logger(name: str, log_file: str, log_level: Union[str, int] = "INFO") -> logging.Logger:
    """
    Setup and configure a logger.
    
    Args:
        name: Logger name
        log_file: Path to log file
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) as string or int constant
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Handle both string and integer log levels
    if isinstance(log_level, str):
        level = getattr(logging, log_level.upper())
    else:
        level = log_level
    
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    try:
        console_handler.stream.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7 compatibility
        pass
    
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
    file_handler.setLevel(level)
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


class PipelineLogger:
    """Custom logger for the pipeline."""
    
    def __init__(self, log_dir: str, log_level: Union[str, int] = "INFO"):
        """Initialize logger with specified directory and level."""
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create download logs directory
        self.download_logs_dir = self.log_dir / "download_logs"
        self.download_logs_dir.mkdir(exist_ok=True)
        
        # Create error logs directory
        self.error_logs_dir = self.log_dir / "error_logs"
        self.error_logs_dir.mkdir(exist_ok=True)
        
        # Handle both string and integer log levels
        if isinstance(log_level, str):
            self.log_level = getattr(logging, log_level.upper())
        else:
            self.log_level = log_level
            
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup and configure logger."""
        logger = logging.getLogger('thetadata_pipeline')
        logger.setLevel(self.log_level)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        try:
            console_handler.stream.reconfigure(encoding='utf-8')  # Force UTF-8 encoding
        except AttributeError:
            pass
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)
        
        # File handler for general logs
        log_file = self.log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(self.log_level)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
        
        return logger
    
    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance."""
        return self.logger
    
    def get_date_log_file(self, date: str) -> Path:
        """Get log file path for a specific date."""
        return self.download_logs_dir / f"{date}_download.log"
    
    def log_date_start(self, date: str, symbol_count: int):
        """Log the start of processing for a date."""
        log_file = self.get_date_log_file(date)
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"Date: {date}\n")
            f.write(f"Start Time: {datetime.now().isoformat()}\n")
            f.write(f"Symbols to process: {symbol_count}\n")
            f.write(f"{'='*80}\n\n")
    
    def log_date_complete(self, date: str, success_count: int, fail_count: int, duration: float):
        """Log the completion of processing for a date."""
        log_file = self.get_date_log_file(date)
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"Date: {date}\n")
            f.write(f"End Time: {datetime.now().isoformat()}\n")
            f.write(f"Successful symbols: {success_count}\n")
            f.write(f"Failed symbols: {fail_count}\n")
            f.write(f"Duration: {duration:.2f} seconds\n")
            f.write(f"{'='*80}\n\n")
    
    def log_symbol_result(self, date: str, symbol: str, success: bool, message: str = ""):
        """Log the result of processing a symbol."""
        log_file = self.get_date_log_file(date)
        status = "SUCCESS" if success else "FAILED"
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - {symbol} - {status} - {message}\n")
