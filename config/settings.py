"""
Configuration settings for Alpaca data pipeline - BULK DOWNLOAD MODE.
Per: https://docs.alpaca.markets/reference/stockbars
"""
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from keys.env
load_dotenv('keys.env')


class Config:
    """Configuration management for the pipeline."""
    
    def __init__(self):
        # ==========================================
        # ALPACA API CONFIGURATION
        # =========================================
        self.ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
        self.ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
        self.ALPACA_BASE_URL = "https://data.alpaca.markets"
        self.ALPACA_ENDPOINT = "/v2/stocks/bars"
        
        # ==========================================
        # DATA FEED CONFIGURATION
        # =========================================
        self.FEED = "sip"  # SIP = 100% market coverage
        
        # ==========================================
        # SYMBOL LOADING CONFIGURATION
        # =========================================
        # Options: 'csv' or 'exchanges'
        self.SYMBOL_SOURCE = "csv"  # Changed to CSV mode
        
        # If SYMBOL_SOURCE = 'exchanges':
        self.EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]  # Main US exchanges
        self.MIN_AVG_VOLUME = 0  # Minimum avg daily volume (0 = no filter)
        
        # If SYMBOL_SOURCE = 'csv':
        self.SYMBOL_FILE = "T:/trading/ticker_data/symbol_universe.csv"
        
        # ==========================================
        # BULK DOWNLOAD CONFIGURATION
        # =========================================
        # Maximize API efficiency by batching symbols and time ranges
        self.SYMBOLS_PER_BATCH = 100  # Batch size for API calls
        self.DAYS_PER_BATCH = 30      # Request 30 days (1 month) at a time
        
        # Master file settings
        self.MASTER_FILE_FORMAT = "arrow"  # Options: 'arrow', 'csv', 'parquet'
        self.TEMP_DIR = "T:/trading/ticker_data/temp"
        self.STATE_FILE = "T:/trading/ticker_data/temp/download_state.json"
        
        # Streaming write buffer (for arrow/parquet)
        self.WRITE_BUFFER_SIZE = 1000  # Rows to buffer before writing
        
        # Parquet compression settings
        self.PARQUET_COMPRESSION = 'snappy'  # Options: 'snappy', 'gzip', 'brotli', 'zstd', 'none'
        
        # ==========================================
        # DATA CONFIGURATION
        # =========================================
        # ⚠️ IMPORTANT: Dates in YYYYMMDD format (internal format)
        # Converted to ISO 8601 (YYYY-MM-DD) when calling Alpaca API
        self.START_DATE = "20160104"  # First available data
        self.END_DATE = "20251231"    # Dec 31, 2025

        # Timeframe per Alpaca docs
        # Valid: 1Min, 5Min, 15Min, 30Min, 1Hour, 2Hour, 4Hour, 1Day, 1Week, 1Month
        self.TIMEFRAME = "1Min"
        
        # Data adjustment type per Alpaca API docs
        # Options: 'raw', 'split', 'dividend', 'all'
        self.ADJUSTMENT = "all"
        
        # Sort order per Alpaca docs
        # Options: 'asc', 'desc'
        self.SORT = "asc"
        
        # Pagination limit - Alpaca max is 10000 bars per request
        self.LIMIT = 10000
        
        # ==========================================
        # FILTERING CONFIGURATION
        # =========================================
        # Filter out bars with zero price OR zero volume
        self.FILTER_ZERO_PRICE = True
        self.FILTER_ZERO_VOLUME = True
        
        # Which price fields must be non-zero?
        # Options: 'close', 'any', 'all'
        # 'close' = only close > 0
        # 'any' = at least one OHLC > 0
        # 'all' = all OHLC > 0
        self.ZERO_PRICE_FILTER_MODE = "all"  # Filter if ANY OHLC field is zero
        
        # ==========================================
        # TIME RANGE - EXTENDED HOURS SUPPORT
        # =========================================
        self.START_TIME = "04:00:00"  # 4:00 AM ET
        self.END_TIME = "20:00:00"    # 8:00 PM ET
        
        # ==========================================
        # FILE PATHS - PROPERLY NESTED IN ticker_data
        # =========================================
        self.OUTPUT_DIR = "T:/trading/ticker_data"
        
        # ==========================================
        # PROCESSING MODE
        # =========================================
        self.CONSOLIDATED_DAILY_MODE = True
        self.BULK_DOWNLOAD_MODE = True  # Enable bulk download optimization
        
        # ==========================================
        # CONCURRENCY & PERFORMANCE
        # =========================================
        self.MAX_CONCURRENT_REQUESTS = 10
        self.RETRY_ATTEMPTS = 3
        self.RETRY_DELAY = 2.0
        
        # ==========================================
        # RATE LIMITING
        # =========================================
        self.RATE_LIMIT_DELAY = 0.1  # Seconds between requests (start conservative)
        self.MAX_RATE_DELAY = 5.0    # Maximum delay on errors
        self.MIN_RATE_DELAY = 0.05   # Minimum delay (speed up on success)
        
        # ==========================================
        # LOGGING
        # =========================================
        self.LOG_FILE = "T:/trading/ticker_data/logs/pipeline.log"
        self.LOG_LEVEL = "INFO"
    
    def calculate_optimal_batch_size(self) -> int:
        """
        Calculate optimal batch size for API requests.
        Alpaca allows up to 100 symbols per request.
        
        Returns:
            Number of symbols to request per API call
        """
        # Alpaca max is 100 symbols per request
        return min(self.SYMBOLS_PER_BATCH, 100)
    
    def get_parquet_settings(self) -> Dict[str, Any]:
        """
        Get parquet file settings.
        
        Returns:
            Dictionary with parquet configuration options
        """
        return {
            'compression': self.PARQUET_COMPRESSION,
            'engine': 'pyarrow',
            'index': False
        }
    
    def validate(self):
        """Validate configuration settings."""
        if not self.ALPACA_API_KEY or not self.ALPACA_SECRET_KEY:
            raise ValueError("Alpaca API credentials not set in keys.env")
        
        if self.SYMBOL_SOURCE not in ['csv', 'exchanges']:
            raise ValueError(f"Invalid SYMBOL_SOURCE: {self.SYMBOL_SOURCE}")
        
        if self.SYMBOL_SOURCE == 'csv' and not os.path.exists(self.SYMBOL_FILE):
            raise ValueError(f"Symbol file not found: {self.SYMBOL_FILE}")
        
        # Create directories
        Path(self.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.TEMP_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
