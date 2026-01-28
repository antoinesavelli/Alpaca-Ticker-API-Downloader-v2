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
        # BULK DOWNLOAD CONFIGURATION - NEW!
        # =========================================
        # Maximize API efficiency by batching symbols and time ranges
        self.SYMBOLS_PER_BATCH = 100  # Start small to verify it works
        self.DAYS_PER_BATCH = 30      # Request 30 days (1 month) at a time
        
        # Master file settings
        self.MASTER_FILE_FORMAT = "arrow"  # Options: 'arrow', 'csv', 'parquet'
        self.TEMP_DIR = "T:/ticker_data/temp"  # Temporary master files
        self.STATE_FILE = "T:/ticker_data/temp/download_state.json"
        
        # Streaming write buffer (for arrow/parquet)
        self.WRITE_BUFFER_SIZE = 1000  # Rows to buffer before writing
        
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
        # FILTERING CONFIGURATION - NEW!
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
        # FILE PATHS
        # =========================================
        self.SYMBOL_FILE = "T:/ticker_data/symbol_universe.csv"
        self.OUTPUT_DIR = "T:/ticker_data"
        
        # ==========================================
        # PROCESSING MODE
        # =========================================
        self.CONSOLIDATED_DAILY_MODE = True
        self.BULK_DOWNLOAD_MODE = True  # NEW: Enable bulk download optimization
        
        # ==========================================
        # REQUEST SETTINGS
        # =========================================
        self.REQUEST_TIMEOUT = 300
        self.MAX_CONCURRENT_REQUESTS = 100
        self.RATE_LIMIT_DELAY = 0.07
        
        # Dynamic rate limiting - auto-adjust if errors occur
        self.ENABLE_DYNAMIC_RATE_LIMITING = True  # ← ADD THIS LINE
        self.MIN_RATE_DELAY = 0.006
        self.MAX_RATE_DELAY = 2.0
        
        # ==========================================
        # PARQUET SETTINGS
        # =========================================
        self.PARQUET_COMPRESSION = "snappy"
        self.PARQUET_ROW_GROUP_SIZE = 100000
        
        # ==========================================
        # LOGGING
        # =========================================
        self.LOG_LEVEL = "INFO"
        self.LOG_FILE = "logs/alpaca_pipeline.log"
        self.LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def calculate_optimal_batch_size(self) -> int:
        """
        Calculate optimal symbols per batch based on timeframe.
        
        Returns:
            Configured batch size (calculation disabled - use manual setting)
        """
        # FOR 1MIN DATA: Manual configuration is better
        # The calculation is too conservative for paginated requests
        return self.SYMBOLS_PER_BATCH  # ← JUST RETURN CONFIGURED VALUE
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return getattr(self, key, default)
    
    def get_parquet_settings(self) -> Dict[str, Any]:
        """Get parquet-specific settings."""
        return {
            'compression': self.PARQUET_COMPRESSION,
            'row_group_size': self.PARQUET_ROW_GROUP_SIZE
        }
    
    def to_iso_date(self, yyyymmdd: str) -> str:
        """Convert YYYYMMDD to ISO 8601 (YYYY-MM-DD) for Alpaca API."""
        if len(yyyymmdd) != 8:
            raise ValueError(f"Invalid date format: {yyyymmdd}. Expected YYYYMMDD")
        
        year = yyyymmdd[:4]
        month = yyyymmdd[4:6]
        day = yyyymmdd[6:8]
        return f"{year}-{month}-{day}"
        
    def validate(self) -> bool:
        """Validate configuration settings per Alpaca requirements."""
        from datetime import datetime
        
        # Validate API credentials
        if not self.ALPACA_API_KEY or not self.ALPACA_SECRET_KEY:
            raise ValueError(
                "❌ Alpaca API credentials not set.\n"
                "Set these environment variables:\n"
                "  - ALPACA_API_KEY\n"
                "  - ALPACA_SECRET_KEY\n\n"
                "Get your keys from: https://alpaca.markets/dashboard"
            )
        
        # Validate feed parameter
        valid_feeds = ['iex', 'sip', 'boats', 'overnight']
        if self.FEED not in valid_feeds:
            raise ValueError(f"Invalid feed '{self.FEED}'. Must be one of: {valid_feeds}")
        
        # Validate timeframe
        valid_timeframes = [
            '1Min', '5Min', '15Min', '30Min',
            '1Hour', '2Hour', '4Hour',
            '1Day', '1Week', '1Month'
        ]
        if self.TIMEFRAME not in valid_timeframes:
            raise ValueError(
                f"Invalid timeframe '{self.TIMEFRAME}'. "
                f"Must be one of: {', '.join(valid_timeframes)}"
            )
        
        # Validate adjustment type
        valid_adjustments = ['raw', 'split', 'dividend', 'all'
        ]
        if self.ADJUSTMENT not in valid_adjustments:
            raise ValueError(
                f"Invalid adjustment '{self.ADJUSTMENT}'. "
                f"Must be one of: {valid_adjustments}"
            )
        
        # Validate sort
        valid_sorts = ['asc', 'desc']
        if self.SORT not in valid_sorts:
            raise ValueError(f"Invalid sort '{self.SORT}'. Must be one of: {valid_sorts}")
        
        # Validate limit
        if self.LIMIT > 10000:
            raise ValueError("Alpaca max limit is 10000 bars per request")
        
        # Validate master file format
        valid_formats = ['arrow', 'csv', 'parquet']
        if self.MASTER_FILE_FORMAT not in valid_formats:
            raise ValueError(f"Invalid master file format '{self.MASTER_FILE_FORMAT}'")
        
        # Validate zero price filter mode
        valid_modes = ['close', 'any', 'all']
        if self.ZERO_PRICE_FILTER_MODE not in valid_modes:
            raise ValueError(f"Invalid zero price filter mode '{self.ZERO_PRICE_FILTER_MODE}'")
        
        # Validate files and directories
        if not Path(self.SYMBOL_FILE).exists():
            raise FileNotFoundError(f"Symbol file not found: {self.SYMBOL_FILE}")
        
        Path(self.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.TEMP_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
        
        # Validate date format
        try:
            datetime.strptime(self.START_DATE, '%Y%m%d')
            datetime.strptime(self.END_DATE, '%Y%m%d')
        except ValueError as e:
            raise ValueError(f"Dates must be in YYYYMMDD format: {e}")
        
        return True
