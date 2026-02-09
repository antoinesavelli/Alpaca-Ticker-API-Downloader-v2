"""
Helper utilities for the pipeline.
"""
from datetime import datetime, timedelta
from typing import List
import pandas as pd
import re


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYYMMDD format."""
    return datetime.strptime(date_str, '%Y%m%d')


def format_date(date_obj: datetime) -> str:
    """Format datetime object to YYYYMMDD string."""
    return date_obj.strftime('%Y%m%d')


def iso_to_yyyymmdd(iso_date: str) -> str:
    """
    Convert ISO 8601 date (YYYY-MM-DD) to YYYYMMDD format.
    
    Args:
        iso_date: Date in YYYY-MM-DD format
        
    Returns:
        Date in YYYYMMDD format
    """
    return iso_date.replace('-', '')


def yyyymmdd_to_iso(yyyymmdd: str) -> str:
    """
    Convert YYYYMMDD to ISO 8601 (YYYY-MM-DD) for Alpaca API.
    
    Args:
        yyyymmdd: Date in YYYYMMDD format
        
    Returns:
        Date in YYYY-MM-DD format
    """
    if len(yyyymmdd) != 8:
        raise ValueError(f"Invalid date format: {yyyymmdd}")
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def get_trading_dates(start_date: str, end_date: str) -> List[str]:
    """
    Generate list of trading dates between start and end (inclusive).
    Excludes weekends and holidays.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        List of trading dates in YYYYMMDD format
    """
    from utils.trading_calendar import TradingCalendar
    calendar = TradingCalendar()
    return calendar.get_trading_days(start_date, end_date)


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol (uppercase, strip whitespace)."""
    return symbol.strip().upper()


def validate_symbol_format(symbol: str) -> bool:
    """
    Validate symbol format for Alpaca API.
    Alpaca supports standard US stock symbols including preferred stocks.
    
    Valid examples:
    - AAPL (common stock)
    - GLOP-PA (preferred stock - hyphen for preferred)
    - BRK.B (class B shares - dot for share class)
    
    Invalid examples:
    - TEK1, LZ1, DBT1 (temporary corporate action symbols - trailing digit)
    - TEST123 (test symbols)
    - ZZZZ (invalid markers)
    
    Args:
        symbol: Stock symbol to validate
        
    Returns:
        bool: True if valid format
    """
    if not symbol:
        return False
    
    # Exclude symbols ending in digits (temporary corporate action symbols like TEK1, LZ1)
    # These represent pre/post split or reorganization temporary placeholders
    if re.search(r'\d$', symbol):
        return False
    
    # Allow alphanumeric and common special characters for preferred stocks and classes
    # Hyphens (-) are valid for preferred stocks (e.g., GLOP-PA)
    # Dots (.) are valid for share classes (e.g., BRK.B)
    # Slashes (/) are rare but sometimes used
    if not all(c.isalnum() or c in ['.', '-', '/'] for c in symbol):
        return False
    
    # Reasonable length check (most symbols are 1-6 chars, but allow up to 15 for edge cases)
    if len(symbol) < 1 or len(symbol) > 15:
        return False
    
    # Exclude common test/invalid patterns
    if symbol.startswith('TEST') or symbol.startswith('ZZZZ') or symbol.startswith('ZVZZT'):
        return False
    
    return True


def validate_ohlc_relationships(df: pd.DataFrame) -> List[int]:
    """
    Validate OHLC price relationships per market data standards.
    Returns list of invalid row indices.
    
    Validates:
    - high >= low
    - high >= open
    - high >= close
    - low <= open
    - low <= close
    - volume >= 0
    """
    if df is None or len(df) == 0:
        return []
    
    invalid_indices = set()
    
    # Check high >= low
    mask = df['high'] < df['low']
    if mask.any():
        invalid_indices.update(df[mask].index.tolist())
    
    # Check high >= open
    mask = df['high'] < df['open']
    if mask.any():
        invalid_indices.update(df[mask].index.tolist())
    
    # Check high >= close
    mask = df['high'] < df['close']
    if mask.any():
        invalid_indices.update(df[mask].index.tolist())
    
    # Check low <= open
    mask = df['low'] > df['open']
    if mask.any():
        invalid_indices.update(df[mask].index.tolist())
    
    # Check low <= close
    mask = df['low'] > df['close']
    if mask.any():
        invalid_indices.update(df[mask].index.tolist())
    
    # Check volume >= 0
    if 'volume' in df.columns:
        mask = df['volume'] < 0
        if mask.any():
            invalid_indices.update(df[mask].index.tolist())
    
    return sorted(list(invalid_indices))


def calculate_file_size_mb(file_path: str) -> float:
    """Calculate file size in MB."""
    from pathlib import Path
    path = Path(file_path)
    if path.exists():
        return path.stat().st_size / (1024 * 1024)
    return 0.0
