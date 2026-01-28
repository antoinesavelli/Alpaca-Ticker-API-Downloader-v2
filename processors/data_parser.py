"""
Data parser for processing Alpaca API responses.
Per: https://docs.alpaca.markets/reference/stockbars
"""
import pandas as pd
import io
import json
import logging
from typing import Optional, Union, Dict, Any, List
from datetime import datetime, timedelta
from pathlib import Path
from utils.helpers import validate_ohlc_relationships
from utils.diagnostics import ErrorTracer  # Only ErrorTracer is still useful
from utils.null_data_tracker import NullDataTracker
import time
import pyarrow as pa
import pyarrow.compute as pc


class DataParser:
    """Parse and validate data from Alpaca API."""
    
    def __init__(self, logger: logging.Logger, output_dir: str = None, config=None):
        self.logger = logger
        self.config = config
        self.output_dir = output_dir
        
        # Enable debug logging based on logger level
        self._debug_enabled = logger.isEnabledFor(logging.DEBUG)
        
        # Pre-compile timezone converter (reuse for all timestamps)
        self.utc_tz = pd.Timestamp.now(tz='UTC').tzinfo
        self.et_tz = pd.Timestamp.now(tz='America/New_York').tzinfo
        
        # Initialize null data tracker
        if output_dir:
            self.null_tracker = NullDataTracker(output_dir, logger)
        else:
            self.null_tracker = None
    
    def parse_market_data(self, data: Union[Dict, List], symbol: str = None) -> Optional[pd.DataFrame]:
        """
        Parse market data from Alpaca API response.
        
        Alpaca returns JSON like:
        {
            "bars": [
                {"t": "2024-01-02T09:30:00Z", "o": 150.5, "h": 151.0, 
                 "l": 150.0, "c": 150.8, "v": 1000000, "n": 5000, "vw": 150.6}
            ]
        }
        """
        if not data:
            return None
        
        try:
            # Handle direct list of bars
            if isinstance(data, list):
                return self._parse_alpaca_bars(data, symbol)
            
            # Handle dict with 'bars' key (from fetch_market_data)
            elif isinstance(data, dict):
                if 'bars' in data:
                    return self._parse_alpaca_bars(data['bars'], symbol)
                # Handle single bar object
                elif all(k in data for k in ['t', 'o', 'h', 'l', 'c']):
                    return self._parse_alpaca_bars([data], symbol)
                return None
            
            return None
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Parse error: {e}")
            return None
    
    def _parse_alpaca_bars(self, bars: List[Dict[str, Any]], symbol: str = None) -> Optional[pd.DataFrame]:
        """
        Parse Alpaca bar format into standardized DataFrame.
        
        Alpaca bar fields per docs:
        - t: timestamp (RFC-3339) - REQUIRED
        - o: open price - REQUIRED
        - h: high price - REQUIRED
        - l: low price - REQUIRED
        - c: close price - REQUIRED
        - v: volume - REQUIRED
        - n: trade count - OPTIONAL
        - vw: volume weighted average price - OPTIONAL
        
        Returns DataFrame with columns:
        REQUIRED: symbol, timestamp, open, high, low, close, volume
        OPTIONAL: cumulative_volume, count, vwap
        """
        if not bars or len(bars) == 0:
            return None
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(bars)
            
            if len(df) == 0:
                return None
            
            # Rename Alpaca columns to spec format
            column_mapping = {
                't': 'timestamp',
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'n': 'count',      # OPTIONAL - Alpaca 'n' -> spec 'count'
                'vw': 'vwap'       # OPTIONAL - Alpaca 'vw' -> spec 'vwap'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Add symbol column if provided
            if symbol:
                df['symbol'] = symbol
            
            # Verify REQUIRED columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                self.logger.error(f"[{symbol}] Missing REQUIRED columns: {missing_cols}")
                return None
            
            # Parse timestamps (Alpaca uses RFC-3339/ISO 8601 in UTC)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            
            # Convert to US/Eastern timezone (per spec)
            df['timestamp'] = df['timestamp'].dt.tz_convert('US/Eastern')
            
            # Sort by timestamp to ensure cumulative volume is calculated correctly
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Convert REQUIRED data types
            df['open'] = pd.to_numeric(df['open'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
            
            # Calculate cumulative_volume (OPTIONAL - running total for the day)
            df['cumulative_volume'] = df['volume'].cumsum().astype('int64')
            
            # Handle OPTIONAL count field
            if 'count' in df.columns:
                df['count'] = pd.to_numeric(df['count'], errors='coerce').fillna(0).astype('int64')
            else:
                self.logger.debug(f"[{symbol}] No 'count' field in data (optional)")
            
            # Handle OPTIONAL vwap field
            if 'vwap' in df.columns:
                df['vwap'] = pd.to_numeric(df['vwap'], errors='coerce')
            else:
                self.logger.debug(f"[{symbol}] No 'vwap' field in data (optional)")
            
            # Data quality validation per spec
            initial_len = len(df)
            
            # Filter: open > 0, close > 0, volume >= 0
            df = df[(df['open'] > 0) & (df['close'] > 0) & (df['volume'] >= 0)]
            
            # Remove null symbols/timestamps
            df = df.dropna(subset=['symbol', 'timestamp'])
            
            if len(df) < initial_len:
                removed = initial_len - len(df)
                self.logger.debug(f"[{symbol}] Removed {removed} invalid bars (quality checks)")
            
            if len(df) == 0:
                return None
            
            # Build final columns list (required + available optional)
            final_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
            
            # Add optional columns if they exist
            if 'cumulative_volume' in df.columns:
                final_columns.append('cumulative_volume')
            if 'count' in df.columns:
                final_columns.append('count')
            if 'vwap' in df.columns:
                final_columns.append('vwap')
            
            df = df[final_columns]
            
            if self._debug_enabled:
                self.logger.debug(f"[{symbol}] Parsed {len(df)} valid bars with columns: {final_columns}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Parse error: {e}", exc_info=True)
            return None
    
    def _filter_trading_bars(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Filter out bars with zero price OR zero volume.
        Updated for configurable filtering per settings.
        """
        if df is None or len(df) == 0:
            return df
        
        try:
            initial_count = len(df)
            
            # Build filter mask based on config
            filter_masks = []
            
            # Filter zero volume if enabled
            if self.config and getattr(self.config, 'FILTER_ZERO_VOLUME', True):
                if 'volume' in df.columns:
                    filter_masks.append(df['volume'] > 0)
            
            # Filter zero price if enabled
            if self.config and getattr(self.config, 'FILTER_ZERO_PRICE', True):
                zero_price_mode = getattr(self.config, 'ZERO_PRICE_FILTER_MODE', 'all')
                
                if zero_price_mode == 'close':
                    # Only close must be > 0
                    if 'close' in df.columns:
                        filter_masks.append(df['close'] > 0)
                
                elif zero_price_mode == 'any':
                    # At least one OHLC field must be > 0
                    if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                        ohlc_mask = (
                            (df['open'] > 0) | (df['high'] > 0) | 
                            (df['low'] > 0) | (df['close'] > 0)
                        )
                        filter_masks.append(ohlc_mask)
                
                elif zero_price_mode == 'all':
                    # ALL OHLC fields must be > 0
                    if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                        ohlc_mask = (
                            (df['open'] > 0) & (df['high'] > 0) & 
                            (df['low'] > 0) & (df['close'] > 0)
                        )
                        filter_masks.append(ohlc_mask)
            
            # Combine all filter masks
            if filter_masks:
                combined_mask = filter_masks[0]
                for mask in filter_masks[1:]:
                    combined_mask = combined_mask & mask
                
                df = df.loc[combined_mask].reset_index(drop=True)
                
                if self._debug_enabled and (removed := initial_count - len(df)) > 0:
                    self.logger.debug(
                        f"[{symbol}] Filtered {removed:,} bars "
                        f"({removed/initial_count*100:.1f}% of total)"
                    )
            
            return df
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Filter error: {e}")
            return df
    
    def validate_dataframe(self, df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
        """Validate OHLC data integrity."""
        if df is None or len(df) == 0:
            return df
        
        try:
            # Check for null values
            null_counts = df[['open', 'high', 'low', 'close', 'volume']].isnull().sum()
            if null_counts.sum() > 0:
                self.logger.warning(f"[{symbol}] Found null values: {null_counts.to_dict()}")
            
            # Validate OHLC relationships (high >= low, etc.)
            invalid_rows = validate_ohlc_relationships(df)
            if len(invalid_rows) > 0:
                self.logger.warning(f"[{symbol}] Found {len(invalid_rows)} invalid OHLC relationships")
                # Remove invalid rows
                df = df.drop(invalid_rows).reset_index(drop=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"[{symbol}] Validation error: {e}")
            return df
    
    def _parse_alpaca_bars_fast(self, bars: List[Dict[str, Any]], symbol: str = None) -> Optional[pd.DataFrame]:
        """ULTRA-FAST parsing using PyArrow (avoid pandas overhead)."""
        if not bars or len(bars) == 0:
            return None
        
        try:
            # Convert to Arrow Table directly (2-3x faster than pandas)
            table = pa.Table.from_pylist(bars)
            
            # Rename columns
            table = table.rename_columns([
                'timestamp' if col == 't' else
                'open' if col == 'o' else
                'high' if col == 'h' else
                'low' if col == 'l' else
                'close' if col == 'c' else
                'volume' if col == 'v' else
                'trade_count' if col == 'n' else
                'vwap' if col == 'vw' else col
                for col in table.column_names
            ])
            
            # Add symbol column
            if symbol:
                table = table.append_column('symbol', pa.array([symbol] * len(table)))
            
            # Apply filters using PyArrow compute (vectorized - FAST!)
            if self.config.FILTER_ZERO_PRICE or self.config.FILTER_ZERO_VOLUME:
                mask = pa.array([True] * len(table))
                
                if self.config.FILTER_ZERO_PRICE:
                    mask = pc.and_(mask, pc.greater(table['close'], 0))
                
                if self.config.FILTER_ZERO_VOLUME:
                    mask = pc.and_(mask, pc.greater(table['volume'], 0))
                
                table = table.filter(mask)
            
            # Convert to pandas only at the end
            df = table.to_pandas()
            
            # Timestamp conversion (still needed)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('America/New_York')
            df['date'] = df['timestamp'].dt.strftime('%Y%m%d')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Fast parse error: {e}")
            return None