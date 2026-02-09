"""
Symbol loader for reading and validating symbols from CSV or fetching from Alpaca API.
"""
import pandas as pd
import httpx
from typing import List, Optional
import logging
from pathlib import Path
from utils.helpers import normalize_symbol, validate_symbol_format


class SymbolLoader:
    """Manages loading and validating symbols from CSV file or Alpaca API."""
    
    def __init__(self, logger: logging.Logger, api_headers: dict = None):
        """
        Initialize symbol loader with logger.
        
        Args:
            logger: Logger instance
            api_headers: Optional Alpaca API headers for fetching symbols from exchanges
        """
        self.logger = logger
        self.api_headers = api_headers
    
    async def load_all_exchange_symbols(self, 
                                       exchanges: Optional[List[str]] = None,
                                       min_avg_volume: int = 0) -> List[str]:
        """
        Load all active stocks from exchanges via Alpaca Assets API.
        Docs: https://docs.alpaca.markets/reference/get-v2-assets
        
        Args:
            exchanges: List of exchange codes (NYSE, NASDAQ, AMEX, ARCA, BATS)
                      If None, includes all US equity exchanges
            min_avg_volume: Minimum average daily volume filter (0 = no filter)
            
        Returns:
            List of all tradable symbols (sorted alphabetically)
            
        Raises:
            Exception: If API call fails or no symbols found
        """
        if not self.api_headers:
            raise ValueError("API headers required for fetching exchange symbols")
        
        if exchanges is None:
            exchanges = ["NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"]
        
        self.logger.info(f"📡 Fetching all symbols from exchanges: {', '.join(exchanges)}")
        
        url = "https://data.alpaca.markets/v2/assets"
        params = {
            "status": "active",
            "asset_class": "us_equity"
        }
        
        try:
            async with httpx.AsyncClient(headers=self.api_headers, timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                assets = response.json()
            
            # Filter by exchange and tradability
            symbols = []
            exchange_counts = {}
            
            for asset in assets:
                # Check if tradable and on desired exchange
                if (asset.get("tradable", False) and 
                    asset.get("status") == "active" and
                    asset.get("exchange") in exchanges):
                    
                    symbol = asset["symbol"]
                    exchange = asset.get("exchange", "UNKNOWN")
                    
                    # Validate symbol format (filters out TEK1, LZ1, etc.)
                    if validate_symbol_format(symbol):
                        symbols.append(symbol)
                        exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
                    else:
                        self.logger.debug(f"Filtered invalid symbol: {symbol}")
            
            # Sort alphabetically and remove duplicates
            symbols = sorted(set(symbols))
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"✅ EXCHANGE SYMBOLS LOADED")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Total symbols: {len(symbols):,}")
            self.logger.info(f"Exchanges: {', '.join(exchanges)}")
            
            # Log exchange breakdown
            self.logger.info(f"\nExchange breakdown:")
            for ex in sorted(exchange_counts.keys()):
                count = exchange_counts[ex]
                pct = (count / len(symbols) * 100) if symbols else 0
                self.logger.info(f"  {ex:10s}: {count:5,} symbols ({pct:5.1f}%)")
            
            # Show sample
            if len(symbols) > 0:
                self.logger.info(f"\nSample symbols: {', '.join(symbols[:15])}")
            
            self.logger.info(f"{'='*60}\n")
            
            if len(symbols) == 0:
                raise ValueError("No valid symbols found from exchanges")
            
            return symbols
            
        except httpx.HTTPStatusError as e:
            self.logger.error(f"API request failed: {e.response.status_code}")
            self.logger.error(f"Response: {e.response.text}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch exchange symbols: {e}")
            raise
    
    def load_symbols(self, csv_path: str) -> List[str]:
        """
        Load symbols from CSV file.
        
        Args:
            csv_path: Path to CSV file with symbols
            
        Returns:
            List of validated, normalized symbols
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV is invalid or empty
        """
        csv_path = Path(csv_path)
        
        # Check file exists
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Read CSV
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            raise ValueError(f"Failed to read CSV file: {e}")
        
        # Check for symbol column (case-insensitive)
        symbol_col = None
        for col in df.columns:
            if col.lower() == 'symbol':
                symbol_col = col
                break
    
        if symbol_col is None:
            raise ValueError("CSV must have a 'symbol' column")
    
        # Check not empty
        if len(df) == 0:
            raise ValueError("CSV file is empty")
    
        # Extract symbols
        raw_symbols = df[symbol_col].tolist()
    
        # Normalize and validate
        valid_symbols = []
        invalid_symbols = []
    
        for symbol in raw_symbols:
            if pd.isna(symbol):
                continue
    
            # Normalize
            norm_symbol = normalize_symbol(str(symbol))
    
            # Validate format (filters out TEK1, LZ1, etc.)
            if validate_symbol_format(norm_symbol):
                valid_symbols.append(norm_symbol)
            else:
                invalid_symbols.append(symbol)
                # Changed from WARNING to DEBUG
                self.logger.debug(f"Invalid symbol format: {symbol}")
    
        # Remove duplicates while preserving order
        seen = set()
        unique_symbols = []
        for symbol in valid_symbols:
            if symbol not in seen:
                seen.add(symbol)
                unique_symbols.append(symbol)
    
        # Log results
        self.logger.info(f"Loaded {len(raw_symbols)} symbols from CSV")
        self.logger.info(f"Valid symbols: {len(unique_symbols)}")
        if len(invalid_symbols) > 0:
            # Show summary instead of individual warnings
            self.logger.info(f"Filtered out {len(invalid_symbols)} invalid symbols (e.g., symbols ending in digits)")
        duplicates_removed = len(valid_symbols) - len(unique_symbols)
        if duplicates_removed > 0:
            self.logger.info(f"Duplicates removed: {duplicates_removed}")
    
        if len(unique_symbols) == 0:
            raise ValueError("No valid symbols found in CSV")
    
        return unique_symbols
    
    def validate_symbols(self, symbols: List[str]) -> List[str]:
        """
        Validate list of symbols.
        
        Args:
            symbols: List of symbols to validate
            
        Returns:
            List of valid symbols
        """
        valid_symbols = []
        for symbol in symbols:
            norm_symbol = normalize_symbol(symbol)
            if validate_symbol_format(norm_symbol):
                valid_symbols.append(norm_symbol)
            else:
                self.logger.warning(f"Invalid symbol: {symbol}")
        
        return valid_symbols
