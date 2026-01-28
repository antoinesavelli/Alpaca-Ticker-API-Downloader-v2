"""
Symbol loader for reading and validating symbols from CSV.
"""
import pandas as pd
from typing import List
import logging
from pathlib import Path
from utils.helpers import normalize_symbol, validate_symbol_format


class SymbolLoader:
    """Manages loading and validating symbols from CSV file."""
    
    def __init__(self, logger: logging.Logger):
        """Initialize symbol loader with logger."""
        self.logger = logger
    
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
            
            # Validate format
            if validate_symbol_format(norm_symbol):
                valid_symbols.append(norm_symbol)
            else:
                invalid_symbols.append(symbol)
                self.logger.warning(f"Invalid symbol format: {symbol}")
        
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
            self.logger.warning(f"Invalid symbols: {len(invalid_symbols)}")
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
