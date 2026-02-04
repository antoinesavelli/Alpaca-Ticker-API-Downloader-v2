"""
Validate symbols from stocks_under_2b.csv against Alpaca API.
Checks which symbols are available for trading/data access.
"""
import asyncio
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timedelta
import httpx
from typing import List, Dict, Set

sys.path.append(str(Path(__file__).parent.parent))

from config.settings import Config
from utils.logger import setup_logger


class AlpacaSymbolValidator:
    """Validate symbols against Alpaca API."""
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger(
            name="alpaca_symbol_validator",
            log_file="logs/alpaca_symbol_validation.log",
            log_level="INFO"
        )
        
        self.headers = {
            "APCA-API-KEY-ID": self.config.ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": self.config.ALPACA_SECRET_KEY,
            "accept": "application/json"
        }
        
        # Configure HTTP client
        timeout = httpx.Timeout(
            connect=10.0,
            read=60.0,
            write=10.0,
            pool=10.0
        )
        
        self.session = httpx.AsyncClient(
            timeout=timeout,
            headers=self.headers,
            http2=True
        )
    
    async def check_symbol_availability(self, symbol: str) -> Dict:
        """
        Check if symbol is available on Alpaca.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dict with symbol info and availability status
        """
        try:
            # Method 1: Try to get recent bar data (last 2 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)  # 5 days to account for weekends
            
            url = f"{self.config.ALPACA_BASE_URL}/v2/stocks/bars"
            params = {
                "symbols": symbol,
                "timeframe": "1Day",
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
                "limit": 10,
                "feed": self.config.FEED
            }
            
            response = await self.session.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                bars = data.get("bars", {}).get(symbol, [])
                
                if bars and len(bars) > 0:
                    # Symbol has data
                    latest_bar = bars[-1]
                    return {
                        'Symbol': symbol,
                        'available': True,
                        'has_data': True,
                        'latest_date': latest_bar.get('t', ''),
                        'latest_close': latest_bar.get('c', 0),
                        'latest_volume': latest_bar.get('v', 0),
                        'error': None
                    }
                else:
                    # No bars returned - might be delisted or no recent data
                    return {
                        'Symbol': symbol,
                        'available': False,
                        'has_data': False,
                        'latest_date': None,
                        'latest_close': None,
                        'latest_volume': None,
                        'error': 'No recent data'
                    }
            elif response.status_code == 422:
                # Unprocessable entity - invalid symbol
                return {
                    'Symbol': symbol,
                    'available': False,
                    'has_data': False,
                    'latest_date': None,
                    'latest_close': None,
                    'latest_volume': None,
                    'error': 'Invalid symbol'
                }
            else:
                self.logger.debug(f"{symbol}: HTTP {response.status_code}")
                return {
                    'Symbol': symbol,
                    'available': False,
                    'has_data': False,
                    'latest_date': None,
                    'latest_close': None,
                    'latest_volume': None,
                    'error': f'HTTP {response.status_code}'
                }
                
        except Exception as e:
            self.logger.debug(f"Error checking {symbol}: {e}")
            return {
                'Symbol': symbol,
                'available': False,
                'has_data': False,
                'latest_date': None,
                'latest_close': None,
                'latest_volume': None,
                'error': str(e)
            }
    
    async def validate_symbols_batch(self, symbols: List[str], batch_size: int = 50):
        """
        Validate a batch of symbols with rate limiting.
        
        Args:
            symbols: List of symbols to validate
            batch_size: Number of concurrent requests
            
        Returns:
            List of validation results
        """
        self.logger.info(f"Validating {len(symbols)} symbols against Alpaca API...")
        self.logger.info(f"Using batch size: {batch_size} concurrent requests")
        
        results = []
        
        # Process in batches to avoid overwhelming the API
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            
            # Create tasks for this batch
            tasks = [self.check_symbol_availability(symbol) for symbol in batch]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    self.logger.warning(f"Exception in batch: {result}")
                    continue
                results.append(result)
            
            # Log progress
            self.logger.info(f"Progress: {len(results)}/{len(symbols)} symbols checked")
            
            # Small delay between batches to be nice to API
            if i + batch_size < len(symbols):
                await asyncio.sleep(0.5)
        
        return results
    
    async def validate_from_csv(self, csv_path: str):
        """
        Validate symbols from CSV file.
        
        Args:
            csv_path: Path to CSV file with Symbol column
            
        Returns:
            DataFrame with validation results
        """
        # Load symbols
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            self.logger.error(f"CSV file not found: {csv_path}")
            return None
        
        df = pd.read_csv(csv_path)
        
        if 'Symbol' not in df.columns:
            self.logger.error("CSV must have 'Symbol' column")
            return None
        
        symbols = df['Symbol'].tolist()
        original_count = len(symbols)
        
        self.logger.info("=" * 80)
        self.logger.info("Alpaca Symbol Validation")
        self.logger.info("=" * 80)
        self.logger.info(f"Input file: {csv_path}")
        self.logger.info(f"Total symbols to validate: {original_count}")
        self.logger.info("=" * 80)
        
        # Validate symbols
        results = await self.validate_symbols_batch(symbols, batch_size=50)
        
        # Debug: Check what we got
        self.logger.info(f"DEBUG: Got {len(results)} results back")
        if len(results) > 0:
            self.logger.info(f"DEBUG: First result: {results[0]}")
            self.logger.info(f"DEBUG: First result keys: {results[0].keys()}")
        
        # Convert to DataFrame
        results_df = pd.DataFrame(results)
        
        self.logger.info(f"DEBUG: Results DataFrame shape: {results_df.shape}")
        self.logger.info(f"DEBUG: Results DataFrame columns: {list(results_df.columns)}")
        
        # Check if Symbol column exists in results
        if 'Symbol' not in results_df.columns and len(results_df) > 0:
            self.logger.error("ERROR: 'Symbol' column not found in results DataFrame!")
            self.logger.error(f"Available columns: {list(results_df.columns)}")
            return None
        
        # Merge with original data
        final_df = df.merge(results_df, on='Symbol', how='left')
        
        # Statistics
        available_count = results_df['available'].sum() if len(results_df) > 0 else 0
        unavailable_count = len(results_df) - available_count
        
        self.logger.info("=" * 80)
        self.logger.info("VALIDATION RESULTS")
        self.logger.info("=" * 80)
        self.logger.info(f"Total symbols checked: {original_count}")
        self.logger.info(f"✓ Available on Alpaca: {available_count} ({available_count/original_count*100:.1f}%)")
        self.logger.info(f"✗ Not available: {unavailable_count} ({unavailable_count/original_count*100:.1f}%)")
        self.logger.info("=" * 80)
        
        return final_df
    
    async def close(self):
        """Close HTTP session."""
        await self.session.aclose()


async def main():
    """Main execution."""
    validator = AlpacaSymbolValidator()
    
    try:
        # Path to your stocks_under_2b.csv in data folder
        input_file = Path("data/stocks_under_2b.csv")
        
        if not input_file.exists():
            print(f"❌ File not found: {input_file}")
            print("   Make sure you moved stocks_under_2b.csv to the data folder")
            return 1
        
        print("=" * 80)
        print("Alpaca Symbol Validation")
        print("=" * 80)
        print(f"Input: {input_file}")
        print("Checking which symbols are available on Alpaca...")
        print("-" * 80)
        
        # Validate symbols
        results_df = await validator.validate_from_csv(str(input_file))
        
        if results_df is None:
            print("❌ Validation failed")
            return 1
        
        # Save results
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Save full results
        full_output = output_dir / "alpaca_symbol_validation_full.csv"
        results_df.to_csv(full_output, index=False)
        print(f"\n✓ Full results saved to: {full_output}")
        
        # Save available symbols only
        available_df = results_df[results_df['available'] == True].copy()
        available_output = output_dir / "stocks_under_2b_alpaca_available.csv"
        available_df.to_csv(available_output, index=False)
        print(f"✓ Available symbols saved to: {available_output}")
        
        # Save unavailable symbols for review
        unavailable_df = results_df[results_df['available'] == False].copy()
        unavailable_output = output_dir / "stocks_under_2b_alpaca_unavailable.csv"
        unavailable_df.to_csv(unavailable_output, index=False)
        print(f"✓ Unavailable symbols saved to: {unavailable_output}")
        
        # Display summary
        print(f"\n{'=' * 80}")
        print("SUMMARY")
        print(f"{'=' * 80}")
        print(f"Total symbols: {len(results_df)}")
        print(f"Available:     {len(available_df)} ({len(available_df)/len(results_df)*100:.1f}%)")
        print(f"Unavailable:   {len(unavailable_df)} ({len(unavailable_df)/len(results_df)*100:.1f}%)")
        
        # Show sample of unavailable symbols
        if len(unavailable_df) > 0:
            print(f"\nSample of unavailable symbols:")
            sample_cols = [col for col in ['Symbol', 'Name', 'Category', 'error'] if col in unavailable_df.columns]
            sample = unavailable_df[sample_cols].head(10)
            print(sample.to_string(index=False))
        
        # Show sample of available symbols
        if len(available_df) > 0:
            print(f"\nSample of available symbols:")
            sample_cols = [col for col in ['Symbol', 'Name', 'Category', 'latest_date', 'latest_close'] if col in available_df.columns]
            sample = available_df[sample_cols].head(10)
            print(sample.to_string(index=False))
        
        print(f"\n{'=' * 80}")
        
        return 0
        
    finally:
        await validator.close()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
