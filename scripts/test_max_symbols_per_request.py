"""
Test maximum number of symbols Alpaca accepts in a single API request.
Finds the practical limit for comma-separated symbol lists.
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from utils.logger import setup_logger


class SymbolLimitTester:
    """Test maximum symbols per request."""
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger('symbol_limit_test', 'logs/symbol_limit_test.log', 'INFO')
        
        # Load test symbols
        self.all_symbols = self._load_symbols()
        
        # Test increments
        self.test_sizes = [1, 5, 10, 25, 50, 100, 150, 200, 250, 300, 400, 500, 750, 1000]
    
    def _load_symbols(self) -> List[str]:
        """Load symbols from universe file."""
        import pandas as pd
        df = pd.read_csv(self.config.SYMBOL_FILE)
        symbols = df['symbol'].tolist()
        self.logger.info(f"Loaded {len(symbols)} symbols from {self.config.SYMBOL_FILE}")
        return symbols
    
    async def run_test(self):
        """Run the test across different symbol counts."""
        self.logger.info("="*80)
        self.logger.info("ALPACA MAX SYMBOLS PER REQUEST TEST")
        self.logger.info("="*80)
        self.logger.info("")
        
        api = AlpacaAPI(self.config, self.logger)
        
        max_working = 0
        
        for symbol_count in self.test_sizes:
            if symbol_count > len(self.all_symbols):
                self.logger.info(f"\n⏭️  Skipping {symbol_count} (only have {len(self.all_symbols)} symbols)")
                break
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"🧪 Testing {symbol_count} symbols in one request")
            self.logger.info(f"{'='*60}")
            
            test_symbols = self.all_symbols[:symbol_count]
            symbols_str = ",".join(test_symbols)
            
            # Build test request (1 day only for speed)
            url = f"{api.base_url}{self.config.ALPACA_ENDPOINT}"
            params = {
                "symbols": symbols_str,
                "timeframe": "1Day",
                "start": "2024-12-20T00:00:00-05:00",
                "end": "2024-12-21T00:00:00-05:00",
                "limit": 10000,
                "feed": self.config.FEED,
                "adjustment": "raw",
                "sort": "asc"
            }
            
            self.logger.info(f"  📏 Symbol string length: {len(symbols_str):,} characters")
            self.logger.info(f"  📋 First 5 symbols: {', '.join(test_symbols[:5])}")
            self.logger.info(f"  ⏳ Sending request...")
            
            try:
                import time
                start_time = time.time()
                
                response = await api.session.get(url, params=params)
                
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Count symbols with data
                    bars_dict = data.get("bars", {})
                    symbols_with_data = len(bars_dict)
                    total_bars = sum(len(bars) for bars in bars_dict.values())
                    
                    self.logger.info(f"  ✅ SUCCESS in {duration:.2f}s")
                    self.logger.info(f"     Response size: {len(response.text):,} bytes")
                    self.logger.info(f"     Symbols with data: {symbols_with_data}/{symbol_count}")
                    self.logger.info(f"     Total bars: {total_bars:,}")
                    
                    max_working = symbol_count
                    
                elif response.status_code == 400:
                    self.logger.error(f"  ❌ BAD REQUEST (400) in {duration:.2f}s")
                    self.logger.error(f"     Response: {response.text[:500]}")
                    self.logger.error(f"     Limit reached at {symbol_count} symbols")
                    break
                    
                elif response.status_code == 414:
                    self.logger.error(f"  ❌ URI TOO LONG (414) in {duration:.2f}s")
                    self.logger.error(f"     URL limit reached at {symbol_count} symbols")
                    self.logger.error(f"     String length was: {len(symbols_str):,} characters")
                    break
                    
                else:
                    self.logger.error(f"  ❌ ERROR {response.status_code} in {duration:.2f}s")
                    self.logger.error(f"     Response: {response.text[:500]}")
                    break
                
                # Small delay between tests
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"  ❌ Exception: {e}")
                break
        
        await api.close()
        
        # Show results
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("🎯 TEST RESULTS")
        self.logger.info("="*80)
        self.logger.info(f"Maximum working symbols per request: {max_working}")
        self.logger.info("")
        
        if max_working > 0:
            # Recommend with safety margin
            recommended = int(max_working * 0.8)  # 80% of max
            
            self.logger.info(f"💡 RECOMMENDED SETTING (80% safety margin):")
            self.logger.info(f"")
            self.logger.info(f"   config/settings.py:")
            self.logger.info(f"   self.SYMBOLS_PER_BATCH = {recommended}")
            self.logger.info(f"")
            
            # Show efficiency gain
            old_requests = 4292  # Your total symbols
            new_requests = (4292 // recommended) + 1
            efficiency_gain = (old_requests / new_requests) - 1
            
            self.logger.info(f"📊 Efficiency Impact:")
            self.logger.info(f"   With 1 symbol/request: 4,292 API calls")
            self.logger.info(f"   With {recommended} symbols/request: {new_requests} API calls")
            self.logger.info(f"   Reduction: {efficiency_gain*100:.1f}% fewer API calls!")
        else:
            self.logger.error("❌ No successful tests - check API connection")
        
        self.logger.info("="*80)


async def main():
    """Run the tester."""
    tester = SymbolLimitTester()
    await tester.run_test()


if __name__ == "__main__":
    asyncio.run(main())
