"""
Auto-tune API limits to find practical maximum throughput.
Incrementally increases concurrency until errors occur.
"""
import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any  # ← ADD THIS
import sys
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from processors.data_parser import DataParser
from utils.logger import setup_logger


class APILimitTuner:
    """Auto-tune API limits by testing incrementally."""
    
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger('api_tuner', 'logs/api_tuner.log', 'INFO')
        
        # Test parameters
        self.test_symbols = ["SPY", "AAPL", "MSFT", "GOOGL", "AMZN"]
        self.test_start = "20241220"  # 2 weeks
        self.test_end = "20241231"
        
        # Tuning ranges
        self.concurrent_tests = [1, 5, 10, 25, 50, 75, 100, 150, 200]
        self.delay_tests = [0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.006]  # seconds
        
        self.results = []
    
    async def run_tuning(self):
        """Run full tuning process."""
        self.logger.info("="*80)
        self.logger.info("ALPACA API LIMIT AUTO-TUNER")
        self.logger.info("="*80)
        self.logger.info(f"Theoretical limit: 100,000 calls/min = 1,666 calls/sec")
        self.logger.info(f"Test date range: {self.test_start} to {self.test_end}")
        self.logger.info(f"Test symbols: {', '.join(self.test_symbols)}")
        self.logger.info("")
        
        # Phase 1: Find optimal concurrency (with safe delay)
        self.logger.info("PHASE 1: Testing Concurrent Request Limits")
        self.logger.info("-"*80)
        
        best_concurrency = await self._test_concurrency_levels()
        
        # Phase 2: Find minimal delay (with optimal concurrency)
        self.logger.info("\nPHASE 2: Testing Rate Limit Delays")
        self.logger.info("-"*80)
        
        best_delay = await self._test_delay_levels(best_concurrency)
        
        # Phase 3: Final validation test
        self.logger.info("\nPHASE 3: Final Validation")
        self.logger.info("-"*80)
        
        final_result = await self._run_validation_test(best_concurrency, best_delay)
        
        # Show recommendations
        self._show_recommendations(best_concurrency, best_delay, final_result)
        
        # Save results
        self._save_results()
    
    async def _test_concurrency_levels(self) -> int:
        """Test different concurrency levels."""
        best_concurrency = 1
        best_throughput = 0
        
        for concurrent_requests in self.concurrent_tests:
            self.logger.info(f"\n🧪 Testing {concurrent_requests} concurrent requests...")
            
            result = await self._run_test(
                concurrent_requests=concurrent_requests,
                rate_delay=0.1,  # Conservative delay for testing
                test_name=f"concurrency_{concurrent_requests}"
            )
            
            self.results.append(result)
            
            # Check for errors
            if result['errors'] > 0:
                self.logger.warning(f"   ⚠️  Errors detected at {concurrent_requests} concurrent requests")
                self.logger.info(f"   Stopping concurrency tests - limit found")
                break
            
            # Track best
            if result['throughput'] > best_throughput:
                best_throughput = result['throughput']
                best_concurrency = concurrent_requests
                self.logger.info(f"   ✓ New best: {best_throughput:.0f} records/sec")
            else:
                self.logger.info(f"   Throughput plateaued - stopping concurrency tests")
                break
        
        self.logger.info(f"\n🏆 Best concurrency: {best_concurrency} requests ({best_throughput:.0f} records/sec)")
        return best_concurrency
    
    async def _test_delay_levels(self, concurrency: int) -> float:
        """Test different rate delays."""
        best_delay = 0.1
        best_throughput = 0
        
        for delay in self.delay_tests:
            calls_per_sec = concurrency / delay if delay > 0 else 999999
            
            if calls_per_sec > 1700:  # Don't exceed theoretical max
                self.logger.info(f"\n⏭️  Skipping {delay}s delay ({calls_per_sec:.0f} calls/sec exceeds safe limit)")
                continue
            
            self.logger.info(f"\n🧪 Testing {delay}s delay ({calls_per_sec:.0f} theoretical calls/sec)...")
            
            result = await self._run_test(
                concurrent_requests=concurrency,
                rate_delay=delay,
                test_name=f"delay_{delay}"
            )
            
            self.results.append(result)
            
            # Check for errors
            if result['errors'] > 0:
                self.logger.warning(f"   ⚠️  Errors detected at {delay}s delay")
                self.logger.info(f"   Stopping delay tests - limit found")
                break
            
            # Track best
            if result['throughput'] > best_throughput:
                best_throughput = result['throughput']
                best_delay = delay
                self.logger.info(f"   ✓ New best: {best_throughput:.0f} records/sec")
            else:
                self.logger.info(f"   Throughput plateaued")
        
        self.logger.info(f"\n🏆 Best delay: {best_delay}s ({best_throughput:.0f} records/sec)")
        return best_delay
    
    async def _run_test(
        self, 
        concurrent_requests: int, 
        rate_delay: float,
        test_name: str
    ) -> Dict[str, Any]:
        """Run a single test configuration."""
        # Update config
        self.config.MAX_CONCURRENT_REQUESTS = concurrent_requests
        self.config.RATE_LIMIT_DELAY = rate_delay
        self.config.ENABLE_DYNAMIC_RATE_LIMITING = False  # Disable for testing
        
        # Initialize API
        api = AlpacaAPI(self.config, self.logger)
        parser = DataParser(self.logger, config=self.config)
        
        # Create test batches
        symbol_batches = [self.test_symbols[i:i+1] for i in range(len(self.test_symbols))]
        
        start_time = time.time()
        total_records = 0
        total_requests = 0
        errors = 0
        error_429 = 0
        timeouts = 0
        
        # Run test batches
        semaphore = asyncio.Semaphore(concurrent_requests)
        
        async def fetch_one_symbol(symbol: str):
            nonlocal total_records, total_requests, errors, error_429, timeouts
            
            async with semaphore:
                try:
                    total_requests += 1
                    
                    # Build request (same as bulk downloader)
                    iso_start = self.config.to_iso_date(self.test_start)
                    iso_end = self.config.to_iso_date(self.test_end)
                    start_ts = f"{iso_start}T{self.config.START_TIME}-05:00"
                    end_ts = f"{iso_end}T{self.config.END_TIME}-05:00"
                    
                    url = f"{api.base_url}{self.config.ALPACA_ENDPOINT}"
                    params = {
                        "symbols": symbol,
                        "timeframe": self.config.TIMEFRAME,
                        "start": start_ts,
                        "end": end_ts,
                        "limit": self.config.LIMIT,
                        "adjustment": self.config.ADJUSTMENT,
                        "feed": self.config.FEED,
                        "sort": self.config.SORT
                    }
                    
                    response = await api.session.get(url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "bars" in data and symbol in data["bars"]:
                            bars = data["bars"][symbol]
                            total_records += len(bars)
                    elif response.status_code == 429:
                        error_429 += 1
                        errors += 1
                    else:
                        errors += 1
                    
                except asyncio.TimeoutError:
                    timeouts += 1
                    errors += 1
                except Exception as e:
                    errors += 1
                
                # Rate limiting
                if rate_delay > 0:
                    await asyncio.sleep(rate_delay)
        
        # Run all test fetches
        tasks = [fetch_one_symbol(sym) for sym in self.test_symbols]
        await asyncio.gather(*tasks)
        
        # Close API
        await api.close()
        
        duration = time.time() - start_time
        throughput = total_records / duration if duration > 0 else 0
        calls_per_sec = total_requests / duration if duration > 0 else 0
        
        result = {
            'test_name': test_name,
            'concurrent_requests': concurrent_requests,
            'rate_delay': rate_delay,
            'duration': duration,
            'total_requests': total_requests,
            'total_records': total_records,
            'errors': errors,
            'error_429': error_429,
            'timeouts': timeouts,
            'throughput': throughput,
            'calls_per_sec': calls_per_sec,
            'theoretical_max_calls_per_sec': concurrent_requests / rate_delay if rate_delay > 0 else 999999
        }
        
        # Log results
        self.logger.info(f"   Duration: {duration:.2f}s")
        self.logger.info(f"   Requests: {total_requests} ({calls_per_sec:.1f} calls/sec)")
        self.logger.info(f"   Records: {total_records:,} ({throughput:.0f} records/sec)")
        self.logger.info(f"   Errors: {errors} (429s: {error_429}, Timeouts: {timeouts})")
        
        return result
    
    async def _run_validation_test(self, concurrency: int, delay: float) -> Dict[str, Any]:
        """Run final validation with recommended settings."""
        self.logger.info(f"🧪 Final validation test:")
        self.logger.info(f"   Concurrency: {concurrency}")
        self.logger.info(f"   Delay: {delay}s")
        self.logger.info(f"   Running 3 iterations...")
        
        results = []
        for i in range(3):
            self.logger.info(f"\n   Iteration {i+1}/3...")
            result = await self._run_test(
                concurrent_requests=concurrency,
                rate_delay=delay,
                test_name=f"validation_{i+1}"
            )
            results.append(result)
            
            if result['errors'] > 0:
                self.logger.warning(f"   ⚠️  Errors in iteration {i+1} - settings may be too aggressive")
            
            await asyncio.sleep(2)  # Brief pause between iterations
        
        # Average results
        avg_throughput = sum(r['throughput'] for r in results) / len(results)
        avg_calls_per_sec = sum(r['calls_per_sec'] for r in results) / len(results)
        total_errors = sum(r['errors'] for r in results)
        
        self.logger.info(f"\n✓ Validation complete:")
        self.logger.info(f"   Average throughput: {avg_throughput:.0f} records/sec")
        self.logger.info(f"   Average call rate: {avg_calls_per_sec:.1f} calls/sec")
        self.logger.info(f"   Total errors: {total_errors}")
        
        return {
            'avg_throughput': avg_throughput,
            'avg_calls_per_sec': avg_calls_per_sec,
            'total_errors': total_errors,
            'iterations': results
        }
    
    def _show_recommendations(self, concurrency: int, delay: float, validation: Dict[str, Any]):
        """Show final recommendations."""
        self.logger.info("\n" + "="*80)
        self.logger.info("🎯 RECOMMENDED SETTINGS")
        self.logger.info("="*80)
        
        # Add safety margin
        safe_concurrency = int(concurrency * 0.9)  # 90% of max
        safe_delay = delay * 1.1  # 110% of min
        
        theoretical_calls_per_min = (safe_concurrency / safe_delay) * 60 if safe_delay > 0 else 999999
        
        self.logger.info(f"")
        self.logger.info(f"Add to config/settings.py:")
        self.logger.info(f"")
        self.logger.info(f"    # API LIMITS - AUTO-TUNED {datetime.now().strftime('%Y-%m-%d')}")
        self.logger.info(f"    self.MAX_CONCURRENT_REQUESTS = {safe_concurrency}")
        self.logger.info(f"    self.RATE_LIMIT_DELAY = {safe_delay:.3f}")
        self.logger.info(f"    self.REQUEST_TIMEOUT = 300  # 5 minutes")
        self.logger.info(f"")
        self.logger.info(f"Expected Performance:")
        self.logger.info(f"  📊 Throughput: ~{validation['avg_throughput']:.0f} records/sec")
        self.logger.info(f"  🚀 API Calls: ~{validation['avg_calls_per_sec']:.0f} calls/sec")
        self.logger.info(f"  📈 Theoretical Max: {theoretical_calls_per_min:,.0f} calls/min")
        self.logger.info(f"  ⚡ Utilization: {(theoretical_calls_per_min/100000)*100:.1f}% of your 100k/min limit")
        self.logger.info(f"")
        
        if validation['total_errors'] == 0:
            self.logger.info("✅ STABLE - No errors during validation")
        else:
            self.logger.warning(f"⚠️  {validation['total_errors']} errors during validation - consider adding more safety margin")
        
        self.logger.info("="*80)
    
    def _save_results(self):
        """Save test results to JSON."""
        output_file = Path("logs/api_tuning_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'theoretical_limit': '100,000 calls/min',
                'test_parameters': {
                    'symbols': self.test_symbols,
                    'date_range': f"{self.test_start} to {self.test_end}"
                },
                'results': self.results
            }, f, indent=2)
        
        self.logger.info(f"\n📄 Results saved to {output_file}")


async def main():
    """Run the tuner."""
    tuner = APILimitTuner()
    await tuner.run_tuning()


if __name__ == "__main__":
    asyncio.run(main())