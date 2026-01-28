"""
Check how far back Alpaca API historical data is available.
Uses adaptive random sampling to efficiently find the earliest date.
"""
import asyncio
import sys
import random
import math
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from utils.logger import setup_logger
from utils.trading_calendar import TradingCalendar


async def check_date_availability(api: AlpacaAPI, symbol: str, date: str) -> dict:
    """
    Check if data is available for a specific date.
    
    Args:
        api: Alpaca API instance
        symbol: Symbol to test (e.g., 'SPY')
        date: Date in YYYYMMDD format
        
    Returns:
        dict: {'date': str, 'available': bool, 'bar_count': int, 'error': str or None}
    """
    try:
        # Convert YYYYMMDD to YYYY-MM-DD for API
        date_api = f"{date[:4]}-{date[4:6]}-{date[6:]}"  
        
        url = f"{api.base_url}/v2/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": "1Day",
            "start": date_api,
            "end": date_api,
            "limit": 10,
            "feed": api.config.FEED
        }
        
        response = await api.session.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            bars = data.get("bars", {}).get(symbol, [])
            return {
                'date': date,
                'available': len(bars) > 0,
                'bar_count': len(bars),
                'error': None
            }
        elif response.status_code == 422:
            # Invalid date or out of range
            return {
                'date': date,
                'available': False,
                'bar_count': 0,
                'error': f"Invalid date (422)"
            }
        else:
            return {
                'date': date,
                'available': False,
                'bar_count': 0,
                'error': f"API error {response.status_code}"
            }
            
    except Exception as e:
        return {
            'date': date,
            'available': False,
            'bar_count': 0,
            'error': str(e)
        }


async def sample_year(api: AlpacaAPI, calendar: TradingCalendar, year: int, symbol: str, logger) -> dict:
    """
    Sample a single year using sqrt(n) random trading days.
    
    Returns:
        dict: {
            'year': int,
            'trading_days': list,
            'sample_size': int,
            'sampled_dates': list,
            'available_dates': list,
            'has_data': bool
        }
    """
    # Get all trading days for this year
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    
    trading_days, _ = calendar.get_trading_days_with_skipped(start_date, end_date)
    
    if not trading_days:
        return {
            'year': year,
            'trading_days': [],
            'sample_size': 0,
            'sampled_dates': [],
            'available_dates': [],
            'has_data': False
        }
    
    # Sample size = sqrt(total trading days)
    sample_size = max(1, int(math.sqrt(len(trading_days))))
    
    # Random sample
    sampled_dates = random.sample(trading_days, min(sample_size, len(trading_days)))
    sampled_dates.sort()
    
    logger.info(f"  Year {year}: {len(trading_days)} trading days, sampling {sample_size} dates")
    
    # Check each sampled date
    available_dates = []
    for date in sampled_dates:
        result = await check_date_availability(api, symbol, date)
        if result['available']:
            available_dates.append(date)
        await asyncio.sleep(0.1)  # Rate limit
    
    has_data = len(available_dates) > 0
    logger.info(f"    → {len(available_dates)}/{sample_size} sampled dates have data")
    
    return {
        'year': year,
        'trading_days': trading_days,
        'sample_size': sample_size,
        'sampled_dates': sampled_dates,
        'available_dates': available_dates,
        'has_data': has_data
    }


async def test_backwards_until_empty(api: AlpacaAPI, calendar: TradingCalendar, start_date: str, symbol: str, logger) -> str:
    """
    Test backwards from start_date until 3 consecutive empty days found.
    
    Returns:
        str: Earliest date with data (YYYYMMDD)
    """
    logger.info(f"\n🔍 Testing backwards from {start_date} until 3 consecutive empty days...")
    
    # Get trading days going backwards from start_date
    # Go back 2 years max to avoid infinite loop
    two_years_ago = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=730)).strftime('%Y%m%d')
    
    trading_days, _ = calendar.get_trading_days_with_skipped(two_years_ago, start_date)
    trading_days.reverse()  # Reverse to go backwards
    
    logger.info(f"   Testing up to {len(trading_days)} trading days backwards")
    
    consecutive_empty = 0
    earliest_with_data = None
    
    for i, date in enumerate(trading_days):
        result = await check_date_availability(api, symbol, date)
        
        if result['available']:
            consecutive_empty = 0
            earliest_with_data = date
            logger.info(f"   [{i+1:3d}] {date}: ✓ Available")
        else:
            consecutive_empty += 1
            logger.info(f"   [{i+1:3d}] {date}: ✗ Empty (streak: {consecutive_empty})")
            
            if consecutive_empty >= 3:
                logger.info(f"\n✓ Found boundary: 3 consecutive empty days")
                logger.info(f"   Earliest available: {earliest_with_data}")
                break
        
        await asyncio.sleep(0.1)  # Rate limit
    
    return earliest_with_data


async def main():
    """Main function with adaptive sampling strategy."""
    # Setup
    config = Config()
    
    # Create log file path
    log_file = Path("logs") / f"data_availability_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logger("data_availability_check", str(log_file), log_level="INFO")
    
    logger.info("="*80)
    logger.info("ALPACA API DATA AVAILABILITY CHECK")
    logger.info("ADAPTIVE RANDOM SAMPLING STRATEGY")
    logger.info("="*80)
    logger.info(f"API Base URL: {config.ALPACA_BASE_URL}")
    logger.info(f"Feed: {config.FEED}")
    logger.info(f"Test Symbol: SPY (S&P 500 ETF)")
    logger.info("="*80)
    
    api = AlpacaAPI(config, logger)
    calendar = TradingCalendar(logger=logger)
    
    try:
        # Test connection
        logger.info("\n🔌 Testing API connection...")
        if not await api.check_terminal_running():
            logger.error("❌ API connection failed. Check your credentials.")
            return
        
        logger.info("✓ API connection successful\n")
        
        # PHASE 1: Random sample each year going backwards until 2 empty years
        logger.info("="*80)
        logger.info("PHASE 1: YEARLY RANDOM SAMPLING")
        logger.info("="*80)
        logger.info("Strategy: Sample sqrt(n) random days per year, stop after 2 empty years\n")
        
        current_year = datetime.now().year
        year_results = []
        consecutive_empty_years = 0
        
        for year in range(current_year, 1989, -1):  # Go back to 1990
            result = await sample_year(api, calendar, year, "SPY", logger)
            year_results.append(result)
            
            if not result['has_data']:
                consecutive_empty_years += 1
                if consecutive_empty_years >= 2:
                    logger.info(f"\n✓ Found 2 consecutive empty years: {year} and {year+1}")
                    logger.info(f"   Stopping backward scan")
                    break
            else:
                consecutive_empty_years = 0
        
        # Find first empty year and last populated year
        empty_years = [r for r in year_results if not r['has_data']]
        populated_years = [r for r in year_results if r['has_data']]
        
        if not populated_years:
            logger.error("\n❌ No data found in any year!")
            return
        
        first_empty_year = empty_years[0]['year'] if empty_years else None
        last_populated_year = populated_years[-1]['year']
        
        logger.info("\n" + "="*80)
        logger.info("PHASE 1 RESULTS")
        logger.info("="*80)
        logger.info(f"Years sampled: {len(year_results)}")
        logger.info(f"Years with data: {len(populated_years)}")
        logger.info(f"Years without data: {len(empty_years)}")
        logger.info(f"Last populated year: {last_populated_year}")
        if first_empty_year:
            logger.info(f"First empty year: {first_empty_year}")
        
        # Get all available dates from first pass
        all_available_dates = []
        for result in populated_years:
            all_available_dates.extend(result['available_dates'])
        all_available_dates.sort()
        
        oldest_from_phase1 = min(all_available_dates) if all_available_dates else None
        logger.info(f"Oldest date found: {oldest_from_phase1}")
        
        # PHASE 2: Re-sample first empty year
        if first_empty_year:
            logger.info("\n" + "="*80)
            logger.info("PHASE 2: RE-SAMPLING FIRST EMPTY YEAR")
            logger.info("="*80)
            
            resample_result = await sample_year(api, calendar, first_empty_year, "SPY", logger)
            
            if resample_result['has_data']:
                logger.info(f"\n✓ Found data in {first_empty_year} on second sampling!")
                all_available_dates.extend(resample_result['available_dates'])
                all_available_dates.sort()
        
        # PHASE 3: Re-sample last populated year for more precision
        logger.info("\n" + "="*80)
        logger.info("PHASE 3: RE-SAMPLING LAST POPULATED YEAR")
        logger.info("="*80)
        
        resample_populated = await sample_year(api, calendar, last_populated_year, "SPY", logger)
        all_available_dates.extend(resample_populated['available_dates'])
        all_available_dates = sorted(set(all_available_dates))  # Remove duplicates
        
        oldest_from_sampling = min(all_available_dates)
        logger.info(f"\n✓ Oldest date from all sampling: {oldest_from_sampling}")
        
        # PHASE 4: Test backwards from oldest found until 3 consecutive empty days
        logger.info("\n" + "="*80)
        logger.info("PHASE 4: BACKWARD TESTING FOR EXACT BOUNDARY")
        logger.info("="*80)
        
        earliest_exact = await test_backwards_until_empty(
            api, calendar, oldest_from_sampling, "SPY", logger
        )
        
        # Final summary
        logger.info("\n" + "="*80)
        logger.info("FINAL SUMMARY")
        logger.info("="*80)
        logger.info(f"Symbol tested: SPY")
        logger.info(f"Feed: {config.FEED}")
        logger.info(f"")
        logger.info(f"📅 Earliest available date: {earliest_exact}")
        
        # Calculate coverage
        latest_date = datetime.now().strftime('%Y%m%d')
        earliest_dt = datetime.strptime(earliest_exact, '%Y%m%d')
        latest_dt = datetime.strptime(latest_date, '%Y%m%d')
        years_of_data = (latest_dt - earliest_dt).days / 365.25
        
        # Count trading days in range
        all_trading_days, _ = calendar.get_trading_days_with_skipped(earliest_exact, latest_date)
        
        logger.info(f"📅 Latest available date:   {latest_date}")
        logger.info(f"")
        logger.info(f"📊 Calendar days: {(latest_dt - earliest_dt).days:,} days ({years_of_data:.1f} years)")
        logger.info(f"📊 Trading days: {len(all_trading_days):,} days")
        logger.info("="*80)
        
        # Additional info
        logger.info("\n💡 METHODOLOGY:")
        logger.info("  1. Random sampled sqrt(n) days per year going backwards")
        logger.info("  2. Stopped after 2 consecutive empty years")
        logger.info("  3. Re-sampled first empty year and last populated year")
        logger.info("  4. Tested backwards from oldest found until 3 consecutive empty days")
        logger.info("\n💡 NOTES:")
        logger.info("  - All tests performed on TRADING DAYS ONLY")
        logger.info("  - SIP feed typically has more historical data than IEX")
        logger.info("  - SPY (S&P 500 ETF) started trading in January 1993")
        
    finally:
        await api.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
