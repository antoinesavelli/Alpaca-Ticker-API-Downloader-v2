"""
Script to fetch and count all stocks with market cap < $2B from Yahoo Finance.
Includes Small Cap ($300M-$2B), Micro Cap ($50M-$300M), and Nano Cap (<$50M).
"""
import logging
import yfinance as yf
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime
import requests
from io import StringIO
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger


# Define User-Agent to avoid 403 errors
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


def get_all_us_stocks(logger: logging.Logger):
    """
    Get comprehensive list of US stock symbols from multiple sources.
    
    Args:
        logger: Logger instance
        
    Returns:
        List of stock symbols
    """
    all_symbols = set()
    
    # Source 1: NASDAQ listed stocks
    try:
        logger.info("Fetching NASDAQ stocks...")
        nasdaq_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25000&exchange=NASDAQ"
        response = requests.get(nasdaq_url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'rows' in data['data']:
                symbols = [row['symbol'] for row in data['data']['rows']]
                all_symbols.update(symbols)
                logger.info(f"✓ Found {len(symbols)} NASDAQ stocks")
    except Exception as e:
        logger.warning(f"Failed to fetch NASDAQ stocks: {e}")
    
    # Source 2: NYSE listed stocks
    try:
        logger.info("Fetching NYSE stocks...")
        nyse_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25000&exchange=NYSE"
        response = requests.get(nyse_url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'rows' in data['data']:
                symbols = [row['symbol'] for row in data['data']['rows']]
                all_symbols.update(symbols)
                logger.info(f"✓ Found {len(symbols)} NYSE stocks")
    except Exception as e:
        logger.warning(f"Failed to fetch NYSE stocks: {e}")
    
    # Source 3: AMEX listed stocks
    try:
        logger.info("Fetching AMEX stocks...")
        amex_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25000&exchange=AMEX"
        response = requests.get(amex_url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'rows' in data['data']:
                symbols = [row['symbol'] for row in data['data']['rows']]
                all_symbols.update(symbols)
                logger.info(f"✓ Found {len(symbols)} AMEX stocks")
    except Exception as e:
        logger.warning(f"Failed to fetch AMEX stocks: {e}")
    
    # Source 4: FTP NASDAQ symbol list (alternative)
    if len(all_symbols) < 1000:
        try:
            logger.info("Trying alternative source: NASDAQ FTP...")
            ftp_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
            df = pd.read_csv(ftp_url, sep="|")
            symbols = df['Symbol'].dropna().tolist()
            all_symbols.update(symbols)
            logger.info(f"✓ Found {len(symbols)} stocks from NASDAQ FTP")
        except Exception as e:
            logger.warning(f"Failed to fetch from NASDAQ FTP: {e}")
    
    # Remove invalid symbols (with special characters, too short/long)
    valid_symbols = [s for s in all_symbols if is_valid_symbol(s)]
    
    logger.info(f"Total unique valid symbols collected: {len(valid_symbols)}")
    return sorted(valid_symbols)


def is_valid_symbol(symbol: str) -> bool:
    """
    Check if symbol is valid for processing.
    
    Args:
        symbol: Stock symbol
        
    Returns:
        True if valid, False otherwise
    """
    if not symbol or len(symbol) < 1 or len(symbol) > 5:
        return False
    
    # Exclude symbols with special characters (preferred stocks, warrants, etc.)
    if any(char in symbol for char in ['^', '.', '/', '-', '~']):
        return False
    
    return True


def categorize_by_market_cap(market_cap: float) -> str:
    """
    Categorize stock by market cap.
    
    Args:
        market_cap: Market capitalization value
        
    Returns:
        Category string
    """
    if market_cap >= 300_000_000:  # $300M+
        return "Small Cap"
    elif market_cap >= 50_000_000:  # $50M - $300M
        return "Micro Cap"
    else:  # < $50M
        return "Nano Cap"


def check_market_cap(symbol: str, logger: logging.Logger, max_cap: float = 2_000_000_000):
    """
    Check if stock's market cap is under the threshold.
    
    Args:
        symbol: Stock symbol
        logger: Logger instance
        max_cap: Maximum market cap (default: $2B)
        
    Returns:
        Tuple of (symbol, market_cap, category, name) or None if error
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Get market cap
        market_cap = info.get('marketCap', 0)
        
        # Only include stocks with market cap data and under threshold
        if market_cap and 0 < market_cap <= max_cap:
            category = categorize_by_market_cap(market_cap)
            name = info.get('shortName', '')
            logger.debug(f"✓ {symbol}: ${market_cap:,.0f} - {category}")
            return (symbol, market_cap, category, name)
        else:
            return None
            
    except Exception as e:
        logger.debug(f"Error checking {symbol}: {e}")
        return None


def get_stocks_under_threshold(logger: logging.Logger, max_workers: int = 10):
    """
    Filter stocks by market cap using Yahoo Finance.
    
    Args:
        logger: Logger instance
        max_workers: Number of parallel threads for checking stocks
        
    Returns:
        DataFrame with stock information
    """
    # Get all US stocks
    logger.info("Step 1: Collecting all US stock symbols...")
    all_symbols = get_all_us_stocks(logger)
    
    if not all_symbols:
        logger.error("Failed to collect stock symbols")
        return pd.DataFrame()
    
    logger.info(f"Step 2: Checking market cap for {len(all_symbols)} stocks...")
    logger.info(f"This may take several minutes. Using {max_workers} parallel threads.")
    
    stocks_found = []
    checked_count = 0
    error_count = 0
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(check_market_cap, symbol, logger): symbol 
            for symbol in all_symbols
        }
        
        # Process results as they complete
        for future in as_completed(future_to_symbol):
            checked_count += 1
            
            # Log progress every 100 stocks
            if checked_count % 100 == 0:
                logger.info(f"Progress: {checked_count}/{len(all_symbols)} stocks checked, "
                          f"{len(stocks_found)} stocks found (< $2B market cap)")
            
            try:
                result = future.result()
                if result:
                    symbol, market_cap, category, name = result
                    stocks_found.append({
                        'Symbol': symbol,
                        'Name': name,
                        'Market_Cap': market_cap,
                        'Market_Cap_Formatted': f"${market_cap:,.0f}",
                        'Category': category,
                        'Source': 'Yahoo Finance'
                    })
                else:
                    error_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.debug(f"Error processing future: {e}")
    
    logger.info(f"Completed! Checked {checked_count} stocks, {error_count} errors")
    logger.info(f"Found {len(stocks_found)} stocks with market cap < $2B")
    
    if stocks_found:
        df = pd.DataFrame(stocks_found)
        # Sort by market cap descending
        df = df.sort_values('Market_Cap', ascending=False).reset_index(drop=True)
        return df
    
    return pd.DataFrame()


def get_smallcap_stocks():
    """
    Main function to fetch and count stocks under $2B market cap.
    
    Returns:
        tuple: (count of stocks, DataFrame with stock details)
    """
    logger = setup_logger(
        name="yahoo_smallcap_counter",
        log_file="logs/yahoo_smallcap_count.log",
        log_level="INFO"
    )
    
    logger.info("=" * 80)
    logger.info("Yahoo Finance Stock Counter - All Stocks < $2B Market Cap")
    logger.info("=" * 80)
    logger.info("Categories:")
    logger.info("  - Small Cap: $300M - $2B")
    logger.info("  - Micro Cap: $50M - $300M")
    logger.info("  - Nano Cap:  < $50M")
    logger.info("=" * 80)
    
    try:
        # Get stocks under $2B market cap
        df = get_stocks_under_threshold(logger, max_workers=20)
        
        if not df.empty:
            count = len(df)
            
            # Count by category
            category_counts = df['Category'].value_counts()
            
            logger.info("=" * 80)
            logger.info(f"FINAL RESULTS: {count} stocks found with market cap < $2B")
            logger.info("-" * 80)
            for category in ['Small Cap', 'Micro Cap', 'Nano Cap']:
                cat_count = category_counts.get(category, 0)
                logger.info(f"  {category}: {cat_count}")
            logger.info("=" * 80)
            
            return count, df
        else:
            logger.error("No stocks found")
            return 0, pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error in get_smallcap_stocks: {e}", exc_info=True)
        return 0, pd.DataFrame()


def save_results(count: int, df: pd.DataFrame, output_path: str = "output/stocks_under_2b.csv"):
    """
    Save results to CSV file.
    
    Args:
        count: Number of stocks
        df: DataFrame with stock details
        output_path: Path to save CSV file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"\n✓ Results saved to: {output_path}")


def main():
    """Main execution function."""
    start_time = time.time()
    
    print("=" * 80)
    print("Yahoo Finance Stock Counter - All Stocks < $2B Market Cap")
    print("=" * 80)
    print("Fetching and filtering stocks by market cap...")
    print("  - Small Cap: $300M - $2B")
    print("  - Micro Cap: $50M - $300M")
    print("  - Nano Cap:  < $50M")
    print("-" * 80)
    
    count, df = get_smallcap_stocks()
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'=' * 80}")
    print(f"TOTAL STOCKS (Market Cap < $2B): {count}")
    print(f"Time taken: {elapsed_time:.1f} seconds")
    print(f"{'=' * 80}")
    
    if not df.empty:
        # Count by category
        category_counts = df['Category'].value_counts()
        
        print("\nBreakdown by Category:")
        print("-" * 80)
        for category in ['Small Cap', 'Micro Cap', 'Nano Cap']:
            cat_count = category_counts.get(category, 0)
            percentage = (cat_count / count * 100) if count > 0 else 0
            print(f"  {category:12} : {cat_count:5} stocks ({percentage:.1f}%)")
        
        print(f"\nTop 20 stocks by market cap:")
        print("-" * 80)
        display_df = df[['Symbol', 'Name', 'Market_Cap_Formatted', 'Category']].head(20)
        print(display_df.to_string(index=True))
        
        # Save to file
        save_results(count, df)
        
        # Statistics
        print(f"\n{'=' * 80}")
        print("Market Cap Statistics:")
        print(f"  Largest:  ${df['Market_Cap'].max():,.0f}")
        print(f"  Smallest: ${df['Market_Cap'].min():,.0f}")
        print(f"  Average:  ${df['Market_Cap'].mean():,.0f}")
        print(f"  Median:   ${df['Market_Cap'].median():,.0f}")
        print(f"{'=' * 80}")
    else:
        print("\n❌ No data retrieved. Check logs for details.")
    
    return count


if __name__ == "__main__":
    result = main()
    sys.exit(0 if result > 0 else 1)