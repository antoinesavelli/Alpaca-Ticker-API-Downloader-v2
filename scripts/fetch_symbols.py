"""
Fetch US stock symbols with market cap ≤ $2B using Alpaca + Yahoo Finance.
Alpaca provides the symbol list, Yahoo Finance provides market cap data.
Limited to NYSE, NASDAQ, and AMEX exchanges only.
"""
import asyncio
import csv
import logging
from datetime import datetime
from pathlib import Path
import httpx
from dotenv import load_dotenv
import os
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import time

# Load environment variables
load_dotenv('keys.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SmallCapSymbolFetcher:
    """Fetch US stock symbols with market cap <= $2B."""
    
    def __init__(self):
        self.api_key = os.getenv("ALPACA_API_KEY", "")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY", "")
        self.base_url = "https://paper-api.alpaca.markets"
        
        if not self.api_key or not self.secret_key:
            raise ValueError("Missing Alpaca API credentials in keys.env")
        
        self.headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "accept": "application/json"
        }
        
        self.session = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers=self.headers
        )
    
    async def get_all_assets(self):
        """Fetch all tradable US stocks from Alpaca (NYSE, NASDAQ, AMEX only)."""
        logger.info("Fetching all tradable US stocks from Alpaca...")
        logger.info("Exchanges: NYSE, NASDAQ, AMEX")
        
        url = f"{self.base_url}/v2/assets"
        params = {
            "status": "active",
            "asset_class": "us_equity",
            "exchange": "NASDAQ,NYSE,AMEX"  # Only major exchanges
        }
        
        try:
            response = await self.session.get(url, params=params)
            response.raise_for_status()
            assets = response.json()
            
            tradable_assets = [
                asset for asset in assets
                if asset.get('tradable', False) and asset.get('status') == 'active'
            ]
            
            logger.info(f"Found {len(tradable_assets)} tradable US stocks (NYSE, NASDAQ, AMEX)")
            return tradable_assets
            
        except Exception as e:
            logger.error(f"Failed to fetch assets: {e}")
            raise
    
    def get_market_cap(self, symbol: str):
        """
        Get market cap for a symbol using Yahoo Finance.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            tuple: (symbol, market_cap, name, exchange) or None if failed
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            market_cap = info.get('marketCap')
            name = info.get('longName', info.get('shortName', ''))
            exchange = info.get('exchange', '')
            
            if market_cap:
                return (symbol, market_cap, name, exchange)
            else:
                logger.debug(f"{symbol}: No market cap data available")
                return None
                
        except Exception as e:
            logger.debug(f"{symbol}: Error fetching market cap - {e}")
            return None
    
    def filter_by_market_cap_batch(self, symbols: list, max_market_cap: float, batch_size: int = 50):
        """
        Filter symbols by market cap using Yahoo Finance in batches.
        
        Args:
            symbols: List of stock symbols
            max_market_cap: Maximum market cap in dollars
            batch_size: Number of symbols to process concurrently
            
        Returns:
            List of tuples: (symbol, market_cap, name, exchange)
        """
        logger.info(f"Filtering {len(symbols)} symbols by market cap <= ${max_market_cap:,.0f}...")
        logger.info("This may take several minutes depending on the number of symbols.")
        
        filtered_stocks = []
        total = len(symbols)
        processed = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                # Process batch
                results = list(executor.map(self.get_market_cap, batch))
                
                # Filter by market cap
                for result in results:
                    if result:
                        symbol, market_cap, name, exchange = result
                        if market_cap <= max_market_cap:
                            filtered_stocks.append((symbol, market_cap, name, exchange))
                
                processed += len(batch)
                logger.info(f"Progress: {processed}/{total} ({processed/total*100:.1f}%) - Found {len(filtered_stocks)} small cap stocks")
                
                # Rate limiting to be respectful
                time.sleep(0.5)
        
        logger.info(f"✓ Found {len(filtered_stocks)} stocks with market cap <= ${max_market_cap:,.0f}")
        return filtered_stocks
    
    def export_to_csv(self, stocks: list, output_path: str):
        """
        Export stocks to CSV file.
        
        Args:
            stocks: List of tuples (symbol, market_cap, name, exchange)
            output_path: Path to output CSV file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Exporting {len(stocks)} symbols to {output_path}...")
        
        # Sort by market cap (largest to smallest)
        stocks_sorted = sorted(stocks, key=lambda x: x[1], reverse=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(['symbol', 'market_cap', 'market_cap_billions', 'name', 'exchange'])
            
            # Write stock data
            for symbol, market_cap, name, exchange in stocks_sorted:
                market_cap_billions = market_cap / 1_000_000_000
                writer.writerow([symbol, market_cap, f"{market_cap_billions:.2f}B", name, exchange])
        
        logger.info(f"✓ Successfully exported {len(stocks)} symbols to {output_path}")
    
    async def close(self):
        """Close the HTTP session."""
        await self.session.aclose()


async def main():
    """Main execution function."""
    # Configuration
    output_file = "T:/ticker_data/small_cap_symbols_2b.csv"
    max_market_cap = 2_000_000_000  # $2 billion
    
    logger.info("=" * 70)
    logger.info("Small Cap Symbol Fetcher (Market Cap ≤ $2B)")
    logger.info("Exchanges: NYSE, NASDAQ, AMEX")
    logger.info("=" * 70)
    
    fetcher = SmallCapSymbolFetcher()
    
    try:
        # Step 1: Fetch all US stocks from Alpaca (NYSE, NASDAQ, AMEX only)
        assets = await fetcher.get_all_assets()
        symbols = [asset['symbol'] for asset in assets]
        
        # Step 2: Filter by market cap using Yahoo Finance
        # Note: This runs synchronously due to yfinance library limitations
        filtered_stocks = fetcher.filter_by_market_cap_batch(
            symbols=symbols,
            max_market_cap=max_market_cap,
            batch_size=50
        )
        
        # Step 3: Export to CSV
        fetcher.export_to_csv(filtered_stocks, output_file)
        
        logger.info("=" * 70)
        logger.info("COMPLETE!")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Total symbols with market cap ≤ $2B: {len(filtered_stocks)}")
        logger.info("Exchanges: NYSE, NASDAQ, AMEX")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())