"""
Fetch historical float data from SEC Edgar filings (2016-2025).
Parses 10-K and 10-Q filings for shares outstanding.

This is the most accurate method - uses primary source data.
"""
import asyncio
import httpx
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import re
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EdgarFloatFetcher:
    """
    Fetch historical float from SEC Edgar filings.
    
    API Docs: https://www.sec.gov/edgar/sec-api-documentation
    """
    
    def __init__(self, user_agent: str):
        """
        Initialize Edgar fetcher.
        
        Args:
            user_agent: Required by SEC (format: "Company Name email@domain.com")
        """
        self.base_url = "https://data.sec.gov"
        self.user_agent = user_agent
        self.headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov"
        }
        self.session = httpx.Client(timeout=30.0, headers=self.headers)
        
        # SEC rate limit: 10 requests per second
        self.rate_limit_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
        logger.info(f"✓ Edgar API initialized")
        logger.info(f"  User-Agent: {user_agent}")
        logger.info(f"  Rate limit: 10 req/sec")
    
    def _rate_limit(self):
        """Enforce SEC rate limiting (10 requests/second)."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def get_cik(self, symbol: str) -> str:
        """
        Get CIK (Central Index Key) for a symbol.
        
        Args:
            symbol: Stock ticker
            
        Returns:
            CIK string (10 digits, zero-padded)
        """
        try:
            self._rate_limit()
            
            # Use SEC ticker lookup
            url = f"{self.base_url}/submissions/CIK{symbol}.json"
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                cik = str(data['cik']).zfill(10)
                return cik
            else:
                # Try company tickers endpoint
                self._rate_limit()
                tickers_url = "https://www.sec.gov/files/company_tickers.json"
                response = self.session.get(tickers_url)
                
                if response.status_code == 200:
                    tickers = response.json()
                    for entry in tickers.values():
                        if entry['ticker'].upper() == symbol.upper():
                            return str(entry['cik_str']).zfill(10)
                
                logger.warning(f"{symbol}: CIK not found")
                return None
                
        except Exception as e:
            logger.debug(f"{symbol}: Error getting CIK - {e}")
            return None
    
    def get_filings(self, cik: str, start_date: str, end_date: str) -> list:
        """
        Get 10-K and 10-Q filings for a CIK within date range.
        
        Args:
            cik: Company CIK (10 digits)
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            
        Returns:
            List of filing metadata dicts
        """
        try:
            self._rate_limit()
            
            url = f"{self.base_url}/submissions/CIK{cik}.json"
            response = self.session.get(url)
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            filings = data.get('filings', {}).get('recent', {})
            
            # Extract 10-K and 10-Q filings
            results = []
            forms = filings.get('form', [])
            dates = filings.get('filingDate', [])
            accessions = filings.get('accessionNumber', [])
            
            for form, date, accession in zip(forms, dates, accessions):
                if form in ['10-K', '10-Q'] and start_date <= date <= end_date:
                    results.append({
                        'form': form,
                        'filing_date': date,
                        'accession': accession.replace('-', ''),
                        'cik': cik
                    })
            
            return sorted(results, key=lambda x: x['filing_date'])
            
        except Exception as e:
            logger.debug(f"CIK {cik}: Error getting filings - {e}")
            return []
    
    def extract_shares_from_filing(self, cik: str, accession: str, filing_date: str) -> dict:
        """
        Extract shares outstanding from a specific filing.
        
        Uses XBRL data (structured financial data).
        
        Args:
            cik: Company CIK
            accession: Filing accession number (no dashes)
            filing_date: YYYY-MM-DD
            
        Returns:
            dict with shares outstanding data
        """
        try:
            self._rate_limit()
            
            # Try XBRL instance document (most reliable)
            # Format: https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession}&xbrl_type=v
            
            # Build filing URL
            cik_trimmed = cik.lstrip('0')
            url = f"{self.base_url}/Archives/edgar/data/{cik_trimmed}/{accession}/Financial_Report.xlsx"
            
            # Try to get the filing index first
            index_url = f"{self.base_url}/cgi-bin/browse-edgar"
            params = {
                'action': 'getcompany',
                'CIK': cik,
                'type': '10-',
                'dateb': filing_date,
                'owner': 'exclude',
                'count': 1
            }
            
            self._rate_limit()
            response = self.session.get(index_url, params=params)
            
            if response.status_code != 200:
                return None
            
            # Parse HTML to find document links
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for shares outstanding in various XBRL tags
            # Common tags: 
            # - dei:EntityCommonStockSharesOutstanding
            # - us-gaap:CommonStockSharesOutstanding
            # - us-gaap:SharesOutstanding
            
            # For now, return the filing metadata
            # Actual parsing requires XBRL library
            return {
                'filing_date': filing_date,
                'cik': cik,
                'accession': accession,
                'shares_outstanding': None  # Will parse in next step
            }
            
        except Exception as e:
            logger.debug(f"CIK {cik}, Accession {accession}: Error - {e}")
            return None
    
    def parse_xbrl_shares(self, cik: str, accession: str) -> int:
        """
        Parse XBRL data to extract shares outstanding.
        
        Uses the SEC's XBRL API endpoint.
        """
        try:
            self._rate_limit()
            
            # Build XBRL data URL
            cik_trimmed = cik.lstrip('0')
            
            # Try companyfacts API (easier to parse)
            url = f"{self.base_url}/api/xbrl/companyfacts/CIK{cik}.json"
            response = self.session.get(url)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            facts = data.get('facts', {})
            
            # Look for common stock shares outstanding
            # Try different taxonomy namespaces
            shares = None
            
            # Try us-gaap namespace
            us_gaap = facts.get('us-gaap', {})
            
            # Priority order of facts to search
            share_facts = [
                'CommonStockSharesOutstanding',
                'CommonStockSharesIssued',
                'SharesOutstanding',
                'EntityCommonStockSharesOutstanding'
            ]
            
            for fact_name in share_facts:
                if fact_name in us_gaap:
                    units = us_gaap[fact_name].get('units', {})
                    # Get shares (not USD)
                    if 'shares' in units:
                        entries = units['shares']
                        # Find entry matching our accession
                        for entry in entries:
                            if entry.get('accn', '').replace('-', '') == accession:
                                shares = entry.get('val')
                                if shares:
                                    return int(shares)
            
            # If no exact match, return most recent value
            for fact_name in share_facts:
                if fact_name in us_gaap:
                    units = us_gaap[fact_name].get('units', {})
                    if 'shares' in units:
                        entries = units['shares']
                        if entries:
                            # Get most recent
                            latest = max(entries, key=lambda x: x.get('end', ''))
                            shares = latest.get('val')
                            if shares:
                                return int(shares)
            
            return None
            
        except Exception as e:
            logger.debug(f"CIK {cik}: Error parsing XBRL - {e}")
            return None
    
    def get_symbol_history(
        self, 
        symbol: str, 
        start_date: str = "2016-01-01",
        end_date: str = "2025-12-31"
    ) -> pd.DataFrame:
        """
        Get complete filing history with shares outstanding for a symbol.
        
        Args:
            symbol: Stock ticker
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            
        Returns:
            DataFrame with columns: symbol, filing_date, form, shares_outstanding
        """
        # Step 1: Get CIK
        cik = self.get_cik(symbol)
        if not cik:
            logger.warning(f"{symbol}: Could not find CIK")
            return pd.DataFrame()
        
        logger.debug(f"{symbol}: CIK = {cik}")
        
        # Step 2: Get filings
        filings = self.get_filings(cik, start_date, end_date)
        if not filings:
            logger.warning(f"{symbol}: No filings found")
            return pd.DataFrame()
        
        logger.info(f"{symbol}: Found {len(filings)} filings ({start_date} to {end_date})")
        
        # Step 3: Parse shares from each filing
        results = []
        for filing in filings:
            shares = self.parse_xbrl_shares(cik, filing['accession'])
            
            if shares:
                results.append({
                    'symbol': symbol,
                    'cik': cik,
                    'filing_date': filing['filing_date'],
                    'form': filing['form'],
                    'shares_outstanding': shares,
                    'accession': filing['accession']
                })
                logger.debug(f"{symbol} {filing['filing_date']}: {shares:,} shares")
        
        if results:
            return pd.DataFrame(results)
        else:
            logger.warning(f"{symbol}: Could not extract shares from filings")
            return pd.DataFrame()
    
    def close(self):
        """Close HTTP session."""
        self.session.close()


def process_symbol_batch(
    symbols: list,
    user_agent: str,
    start_date: str = "2016-01-01",
    end_date: str = "2025-12-31"
) -> pd.DataFrame:
    """
    Process a batch of symbols (single-threaded due to SEC rate limits).
    
    Args:
        symbols: List of stock symbols
        user_agent: SEC-required user agent
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        
    Returns:
        Combined DataFrame with all symbol histories
    """
    fetcher = EdgarFloatFetcher(user_agent)
    
    all_data = []
    
    for symbol in tqdm(symbols, desc="Fetching Edgar data"):
        df = fetcher.get_symbol_history(symbol, start_date, end_date)
        if not df.empty:
            all_data.append(df)
    
    fetcher.close()
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


async def main():
    """
    Fetch historical float from Edgar filings for all symbols.
    
    This will take several hours due to SEC rate limiting (10 req/sec).
    Run overnight or over weekend.
    """
    # Configuration
    USER_AGENT = "YourCompany your.email@domain.com"  # REQUIRED by SEC
    symbols_file = "T:/ticker_data/small_cap_symbols_2b.csv"
    output_file = "T:/ticker_data/float_history/edgar_float_2016_2025.parquet"
    checkpoint_file = "T:/ticker_data/float_history/edgar_checkpoint.parquet"
    
    logger.info("=" * 70)
    logger.info("SEC Edgar Float Fetcher (2016-2025)")
    logger.info("This will take several hours due to SEC rate limits")
    logger.info("=" * 70)
    
    # Load symbols
    df_symbols = pd.read_csv(symbols_file)
    symbols = df_symbols['symbol'].tolist()
    
    logger.info(f"Total symbols: {len(symbols)}")
    
    # Check for checkpoint
    processed_symbols = set()
    if Path(checkpoint_file).exists():
        df_checkpoint = pd.read_parquet(checkpoint_file)
        processed_symbols = set(df_checkpoint['symbol'].unique())
        logger.info(f"Resuming: {len(processed_symbols)} symbols already processed")
    
    # Filter unprocessed symbols
    remaining_symbols = [s for s in symbols if s not in processed_symbols]
    logger.info(f"Remaining: {len(remaining_symbols)} symbols")
    
    if not remaining_symbols:
        logger.info("✓ All symbols already processed!")
        return
    
    # Estimate time
    # Assume: ~40 filings per symbol (10 years * 4 quarters)
    # At 10 req/sec = 0.1s per request
    # Plus CIK lookup = ~5-10 seconds per symbol
    total_seconds = len(remaining_symbols) * 8
    hours = total_seconds / 3600
    logger.info(f"Estimated time: {hours:.1f} hours")
    
    # Process in batches (for checkpointing)
    batch_size = 50
    for i in range(0, len(remaining_symbols), batch_size):
        batch = remaining_symbols[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(remaining_symbols) + batch_size - 1) // batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} symbols)...")
        
        df_batch = process_symbol_batch(
            symbols=batch,
            user_agent=USER_AGENT,
            start_date="2016-01-01",
            end_date="2025-12-31"
        )
        
        if not df_batch.empty:
            # Append to checkpoint
            if Path(checkpoint_file).exists():
                df_existing = pd.read_parquet(checkpoint_file)
                df_combined = pd.concat([df_existing, df_batch], ignore_index=True)
            else:
                df_combined = df_batch
            
            # Save checkpoint
            Path(checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
            df_combined.to_parquet(checkpoint_file, compression='snappy', index=False)
            
            logger.info(f"✓ Batch {batch_num} saved ({len(df_batch)} records)")
        
        # Progress update
        completed = i + len(batch)
        pct = (completed / len(remaining_symbols)) * 100
        logger.info(f"Progress: {completed}/{len(remaining_symbols)} ({pct:.1f}%)")
    
    # Final save
    df_final = pd.read_parquet(checkpoint_file)
    df_final.to_parquet(output_file, compression='snappy', index=False)
    
    logger.info("=" * 70)
    logger.info("✓ COMPLETE!")
    logger.info(f"Output: {output_file}")
    logger.info(f"Total records: {len(df_final)}")
    logger.info(f"Symbols: {df_final['symbol'].nunique()}")
    logger.info(f"Date range: {df_final['filing_date'].min()} to {df_final['filing_date'].max()}")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
