"""
Analyze small cap coverage across US exchanges using yfinance.
Cross-references against Alpaca to find tradable small caps.
"""
import asyncio
import pandas as pd
import yfinance as yf
from pathlib import Path
import logging
from datetime import datetime
from typing import Dict, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from utils.logger import setup_logger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class SmallCapAnalyzer:
    """Analyze small cap stock coverage using yfinance + Alpaca validation."""
    
    # Market cap thresholds
    SMALL_CAP_MIN = 300_000_000   # $300M
    SMALL_CAP_MAX = 2_000_000_000  # $2B
    
    # Exchange name mappings (yfinance uses different names)
    EXCHANGE_MAPPINGS = {
        'NMS': 'NASDAQ',
        'NGM': 'NASDAQ',
        'NCM': 'NASDAQ',
        'NAS': 'NASDAQ',
        'NASDAQ': 'NASDAQ',
        'NYQ': 'NYSE',
        'NYSE': 'NYSE',
        'NYE': 'NYSE',
        'NYSEAMERICAN': 'AMEX',
        'AMEX': 'AMEX',
        'ASE': 'AMEX',
        'NYSEAMER': 'AMEX'
    }
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.alpaca = AlpacaAPI(config, logger)
        
        self.results = {
            'by_exchange': {
                'NASDAQ': {'total': 0, 'MICRO': 0, 'SMALL': 0, 'MID': 0, 'LARGE': 0, 'UNKNOWN': 0, 'small_cap_symbols': [], 'alpaca_tradable_small': []},
                'NYSE': {'total': 0, 'MICRO': 0, 'SMALL': 0, 'MID': 0, 'LARGE': 0, 'UNKNOWN': 0, 'small_cap_symbols': [], 'alpaca_tradable_small': []},
                'AMEX': {'total': 0, 'MICRO': 0, 'SMALL': 0, 'MID': 0, 'LARGE': 0, 'UNKNOWN': 0, 'small_cap_symbols': [], 'alpaca_tradable_small': []},
                'OTHER': {'total': 0, 'MICRO': 0, 'SMALL': 0, 'MID': 0, 'LARGE': 0, 'UNKNOWN': 0, 'small_cap_symbols': [], 'alpaca_tradable_small': []}
            },
            'small_caps': [],
            'failed_lookups': [],
            'exchange_labels_found': {},
            'alpaca_validation': {
                'tradable': [],
                'not_tradable': [],
                'not_found': []
            }
        }
    
    def get_ticker_lists(self) -> Dict[str, list]:
        """Download ticker lists from NASDAQ FTP."""
        self.logger.info("Downloading ticker lists from NASDAQ FTP...")
        
        tickers = {
            'NASDAQ': [],
            'NYSE': [],
            'AMEX': []
        }
        
        try:
            # NASDAQ listed stocks
            self.logger.info("Fetching NASDAQ listed stocks...")
            df_nasdaq = pd.read_csv(
                'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt',
                sep='|'
            )
            df_nasdaq = df_nasdaq[df_nasdaq['Symbol'].notna()]
            df_nasdaq = df_nasdaq[df_nasdaq['Test Issue'] == 'N']
            # Remove footer row
            df_nasdaq = df_nasdaq[df_nasdaq['Symbol'] != 'Symbol']
            tickers['NASDAQ'] = df_nasdaq['Symbol'].str.strip().tolist()
            self.logger.info(f"✓ NASDAQ: {len(tickers['NASDAQ'])} symbols")
            
            # Other listed (NYSE + AMEX)
            self.logger.info("Fetching NYSE and AMEX listed stocks...")
            df_other = pd.read_csv(
                'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt',
                sep='|'
            )
            df_other = df_other[df_other['ACT Symbol'].notna()]
            df_other = df_other[df_other['Test Issue'] == 'N']
            df_other = df_other[df_other['ACT Symbol'] != 'ACT Symbol']  # Remove footer
            
            # Exchange codes: N=NYSE, A=AMEX
            tickers['NYSE'] = df_other[df_other['Exchange'] == 'N']['ACT Symbol'].str.strip().tolist()
            tickers['AMEX'] = df_other[df_other['Exchange'] == 'A']['ACT Symbol'].str.strip().tolist()
            
            self.logger.info(f"✓ NYSE: {len(tickers['NYSE'])} symbols")
            self.logger.info(f"✓ AMEX: {len(tickers['AMEX'])} symbols")
            
        except Exception as e:
            self.logger.error(f"Error downloading ticker lists: {e}", exc_info=True)
            return {}
        
        return tickers
    
    def normalize_exchange(self, yf_exchange: str) -> str:
        """Map yfinance exchange name to standard name."""
        if not yf_exchange:
            return 'OTHER'
        
        yf_exchange = yf_exchange.upper().strip()
        return self.EXCHANGE_MAPPINGS.get(yf_exchange, 'OTHER')
    
    def get_market_cap(self, symbol: str) -> Tuple[float, str]:
        """Get market cap using yfinance."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            market_cap = info.get('marketCap') or info.get('market_cap')
            exchange = info.get('exchange', '')
            
            return market_cap, exchange
            
        except Exception as e:
            self.logger.debug(f"{symbol}: Error - {e}")
            return None, None
    
    def classify_market_cap(self, market_cap: float) -> str:
        """Classify market cap."""
        if market_cap is None:
            return "UNKNOWN"
        elif market_cap < self.SMALL_CAP_MIN:
            return "MICRO"
        elif market_cap <= self.SMALL_CAP_MAX:
            return "SMALL"
        elif market_cap <= 10_000_000_000:
            return "MID"
        else:
            return "LARGE"
    
    def analyze_symbols(self, exchange_name: str, symbols: list):
        """Analyze symbols for a given exchange using yfinance."""
        self.logger.info(f"\nAnalyzing {exchange_name}: {len(symbols)} symbols...")
        
        analyzed = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_symbol = {
                executor.submit(self.get_market_cap, symbol): symbol 
                for symbol in symbols
            }
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                market_cap, yf_exchange = future.result()
                
                analyzed += 1
                
                # Track what yfinance returns for exchange labels
                if yf_exchange:
                    yf_exchange_clean = yf_exchange.upper().strip()
                    self.results['exchange_labels_found'][yf_exchange_clean] = \
                        self.results['exchange_labels_found'].get(yf_exchange_clean, 0) + 1
                
                # Normalize exchange
                normalized_exchange = self.normalize_exchange(yf_exchange)
                
                # Use yfinance exchange if available, otherwise use source exchange
                if normalized_exchange in ['NASDAQ', 'NYSE', 'AMEX']:
                    target_exchange = normalized_exchange
                else:
                    target_exchange = exchange_name
                
                # Update counts
                self.results['by_exchange'][target_exchange]['total'] += 1
                
                if market_cap is None:
                    self.results['failed_lookups'].append({
                        'symbol': symbol,
                        'listed_exchange': exchange_name,
                        'yf_exchange': yf_exchange
                    })
                    self.results['by_exchange'][target_exchange]['UNKNOWN'] += 1
                else:
                    cap_class = self.classify_market_cap(market_cap)
                    self.results['by_exchange'][target_exchange][cap_class] += 1
                    
                    if cap_class == 'SMALL':
                        self.results['small_caps'].append({
                            'symbol': symbol,
                            'market_cap': market_cap,
                            'listed_exchange': exchange_name,
                            'yf_exchange': yf_exchange,
                            'normalized_exchange': target_exchange
                        })
                        self.results['by_exchange'][target_exchange]['small_cap_symbols'].append(symbol)
                
                # Progress
                if analyzed % 100 == 0:
                    pct = (analyzed / len(symbols)) * 100
                    self.logger.info(f"  Progress: {analyzed}/{len(symbols)} ({pct:.1f}%)")
        
        self.logger.info(f"✓ {exchange_name} complete: {self.results['by_exchange'][exchange_name]['SMALL']} small caps")
    
    async def validate_against_alpaca(self):
        """
        Check which small caps are tradable on Alpaca.
        Validates: asset_class='us_equity', status='active', tradable=True
        """
        small_cap_symbols = [sc['symbol'] for sc in self.results['small_caps']]
        
        if not small_cap_symbols:
            self.logger.warning("No small caps to validate")
            return
        
        self.logger.info(f"\nValidating {len(small_cap_symbols)} small caps against Alpaca...")
        
        # Batch check
        batch_size = 100
        checked = 0
        
        for i in range(0, len(small_cap_symbols), batch_size):
            batch = small_cap_symbols[i:i+batch_size]
            
            # Check each symbol
            for symbol in batch:
                try:
                    url = f"{self.config.ALPACA_BASE_URL}/v2/assets/{symbol}"
                    response = await self.alpaca.session.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Check Alpaca trading criteria
                        is_tradable = (
                            data.get('class') == 'us_equity' and
                            data.get('status') == 'active' and
                            data.get('tradable') is True
                        )
                        
                        alpaca_info = {
                            'symbol': symbol,
                            'asset_class': data.get('class'),
                            'status': data.get('status'),
                            'tradable': data.get('tradable'),
                            'exchange': data.get('exchange'),
                            'meets_criteria': is_tradable
                        }
                        
                        if is_tradable:
                            self.results['alpaca_validation']['tradable'].append(alpaca_info)
                            
                            # Update exchange tracking
                            for sc in self.results['small_caps']:
                                if sc['symbol'] == symbol:
                                    exchange = sc['normalized_exchange']
                                    self.results['by_exchange'][exchange]['alpaca_tradable_small'].append(symbol)
                                    break
                        else:
                            self.results['alpaca_validation']['not_tradable'].append(alpaca_info)
                    else:
                        self.results['alpaca_validation']['not_found'].append(symbol)
                
                except Exception as e:
                    self.logger.debug(f"{symbol}: Alpaca check error - {e}")
                    self.results['alpaca_validation']['not_found'].append(symbol)
                
                checked += 1
            
            # Progress
            pct = (checked / len(small_cap_symbols)) * 100
            self.logger.info(f"  Alpaca validation: {checked}/{len(small_cap_symbols)} ({pct:.1f}%)")
            
            # Rate limiting
            await asyncio.sleep(0.5)
        
        tradable_count = len(self.results['alpaca_validation']['tradable'])
        self.logger.info(f"✓ Alpaca validation complete: {tradable_count}/{len(small_cap_symbols)} tradable")
    
    def generate_report(self) -> str:
        """Generate comprehensive report."""
        report = []
        report.append("=" * 80)
        report.append("SMALL CAP ANALYSIS - YFINANCE + ALPACA VALIDATION")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Small Cap: ${self.SMALL_CAP_MIN:,} - ${self.SMALL_CAP_MAX:,}")
        report.append("=" * 80)
        report.append("")
        
        # Main summary with Alpaca comparison
        report.append("📊 SMALL CAP COUNT BY EXCHANGE (with Alpaca validation)")
        report.append("-" * 80)
        report.append(f"{'Exchange':<10} | {'Total':<7} | {'Small':<7} | {'%':<7} | {'Alpaca':<7} | {'Coverage':<9} | {'Expected':<12} | {'Status':<6}")
        report.append("-" * 80)
        
        expected_ranges = {
            'NASDAQ': (15, 25),
            'NYSE': (10, 18),
            'AMEX': (30, 50)
        }
        
        total_symbols = 0
        total_small_caps = 0
        total_alpaca_tradable = 0
        
        for exchange in ['NASDAQ', 'NYSE', 'AMEX']:
            data = self.results['by_exchange'][exchange]
            small_count = data['SMALL']
            total = data['total']
            alpaca_count = len(data['alpaca_tradable_small'])
            
            pct = (small_count / total * 100) if total > 0 else 0
            alpaca_coverage = (alpaca_count / small_count * 100) if small_count > 0 else 0
            
            expected_min, expected_max = expected_ranges.get(exchange, (0, 0))
            
            # Status check
            if pct < 5 and exchange == 'AMEX':
                status = "❌"
            elif pct >= expected_min and pct <= expected_max:
                status = "✅"
            else:
                status = "⚠️"
            
            report.append(
                f"{exchange:<10} | {total:<7} | {small_count:<7} | {pct:>5.1f}% | "
                f"{alpaca_count:<7} | {alpaca_coverage:>7.1f}% | "
                f"{expected_min}-{expected_max}%{'':<7} | {status}"
            )
            
            total_symbols += total
            total_small_caps += small_count
            total_alpaca_tradable += alpaca_count
        
        report.append("-" * 80)
        overall_pct = (total_small_caps / total_symbols * 100) if total_symbols > 0 else 0
        overall_alpaca = (total_alpaca_tradable / total_small_caps * 100) if total_small_caps > 0 else 0
        
        report.append(
            f"{'TOTAL':<10} | {total_symbols:<7} | {total_small_caps:<7} | {overall_pct:>5.1f}% | "
            f"{total_alpaca_tradable:<7} | {overall_alpaca:>7.1f}% | {'15-20%':<11} |"
        )
        report.append("")
        
        # Detailed breakdown
        report.append("📈 DETAILED BREAKDOWN BY EXCHANGE")
        report.append("-" * 80)
        for exchange in ['NASDAQ', 'NYSE', 'AMEX']:
            data = self.results['by_exchange'][exchange]
            total = data['total']
            
            if total == 0:
                continue
            
            alpaca_tradable = len(data['alpaca_tradable_small'])
            small_total = data['SMALL']
            
            report.append(f"\n{exchange}:")
            report.append(f"  Total symbols:          {total}")
            report.append(f"  Micro cap (<$300M):     {data['MICRO']:>5} ({data['MICRO']/total*100:>5.1f}%)")
            report.append(f"  Small cap ($300M-$2B):  {data['SMALL']:>5} ({data['SMALL']/total*100:>5.1f}%) ⭐")
            report.append(f"  Mid cap ($2B-$10B):     {data['MID']:>5} ({data['MID']/total*100:>5.1f}%)")
            report.append(f"  Large cap (>$10B):      {data['LARGE']:>5} ({data['LARGE']/total*100:>5.1f}%)")
            report.append(f"  No data:                {data['UNKNOWN']:>5} ({data['UNKNOWN']/total*100:>5.1f}%)")
            
            if small_total > 0:
                report.append(f"  ├─ Alpaca tradable:     {alpaca_tradable:>5} ({alpaca_tradable/small_total*100:>5.1f}% of small caps)")
                report.append(f"  └─ NOT on Alpaca:       {small_total - alpaca_tradable:>5} ({(small_total - alpaca_tradable)/small_total*100:>5.1f}%)")
        
        # Alpaca validation summary
        if self.results['small_caps']:
            report.append("\n🔍 ALPACA VALIDATION RESULTS")
            report.append("-" * 80)
            total_validated = len(self.results['alpaca_validation']['tradable']) + \
                            len(self.results['alpaca_validation']['not_tradable']) + \
                            len(self.results['alpaca_validation']['not_found'])
            
            report.append(f"Small caps checked:        {total_validated}")
            report.append(f"✅ Tradable on Alpaca:     {len(self.results['alpaca_validation']['tradable'])} "
                        f"({len(self.results['alpaca_validation']['tradable'])/total_validated*100:.1f}%)")
            report.append(f"⚠️  In Alpaca but locked:   {len(self.results['alpaca_validation']['not_tradable'])} "
                        f"(status/tradable flag)")
            report.append(f"❌ Not found in Alpaca:    {len(self.results['alpaca_validation']['not_found'])}")
            report.append("")
        
        # Show exchange label mapping (for debugging AMEX issue)
        report.append("🔍 YFINANCE EXCHANGE LABELS FOUND (for debugging)")
        report.append("-" * 80)
        for yf_label, count in sorted(self.results['exchange_labels_found'].items(), key=lambda x: x[1], reverse=True)[:15]:
            normalized = self.normalize_exchange(yf_label)
            report.append(f"  {yf_label:<20} -> {normalized:<10} ({count} symbols)")
        
        if len(self.results['exchange_labels_found']) > 15:
            report.append(f"  ... and {len(self.results['exchange_labels_found']) - 15} more labels")
        
        report.append("")
        
        # Diagnostics
        report.append("💡 DIAGNOSTICS")
        report.append("-" * 80)
        
        amex_small = self.results['by_exchange']['AMEX']['SMALL']
        if amex_small == 0:
            report.append("❌ AMEX ISSUE DETECTED:")
            report.append("   - Expected: 30-50% small cap (~90-150 symbols)")
            report.append("   - Found: 0 symbols")
            report.append("   - Possible causes:")
            report.append("     1. yfinance labels AMEX as 'NYSEAMERICAN' or other variant")
            report.append("     2. AMEX stocks migrated to NYSE in yfinance data")
            report.append("     3. Market cap data missing for AMEX symbols")
            report.append("   - Check the exchange labels above for AMEX-related names")
            report.append("")
        
        report.append(f"✓ Found {len(self.results['small_caps'])} total small caps")
        report.append(f"✓ {total_alpaca_tradable} ({overall_alpaca:.1f}%) are tradable on Alpaca")
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def export_results(self, output_dir: Path):
        """Export detailed results."""
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # All small caps with Alpaca status
        if self.results['small_caps']:
            df = pd.DataFrame(self.results['small_caps'])
            
            # Add Alpaca tradable flag
            tradable_symbols = {item['symbol'] for item in self.results['alpaca_validation']['tradable']}
            df['alpaca_tradable'] = df['symbol'].isin(tradable_symbols)
            
            output_file = output_dir / f"small_caps_all_{timestamp}.csv"
            df.to_csv(output_file, index=False)
            self.logger.info(f"✓ Exported {len(df)} small caps to {output_file.name}")
        
        # Alpaca tradable small caps only
        if self.results['alpaca_validation']['tradable']:
            df_tradable = pd.DataFrame(self.results['alpaca_validation']['tradable'])
            
            # Merge with market cap data
            small_caps_df = pd.DataFrame(self.results['small_caps'])
            df_tradable = df_tradable.merge(
                small_caps_df[['symbol', 'market_cap', 'normalized_exchange']],
                on='symbol',
                how='left'
            )
            
            output_file = output_dir / f"small_caps_alpaca_tradable_{timestamp}.csv"
            df_tradable.to_csv(output_file, index=False)
            self.logger.info(f"✓ Exported {len(df_tradable)} Alpaca-tradable small caps")
        
        # Exchange summary
        summary = []
        for exchange, data in self.results['by_exchange'].items():
            if data['total'] > 0:
                summary.append({
                    'exchange': exchange,
                    'total': data['total'],
                    'micro': data['MICRO'],
                    'small': data['SMALL'],
                    'mid': data['MID'],
                    'large': data['LARGE'],
                    'unknown': data['UNKNOWN'],
                    'small_cap_pct': (data['SMALL'] / data['total'] * 100),
                    'alpaca_tradable_small': len(data['alpaca_tradable_small']),
                    'alpaca_coverage_pct': (len(data['alpaca_tradable_small']) / data['SMALL'] * 100) if data['SMALL'] > 0 else 0
                })
        
        df_summary = pd.DataFrame(summary)
        summary_file = output_dir / f"exchange_summary_{timestamp}.csv"
        df_summary.to_csv(summary_file, index=False)
        self.logger.info(f"✓ Exported exchange summary")
        
        # Exchange label mapping (debugging)
        df_labels = pd.DataFrame([
            {'yfinance_label': k, 'count': v, 'normalized_to': self.normalize_exchange(k)}
            for k, v in self.results['exchange_labels_found'].items()
        ]).sort_values('count', ascending=False)
        
        label_file = output_dir / f"yfinance_labels_{timestamp}.csv"
        df_labels.to_csv(label_file, index=False)
        self.logger.info(f"✓ Exported yfinance label mappings")
    
    async def close(self):
        """Close connections."""
        await self.alpaca.close()


async def main():
    """Run analysis."""
    config = Config()
    
    log_dir = Path(config.TEMP_DIR) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"smallcap_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logger = setup_logger("smallcap_analyzer", str(log_file))
    
    logger.info("=" * 80)
    logger.info("SMALL CAP ANALYZER - YFINANCE + ALPACA VALIDATION")
    logger.info("=" * 80)
    
    analyzer = SmallCapAnalyzer(config, logger)
    
    try:
        # Step 1: Get ticker lists from NASDAQ FTP
        tickers = analyzer.get_ticker_lists()
        
        if not tickers:
            logger.error("❌ Failed to retrieve ticker lists")
            return
        
        total = sum(len(symbols) for symbols in tickers.values())
        logger.info(f"\nTotal symbols to analyze: {total}")
        
        # Step 2: Analyze with yfinance
        for exchange, symbols in tickers.items():
            if symbols:
                analyzer.analyze_symbols(exchange, symbols)
        
        # Step 3: Validate small caps against Alpaca
        if await analyzer.alpaca.check_terminal_running():
            await analyzer.validate_against_alpaca()
        else:
            logger.warning("⚠️ Alpaca API not available - skipping validation")
        
        # Step 4: Generate report
        report = analyzer.generate_report()
        print("\n" + report)
        
        # Step 5: Export
        output_dir = Path(config.TEMP_DIR) / "smallcap_analysis"
        analyzer.export_results(output_dir)
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ ANALYSIS COMPLETE")
        logger.info(f"Results saved to: {output_dir}")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        raise
    finally:
        await analyzer.close()


if __name__ == "__main__":
    asyncio.run(main())
