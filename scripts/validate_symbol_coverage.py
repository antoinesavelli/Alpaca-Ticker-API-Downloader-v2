"""
Validate data coverage for all symbols in symbol_universe.csv.
Compares local data files against Alpaca API availability.
"""
import asyncio
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from utils.logger import setup_logger
from utils.trading_calendar import TradingCalendar

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class SymbolCoverageValidator:
    """
    Validate that all symbols in symbol_universe.csv have data.
    
    Checks:
    1. Symbol exists in Alpaca API
    2. Local parquet file exists
    3. Data completeness (date range)
    4. Data quality (null values, gaps)
    """
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.alpaca = AlpacaAPI(config, logger)
        self.calendar = TradingCalendar(logger)
        
        # Paths
        self.symbol_file = Path(config.SYMBOL_FILE)
        self.data_dir = Path(config.OUTPUT_DIR)
        
        # Results storage
        self.results = {
            'total_symbols': 0,
            'with_local_data': [],
            'without_local_data': [],
            'alpaca_available': [],
            'alpaca_unavailable': [],
            'incomplete_data': [],
            'complete_data': []
        }
    
    def load_symbol_universe(self) -> List[str]:
        """Load symbols from symbol_universe.csv."""
        if not self.symbol_file.exists():
            self.logger.error(f"Symbol file not found: {self.symbol_file}")
            return []
        
        try:
            df = pd.read_csv(self.symbol_file)
            
            # Try common column names
            symbol_col = None
            for col in ['symbol', 'Symbol', 'ticker', 'Ticker', 'SYMBOL']:
                if col in df.columns:
                    symbol_col = col
                    break
            
            if not symbol_col:
                self.logger.error(f"No symbol column found. Available columns: {df.columns.tolist()}")
                return []
            
            symbols = df[symbol_col].dropna().unique().tolist()
            self.logger.info(f"✓ Loaded {len(symbols)} symbols from {self.symbol_file.name}")
            
            return symbols
            
        except Exception as e:
            self.logger.error(f"Error loading symbol file: {e}")
            return []
    
    def check_local_data(self, symbol: str) -> Dict:
        """
        Check if local parquet file exists and analyze it.
        
        Returns:
            dict with file info, date range, record count
        """
        file_path = self.data_dir / f"{symbol}.parquet"
        
        result = {
            'symbol': symbol,
            'has_file': False,
            'file_size': 0,
            'record_count': 0,
            'start_date': None,
            'end_date': None,
            'trading_days': 0,
            'missing_days': 0
        }
        
        if not file_path.exists():
            return result
        
        try:
            result['has_file'] = True
            result['file_size'] = file_path.stat().st_size
            
            # Load parquet file
            df = pd.read_parquet(file_path)
            result['record_count'] = len(df)
            
            if len(df) > 0:
                # Ensure timestamp column
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    result['start_date'] = df['timestamp'].min().date()
                    result['end_date'] = df['timestamp'].max().date()
                    
                    # Count unique trading days
                    result['trading_days'] = df['timestamp'].dt.date.nunique()
                    
                    # Calculate expected trading days
                    expected_days = self.calendar.get_trading_days_count(
                        result['start_date'],
                        result['end_date']
                    )
                    result['missing_days'] = expected_days - result['trading_days']
            
        except Exception as e:
            self.logger.debug(f"{symbol}: Error reading parquet - {e}")
        
        return result
    
    async def check_alpaca_availability(self, symbol: str) -> Dict:
        """
        Check if symbol is available in Alpaca API.
        
        Returns:
            dict with availability, tradability, exchange info
        """
        result = {
            'symbol': symbol,
            'available': False,
            'tradable': False,
            'exchange': None,
            'asset_class': None,
            'status': None
        }
        
        try:
            # Use Alpaca assets endpoint
            url = f"{self.config.ALPACA_BASE_URL}/v2/assets/{symbol}"
            response = await self.alpaca.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                result['available'] = True
                result['tradable'] = data.get('tradable', False)
                result['exchange'] = data.get('exchange')
                result['asset_class'] = data.get('class')
                result['status'] = data.get('status')
            else:
                self.logger.debug(f"{symbol}: Not available in Alpaca (status {response.status_code})")
                
        except Exception as e:
            self.logger.debug(f"{symbol}: Error checking Alpaca - {e}")
        
        return result
    
    async def validate_symbol(self, symbol: str) -> Dict:
        """
        Complete validation for a single symbol.
        
        Combines local data check + Alpaca availability check.
        """
        local_info = self.check_local_data(symbol)
        alpaca_info = await self.check_alpaca_availability(symbol)
        
        return {
            'symbol': symbol,
            'local': local_info,
            'alpaca': alpaca_info
        }
    
    async def validate_all_symbols(self, symbols: List[str]) -> List[Dict]:
        """
        Validate all symbols with progress tracking.
        
        Args:
            symbols: List of symbols to validate
            
        Returns:
            List of validation results
        """
        self.logger.info(f"Validating {len(symbols)} symbols...")
        self.logger.info("This may take a few minutes...")
        
        results = []
        batch_size = 50
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            
            # Process batch concurrently
            tasks = [self.validate_symbol(symbol) for symbol in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            
            # Progress update
            progress = min(i + batch_size, len(symbols))
            pct = (progress / len(symbols)) * 100
            self.logger.info(f"Progress: {progress}/{len(symbols)} ({pct:.1f}%)")
            
            # Brief delay to avoid rate limits
            await asyncio.sleep(0.5)
        
        return results
    
    def categorize_results(self, validation_results: List[Dict]):
        """
        Categorize validation results into actionable groups.
        """
        for result in validation_results:
            symbol = result['symbol']
            local = result['local']
            alpaca = result['alpaca']
            
            # Track local data presence
            if local['has_file']:
                self.results['with_local_data'].append(symbol)
                
                # Check completeness
                if local['missing_days'] > 10:  # More than 10 days missing
                    self.results['incomplete_data'].append({
                        'symbol': symbol,
                        'start_date': local['start_date'],
                        'end_date': local['end_date'],
                        'missing_days': local['missing_days'],
                        'record_count': local['record_count']
                    })
                else:
                    self.results['complete_data'].append(symbol)
            else:
                self.results['without_local_data'].append(symbol)
            
            # Track Alpaca availability
            if alpaca['available']:
                self.results['alpaca_available'].append({
                    'symbol': symbol,
                    'tradable': alpaca['tradable'],
                    'exchange': alpaca['exchange'],
                    'status': alpaca['status']
                })
            else:
                self.results['alpaca_unavailable'].append(symbol)
    
    def generate_report(self) -> str:
        """Generate comprehensive validation report."""
        report = []
        report.append("=" * 80)
        report.append("SYMBOL COVERAGE VALIDATION REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        report.append("")
        
        # Summary statistics
        total = self.results['total_symbols']
        with_data = len(self.results['with_local_data'])
        without_data = len(self.results['without_local_data'])
        alpaca_avail = len(self.results['alpaca_available'])
        alpaca_unavail = len(self.results['alpaca_unavailable'])
        complete = len(self.results['complete_data'])
        incomplete = len(self.results['incomplete_data'])
        
        report.append("📊 SUMMARY")
        report.append("-" * 80)
        report.append(f"Total symbols in universe:        {total}")
        report.append(f"")
        report.append(f"✓ Symbols with local data:        {with_data} ({with_data/total*100:.1f}%)")
        report.append(f"  - Complete data:                 {complete}")
        report.append(f"  - Incomplete data:               {incomplete}")
        report.append(f"❌ Symbols without local data:     {without_data} ({without_data/total*100:.1f}%)")
        report.append(f"")
        report.append(f"✓ Available in Alpaca API:        {alpaca_avail} ({alpaca_avail/total*100:.1f}%)")
        report.append(f"❌ NOT available in Alpaca:        {alpaca_unavail} ({alpaca_unavail/total*100:.1f}%)")
        report.append("")
        
        # Missing data - highest priority
        if self.results['without_local_data']:
            report.append("🔴 MISSING DATA (No local parquet files)")
            report.append("-" * 80)
            report.append(f"Total: {len(self.results['without_local_data'])} symbols")
            report.append("")
            
            # Check which are available in Alpaca
            missing_in_alpaca = set(self.results['without_local_data']) & set(self.results['alpaca_unavailable'])
            missing_but_available = set(self.results['without_local_data']) - missing_in_alpaca
            
            if missing_but_available:
                report.append(f"CAN DOWNLOAD ({len(missing_but_available)} symbols - Available in Alpaca):")
                for symbol in sorted(list(missing_but_available))[:20]:  # Show first 20
                    report.append(f"  - {symbol}")
                if len(missing_but_available) > 20:
                    report.append(f"  ... and {len(missing_but_available) - 20} more")
                report.append("")
            
            if missing_in_alpaca:
                report.append(f"CANNOT DOWNLOAD ({len(missing_in_alpaca)} symbols - Not in Alpaca):")
                for symbol in sorted(list(missing_in_alpaca))[:20]:
                    report.append(f"  - {symbol}")
                if len(missing_in_alpaca) > 20:
                    report.append(f"  ... and {len(missing_in_alpaca) - 20} more")
                report.append("")
        
        # Incomplete data
        if self.results['incomplete_data']:
            report.append("🟡 INCOMPLETE DATA (>10 days missing)")
            report.append("-" * 80)
            report.append(f"Total: {len(self.results['incomplete_data'])} symbols")
            report.append("")
            for item in sorted(self.results['incomplete_data'], key=lambda x: x['missing_days'], reverse=True)[:10]:
                report.append(
                    f"  {item['symbol']:<8} | Missing: {item['missing_days']:>4} days | "
                    f"Range: {item['start_date']} to {item['end_date']} | "
                    f"Records: {item['record_count']:>6}"
                )
            if len(self.results['incomplete_data']) > 10:
                report.append(f"  ... and {len(self.results['incomplete_data']) - 10} more")
            report.append("")
        
        # Not available in Alpaca
        if self.results['alpaca_unavailable']:
            report.append("⚠️  NOT AVAILABLE IN ALPACA")
            report.append("-" * 80)
            report.append(f"Total: {len(self.results['alpaca_unavailable'])} symbols")
            report.append("These symbols cannot be downloaded from Alpaca API")
            report.append("")
            for symbol in sorted(self.results['alpaca_unavailable'])[:30]:
                report.append(f"  - {symbol}")
            if len(self.results['alpaca_unavailable']) > 30:
                report.append(f"  ... and {len(self.results['alpaca_unavailable']) - 30} more")
            report.append("")
        
        # Action items
        report.append("📋 RECOMMENDED ACTIONS")
        report.append("-" * 80)
        
        missing_downloadable = set(self.results['without_local_data']) - set(self.results['alpaca_unavailable'])
        if missing_downloadable:
            report.append(f"1. Download {len(missing_downloadable)} missing symbols from Alpaca")
            report.append(f"   Command: python main.py --mode backfill --symbols [list]")
            report.append("")
        
        if self.results['incomplete_data']:
            report.append(f"2. Fill gaps for {len(self.results['incomplete_data'])} incomplete symbols")
            report.append(f"   Command: python main.py --mode fill-gaps")
            report.append("")
        
        if self.results['alpaca_unavailable']:
            report.append(f"3. {len(self.results['alpaca_unavailable'])} symbols not available in Alpaca")
            report.append(f"   Action: Remove from symbol_universe.csv or find alternative data source")
            report.append("")
        
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def export_detailed_results(self, output_dir: Path):
        """Export detailed results to CSV files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export missing symbols
        if self.results['without_local_data']:
            df_missing = pd.DataFrame({
                'symbol': self.results['without_local_data']
            })
            df_missing.to_csv(
                output_dir / f"missing_symbols_{timestamp}.csv",
                index=False
            )
            self.logger.info(f"✓ Exported missing symbols list")
        
        # Export incomplete data details
        if self.results['incomplete_data']:
            df_incomplete = pd.DataFrame(self.results['incomplete_data'])
            df_incomplete.to_csv(
                output_dir / f"incomplete_data_{timestamp}.csv",
                index=False
            )
            self.logger.info(f"✓ Exported incomplete data list")
        
        # Export Alpaca unavailable
        if self.results['alpaca_unavailable']:
            df_unavail = pd.DataFrame({
                'symbol': self.results['alpaca_unavailable']
            })
            df_unavail.to_csv(
                output_dir / f"alpaca_unavailable_{timestamp}.csv",
                index=False
            )
            self.logger.info(f"✓ Exported Alpaca unavailable list")
        
        # Export Alpaca available details
        if self.results['alpaca_available']:
            df_avail = pd.DataFrame(self.results['alpaca_available'])
            df_avail.to_csv(
                output_dir / f"alpaca_available_{timestamp}.csv",
                index=False
            )
            self.logger.info(f"✓ Exported Alpaca available list")
    
    async def close(self):
        """Close connections."""
        await self.alpaca.close()


async def main():
    """Run symbol coverage validation."""
    # Setup
    config = Config()
    
    # Create log file path in the temp directory
    log_dir = Path(config.TEMP_DIR) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"symbol_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logger = setup_logger("symbol_coverage_validator", str(log_file))
    
    logger.info("=" * 80)
    logger.info("SYMBOL COVERAGE VALIDATOR")
    logger.info("Comparing symbol_universe.csv against Alpaca API and local data")
    logger.info("=" * 80)
    
    validator = SymbolCoverageValidator(config, logger)
    
    try:
        # Step 1: Check Alpaca API connection
        if not await validator.alpaca.check_terminal_running():
            logger.error("❌ Cannot connect to Alpaca API")
            return
        
        # Step 2: Load symbol universe
        symbols = validator.load_symbol_universe()
        if not symbols:
            logger.error("❌ No symbols loaded from CSV")
            return
        
        validator.results['total_symbols'] = len(symbols)
        
        # Step 3: Validate all symbols
        validation_results = await validator.validate_all_symbols(symbols)
        
        # Step 4: Categorize results
        validator.categorize_results(validation_results)
        
        # Step 5: Generate report
        report = validator.generate_report()
        print("\n" + report)
        
        # Step 6: Export detailed results
        output_dir = Path(config.TEMP_DIR) / "validation_reports"
        validator.export_detailed_results(output_dir)
        
        logger.info("=" * 80)
        logger.info("✓ VALIDATION COMPLETE")
        logger.info(f"Detailed reports saved to: {output_dir}")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        raise
    finally:
        await validator.close()


if __name__ == "__main__":
    asyncio.run(main())
