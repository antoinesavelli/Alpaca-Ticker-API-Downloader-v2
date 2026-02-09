"""
Bulk downloader - maximizes API efficiency with multi-symbol, multi-day requests.
Per Alpaca docs: https://docs.alpaca.markets/reference/stockbars
Supports comma-separated symbol lists for maximum efficiency.
"""
import pandas as pd
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
import time
import concurrent.futures

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from processors.data_parser import DataParser
from storage.master_file_writer import MasterFileWriter
from storage.file_splitter import FileSplitter
from storage.state_manager import StateManager
from utils.trading_calendar import TradingCalendar


class BulkDownloader:
    """
    Optimized bulk downloader for Alpaca API.
    
    Strategy:
    1. Batch symbols (50 at a time) - uses comma-separated API parameter
    2. Request large time ranges (1 month at a time)
    3. Stream writes to master file (low memory usage)
    4. Track progress with state file (fully resumable)
    5. Split master into daily files when complete
    6. Dynamic rate limiting (speed up if successful, slow down if errors)
    """
    
    def __init__(self, config: Config, api: AlpacaAPI, parser: DataParser, logger: logging.Logger):
        """Initialize bulk downloader."""
        self.config = config
        self.api = api
        self.parser = parser
        self.logger = logger
        
        self.calendar = TradingCalendar(logger=logger)
        # Pass BOTH output_dir and temp_dir to state manager for complete filesystem sync
        self.state = StateManager(
            config.STATE_FILE, 
            logger, 
            output_dir=config.OUTPUT_DIR,
            temp_dir=config.TEMP_DIR  # NEW: Pass temp_dir
        )
        self.splitter = FileSplitter(config, logger)
        
        # Dynamic rate limiting
        self.current_rate_delay = config.RATE_LIMIT_DELAY
        self.consecutive_successes = 0
        self.consecutive_failures = 0
    
    def _normalize_symbols(self, symbols: List[str]) -> List[str]:
        """
        Normalize symbols by removing warrant/unit suffixes.
        Handles special characters properly for preferred stocks and other securities.
        
        Examples:
        - BBAI-WT (warrant) → BBAI
        - GLOP-PA (preferred stock) → GLOP-PA (keep as-is)
        - XYZ-U (unit) → XYZ
        """
        normalized = []
        normalized_set = set()  # Track duplicates
        
        # Define suffixes to remove (warrants, units, rights)
        # Be specific to avoid removing valid preferred stock suffixes
        removable_suffixes = [
            '-WT', '-W',      # Warrants
            '-U',             # Units
            '-R',             # Rights
            '.WS', '/WS',     # Warrants (alternative notation)
            '.U',             # Units (alternative notation)
            '.WS.A', '.WS.B'  # Warrants (class specific)
        ]
        
        for sym in symbols:
            # Start with the original symbol
            base_symbol = sym
            removed_suffix = False
            
            # Check if symbol ends with a removable suffix
            for suffix in removable_suffixes:
                if base_symbol.upper().endswith(suffix.upper()):
                    base_symbol = base_symbol[:-len(suffix)]
                    self.logger.debug(f"Normalized {sym} → {base_symbol} (removed {suffix})")
                    removed_suffix = True
                    break
            
            # Clean up any trailing special chars only if we removed a suffix
            if removed_suffix:
                base_symbol = base_symbol.rstrip('.-/').strip()
            else:
                # Keep original symbol (e.g., GLOP-PA for preferred stocks)
                base_symbol = base_symbol.strip()
            
            # Add only if not duplicate and not empty
            if base_symbol and base_symbol not in normalized_set:
                normalized.append(base_symbol)
                normalized_set.add(base_symbol)
            elif base_symbol in normalized_set:
                self.logger.debug(f"Skipping duplicate: {sym} (already have {base_symbol})")
        
        if len(normalized) < len(symbols):
            self.logger.info(f"Normalized symbols: {len(symbols)} → {len(normalized)} unique base symbols")
        
        return normalized
    
    async def download_bulk(self, symbols: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        """Main bulk download orchestrator with automatic resume."""
        # Normalize symbols first
        symbols = self._normalize_symbols(symbols)
        
        # Clean up any corrupted master files from previous runs
        self._cleanup_corrupted_masters()
        
        # Sync state with existing master files in temp directory
        self._sync_state_with_filesystem()
        
        # Create month batches for the date range
        month_batches = self._create_month_batches(start_date, end_date)
        all_months = [m[0][:6] for m in month_batches]  # Extract YYYYMM
        
        # Get work summary
        work = self.state.get_work_summary(all_months)
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"BULK DOWNLOAD MODE - SMART RESUME")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Total symbols: {len(symbols)}")
        self.logger.info(f"Date range: {start_date} to {end_date}")
        self.logger.info(f"Total months: {work['total_months']}")
        self.logger.info(f"")
        self.logger.info(f"📊 WORK SUMMARY:")
        self.logger.info(f"  ✓ Completed: {work['completed_months']} months ({work['completion_pct']:.1f}%)")
        self.logger.info(f"  🔄 Need download: {work['needs_download']} months")
        self.logger.info(f"  📂 Need split: {work['needs_split']} months")
        
        if work['needs_download'] == 0 and work['needs_split'] == 0:
            self.logger.info(f"\n✓ ALL WORK COMPLETE! Nothing to do.")
            return self.state.get_summary()
        
        # ==========================================
        # PHASE 1: DOWNLOAD ALL MISSING MONTHS
        # ==========================================
        if work['needs_download'] > 0:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"PHASE 1: DOWNLOADING {work['needs_download']} MISSING MONTHS")
            self.logger.info(f"{'='*80}")
            
            # Create symbol batches
            symbol_batch_size = self.config.calculate_optimal_batch_size()
            symbol_batches = self._create_symbol_batches(symbols, symbol_batch_size)
            
            self.logger.info(f"Batch size: {symbol_batch_size} symbols per request")
            self.logger.info(f"Symbol batches: {len(symbol_batches)}")
            
            overall_start = time.time()
            
            # Download ALL months that need downloading (NO SPLITTING)
            for month_idx, (month_start, month_end) in enumerate(month_batches, 1):
                month_label = month_start[:6]
                
                if month_label in work['download_list']:
                    self.logger.info(f"\n{'='*80}")
                    self.logger.info(f"DOWNLOADING MONTH {month_idx}/{work['total_months']}: {month_label}")
                    self.logger.info(f"{'='*80}")
                    
                    await self._download_month_only(
                        month_start, month_end, symbol_batches, month_label
                    )
                else:
                    self.logger.debug(f"⏭️  Skipping month {month_label} (already downloaded)")
            
            overall_duration = time.time() - overall_start
            self.logger.info(f"\n✓ All downloads completed in {overall_duration/60:.2f} minutes")
        
        # ==========================================
        # PHASE 2: SPLIT ALL DOWNLOADED MASTERS
        # ==========================================
        # Refresh work summary to get updated split list
        work = self.state.get_work_summary(all_months)
        
        if work['needs_split'] > 0:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"PHASE 2: SPLITTING {work['needs_split']} MASTER FILES")
            self.logger.info(f"{'='*80}")
            
            for month_label in work['split_list']:
                await self._split_existing_month(month_label)
        
        # Final summary
        summary = self.state.get_summary()
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"PIPELINE COMPLETE")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Total records: {summary['total_records']:,}")
        self.logger.info(f"Months split: {summary['months_split']}")
        self.logger.info(f"Daily files: {summary['daily_files']}")
        self.logger.info(f"{'='*80}")
        
        return summary
    
    def _create_month_batches(self, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """
        Create monthly batches of dates.
        
        Args:
            start_date: Start date YYYYMMDD
            end_date: End date YYYYMMDD
            
        Returns:
            List of (month_start, month_end) tuples in YYYYMMDD format
        """
        batches = []
        
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        current = start_dt
        while current <= end_dt:
            # Month start
            month_start = current
            
            # Calculate month end (last day of current month)
            if current.month == 12:
                month_end_dt = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                month_end_dt = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
            
            # Don't exceed end_date
            if month_end_dt > end_dt:
                month_end_dt = end_dt
            
            batches.append((
                month_start.strftime('%Y%m%d'),
                month_end_dt.strftime('%Y%m%d')
            ))
            
            # Move to next month
            current = month_end_dt + timedelta(days=1)
        
        return batches
    
    def _create_symbol_batches(self, symbols: List[str], batch_size: int) -> List[List[str]]:
        """
        Split symbols into batches.
        
        Args:
            symbols: List of all symbols
            batch_size: Number of symbols per batch
            
        Returns:
            List of symbol batches
        """
        batches = []
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            batches.append(batch)
        return batches
    
    async def _process_month_batch(
        self, 
        month_start: str, 
        month_end: str,
        symbol_batches: List[List[str]],
        month_idx: int
    ):
        """Process one month with automatic skip logic."""
        month_label = month_start[:6]  # YYYYMM
        
        # Double-check if month is already done (shouldn't happen but safety check)
        if self.state.is_month_split(month_label):
            self.logger.info(f"✓ Month {month_label} already complete - skipping")
            return
        
        # If downloaded but not split, just split it
        if self.state.is_month_downloaded(month_label):
            self.logger.info(f"⚠️ Month {month_label} downloaded but not split - splitting now")
            await self._split_existing_month(month_label)
            return
        
        # Need to download this month
        master_file = Path(self.config.TEMP_DIR) / f"{month_label}_master.{self.config.MASTER_FILE_FORMAT}"
        
        self.logger.info(f"Master file: {master_file}")
        
        writer = MasterFileWriter(
            file_path=str(master_file),
            file_format=self.config.MASTER_FILE_FORMAT,
            logger=self.logger
        )
        
        # Process batches
        month_records = 0
        month_symbols = set()
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_REQUESTS)
        
        async def process_single_batch(batch_idx: int, symbol_batch: List[str]):
            nonlocal month_records
            
            async with semaphore:
                batch_id = f"{month_label}_batch{batch_idx:04d}"
                
                # Skip completed batches
                if self.state.is_batch_completed(batch_id):
                    self.logger.debug(f"[{batch_id}] Already completed, skipping")
                    month_symbols.update(symbol_batch)
                    return
                
                self.logger.info(
                    f"\n[BATCH {batch_idx}/{len(symbol_batches)}] "
                    f"🔄 Downloading {len(symbol_batch)} symbols"
                )
                
                self.state.mark_batch_started(batch_id, {
                    'month_start': month_start,
                    'month_end': month_end,
                    'symbols': symbol_batch,
                    'symbol_count': len(symbol_batch)
                })
                
                batch_data = await self._fetch_batch(
                    symbols=symbol_batch,
                    start_date=month_start,
                    end_date=month_end,
                    batch_id=batch_id
                )
                
                if batch_data is not None and len(batch_data) > 0:
                    if writer.append_data(batch_data):
                        month_records += len(batch_data)
                        month_symbols.update(symbol_batch)
                        self.state.mark_batch_completed(
                            batch_id,
                            records=len(batch_data),
                            symbols=len(symbol_batch)
                        )
                        self.logger.info(f"✓ Batch {batch_id}: {len(batch_data):,} records")
                    else:
                        self.state.mark_batch_failed(batch_id, "Write failed")
                else:
                    # Mark as completed but with no data (not a failure)
                    self.state.mark_batch_completed(batch_id, records=0, symbols=0)
                    self.logger.debug(f"[{batch_id}] No data returned (empty batch)")
                
                if self.current_rate_delay > 0:
                    await asyncio.sleep(self.current_rate_delay)
        
        # Launch all batches
        tasks = [
            process_single_batch(batch_idx, symbol_batch)
            for batch_idx, symbol_batch in enumerate(symbol_batches, 1)
        ]
        
        await asyncio.gather(*tasks)
        
        # Close writer
        writer.close()
        
        # Mark month as downloaded
        self.state.mark_month_downloaded(month_label, len(month_symbols), month_records)
        
        # Split immediately
        if master_file.exists():
            self.logger.info(f"\n✓ Month {month_label} download complete: {month_records:,} records")
            await self._split_existing_month(month_label)
    
    async def _download_month_only(
        self,
        month_start: str,
        month_end: str,
        symbol_batches: List[List[str]],
        month_label: str
    ):
        """Download only - skips splitting."""
        # Skip if already downloaded
        if self.state.is_month_downloaded(month_label):
            self.logger.info(f"✓ Month {month_label} already downloaded - skipping")
            return
        
        # Setup master file
        master_file = Path(self.config.TEMP_DIR) / f"{month_label}_master.{self.config.MASTER_FILE_FORMAT}"
        
        self.logger.info(f"Master file: {master_file}")
        
        writer = MasterFileWriter(
            file_path=str(master_file),
            file_format=self.config.MASTER_FILE_FORMAT,
            logger=self.logger
        )
        
        # Process batches
        month_records = 0
        month_symbols = set()
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_REQUESTS)
        
        async def process_single_batch(batch_idx: int, symbol_batch: List[str]):
            nonlocal month_records
            
            async with semaphore:
                batch_id = f"{month_label}_batch{batch_idx:04d}"
                
                # Skip completed batches
                if self.state.is_batch_completed(batch_id):
                    self.logger.debug(f"[{batch_id}] Already completed, skipping")
                    month_symbols.update(symbol_batch)
                    return
                
                self.logger.info(
                    f"\n[BATCH {batch_idx}/{len(symbol_batches)}] "
                    f"🔄 Downloading {len(symbol_batch)} symbols"
                )
                
                self.state.mark_batch_started(batch_id, {
                    'month_start': month_start,
                    'month_end': month_end,
                    'symbols': symbol_batch,
                    'symbol_count': len(symbol_batch)
                })
                
                batch_data = await self._fetch_batch(
                    symbols=symbol_batch,
                    start_date=month_start,
                    end_date=month_end,
                    batch_id=batch_id
                )
                
                if batch_data is not None and len(batch_data) > 0:
                    if writer.append_data(batch_data):
                        month_records += len(batch_data)
                        month_symbols.update(symbol_batch)
                        self.state.mark_batch_completed(
                            batch_id,
                            records=len(batch_data),
                            symbols=len(symbol_batch)
                        )
                        self.logger.info(f"✓ Batch {batch_id}: {len(batch_data):,} records")
                    else:
                        self.state.mark_batch_failed(batch_id, "Write failed")
                else:
                    # Mark as completed but with no data (not a failure)
                    self.state.mark_batch_completed(batch_id, records=0, symbols=0)
                    self.logger.debug(f"[{batch_id}] No data returned (empty batch)")
                
                if self.current_rate_delay > 0:
                    await asyncio.sleep(self.current_rate_delay)
        
        # Launch all batches
        tasks = [
            process_single_batch(batch_idx, symbol_batch)
            for batch_idx, symbol_batch in enumerate(symbol_batches, 1)
        ]
        
        await asyncio.gather(*tasks)
        
        # Close writer (NO SPLITTING HERE!)
        writer.close()
        
        # Mark month as downloaded
        self.state.mark_month_downloaded(month_label, len(month_symbols), month_records)
    
    async def _split_existing_month(self, month_label: str):
        """Split an existing master file into daily files (supports partial splits)."""
        master_file = Path(self.config.TEMP_DIR) / f"{month_label}_master.{self.config.MASTER_FILE_FORMAT}"
        
        if not master_file.exists():
            self.logger.warning(f"⚠️ Master file not found: {master_file}")
            return
        
        # Check if this is a partial split (only missing days)
        missing_days = self.state.get_missing_days_for_month(month_label)
        
        if missing_days:
            self.logger.info(
                f"📂 Splitting {month_label} (PARTIAL: {len(missing_days)} missing days)..."
            )
        else:
            self.logger.info(f"📂 Splitting {month_label} (FULL split)...")
        
        try:
            split_summary = self.splitter.split_master_to_daily(
                master_file=str(master_file),
                output_dir=self.config.OUTPUT_DIR,
                specific_dates=missing_days if missing_days else None  # Pass missing days for partial split
            )
            
            # Mark month as split
            self.state.mark_month_split(
                month_label, 
                split_summary['dates_written'],
                split_summary['total_rows']
            )
            
            # Clean up master file only if FULL split was done
            if not missing_days and split_summary['dates_written'] > 0:
                # Full split complete - delete master
                import time
                for attempt in range(5):
                    try:
                        master_file.unlink()
                        self.logger.info(
                            f"✓ Split complete: {split_summary['dates_written']} dates, "
                            f"{split_summary['total_rows']:,} rows, master file deleted"
                        )
                        break
                    except PermissionError:
                        if attempt < 4:
                            self.logger.debug(f"File locked, retrying deletion in 1s... (attempt {attempt+1}/5)")
                            time.sleep(1)
                        else:
                            self.logger.warning(
                                f"⚠️ Could not delete master file (file locked): {master_file}"
                            )
                            self.logger.warning(
                                f"   The split was successful, you can manually delete this file later"
                            )
            elif missing_days:
                # Partial split - keep master for potential future extractions
                self.logger.info(
                    f"✓ Partial split complete: {split_summary['dates_written']} dates, "
                    f"{split_summary['total_rows']:,} rows (master file retained)"
                )
            else:
                self.logger.warning(f"⚠️ No dates written - keeping master file")
                
        except RuntimeError as e:
            # Corrupted master file detected
            if "Corrupted master file" in str(e):
                self.logger.error(f"❌ Master file corrupted: {master_file}")
                self.logger.warning(f"   Attempting to delete corrupted file and mark for re-download...")
                
                try:
                    # Delete the corrupted file
                    master_file.unlink()
                    self.logger.info(f"   ✓ Deleted corrupted master file")
                except Exception as del_error:
                    self.logger.error(f"   ✗ Could not delete corrupted file: {del_error}")
                    self.logger.error(f"   Please manually delete: {master_file}")
                
                # Remove from state so it will be re-downloaded
                if month_label in self.state.state['completed_months']:
                    del self.state.state['completed_months'][month_label]
                    self.state._save_state()
                    self.logger.info(f"   ✓ Unmarked {month_label} for re-download")
                else:
                    self.logger.debug(f"   Month {month_label} not in completed_months")
                
        except Exception as e:
            self.logger.error(f"❌ Split failed for {month_label}: {e}", exc_info=True)
            self.logger.warning(f"   Keeping master file for investigation: {master_file}")
            
            # Check if it's a corrupted file error
            if "invalid" in str(e).lower() or "flatbuffer" in str(e).lower() or "corrupt" in str(e).lower():
                self.logger.warning(f"⚠️ Master file appears corrupted: {master_file}")
                self.logger.warning(f"   Attempting to delete and re-download...")
                
                try:
                    master_file.unlink()
                    self.logger.info(f"   ✓ Deleted corrupt master file")
                    
                    # Mark as failed so it will be re-downloaded
                    if month_label in self.state.state['completed_months']:
                        del self.state.state['completed_months'][month_label]
                        self.state._save_state()
                        self.logger.info(f"   ✓ Unmarked month {month_label} for re-download")
                        
                except Exception as del_err:
                    self.logger.error(f"   ✗ Could not delete: {del_err}")
    
    async def split_all_pending(self) -> Dict[str, Any]:
        """
        Split all downloaded but unsplit months.
        Use this when downloads completed but splitting failed.
        """
        unsplit_months = self.state.get_unsplit_months()
        
        if not unsplit_months:
            self.logger.info("No pending months to split")
            return {'months_split': 0, 'total_dates': 0, 'total_rows': 0}
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"SPLIT-ONLY MODE")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Found {len(unsplit_months)} months to split: {', '.join(unsplit_months)}")
        
        total_dates = 0
        total_rows = 0
        months_split = 0
        
        for month_label in unsplit_months:
            await self._split_existing_month(month_label)
            months_split += 1
        
        summary = self.state.get_summary()
        return {
            'months_split': months_split,
            'total_dates': summary.get('months_split', 0),
            'total_rows': 0
        }
    
    def _sync_state_with_filesystem(self):
        """
        Sync state manager with existing master files in temp directory.
        Marks months as downloaded if master files exist but aren't in state.
        """
        temp_dir = Path(self.config.TEMP_DIR)
        if not temp_dir.exists():
            return
        
        master_files = list(temp_dir.glob(f"*_master.{self.config.MASTER_FILE_FORMAT}"))
        if not master_files:
            return
        
        self.logger.info(f"🔍 Syncing state with {len(master_files)} existing master files...")
        
        synced_count = 0
        for master_file in master_files:
            month_label = master_file.stem.replace('_master', '')
            
            # Skip if already tracked as downloaded
            if self.state.is_month_downloaded(month_label):
                self.logger.debug(f"  ✓ {month_label}: Already in state")
                continue
            
            # Check file size to ensure it's not empty or corrupted
            file_size = master_file.stat().st_size
            if file_size > 0:
                # Mark as downloaded (we don't know exact counts, use placeholders)
                self.state.mark_month_downloaded(month_label, symbols=0, records=0)
                synced_count += 1
                self.logger.info(f"  ✓ {month_label}: Synced to state ({file_size:,} bytes)")
            else:
                self.logger.warning(f"  ⚠️ {month_label}: Empty file, will re-download")
                try:
                    master_file.unlink()
                    self.logger.info(f"      Deleted empty file")
                except Exception as e:
                    self.logger.error(f"      Could not delete empty file: {e}")
        
        if synced_count > 0:
            self.logger.info(f"✓ Synced {synced_count} existing master file(s) to state")
    
    def _cleanup_corrupted_masters(self) -> int:
        """
        Detect and delete corrupted master files.
        Returns count of deleted files.
        """
        import pyarrow as pa
        import pyarrow.ipc as ipc
        
        temp_dir = Path(self.config.TEMP_DIR)
        if not temp_dir.exists():
            return 0
        
        master_files = list(temp_dir.glob(f"*_master.{self.config.MASTER_FILE_FORMAT}"))
        if not master_files:
            return 0
        
        self.logger.info(f"🔍 Checking {len(master_files)} master files for corruption...")
        
        deleted_count = 0
        for master_file in master_files:
            month_label = master_file.stem.replace('_master', '')
            
            try:
                # Try to open the file to verify it's valid
                if self.config.MASTER_FILE_FORMAT == 'arrow':
                    with pa.memory_map(str(master_file), 'r') as source:
                        reader = ipc.open_file(source)
                        _ = reader.num_record_batches  # Just check if readable
                # File is valid
                self.logger.debug(f"  ✓ {month_label}: Valid")
                
            except Exception as e:
                # File is corrupted
                self.logger.warning(f"  ✗ {month_label}: Corrupted - {str(e)[:50]}")
                try:
                    master_file.unlink()
                    deleted_count += 1
                    self.logger.info(f"    ✓ Deleted corrupted file")
                    
                    # Remove from state
                    if month_label in self.state.state['completed_months']:
                        del self.state.state['completed_months'][month_label]
                        self.logger.info(f"    ✓ Unmarked for re-download")
                        
                except Exception as del_error:
                    self.logger.error(f"    ✗ Could not delete: {del_error}")
        
        if deleted_count > 0:
            self.state._save_state()
            self.logger.info(f"✓ Cleanup complete: removed {deleted_count} corrupted file(s)")
        
        return deleted_count
    
    async def _fetch_batch(
        self, 
        symbols: List[str], 
        start_date: str, 
        end_date: str,
        batch_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Fetch batch of symbols for date range from Alpaca API.
        
        Args:
            symbols: List of symbols to fetch
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            batch_id: Unique identifier for this batch
            
        Returns:
            DataFrame with columns: symbol, timestamp, open, high, low, close, volume
            Returns None if request fails
        """
        try:
            # Convert YYYYMMDD to YYYY-MM-DD for API
            start_api = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
            end_api = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            
            # Build API request
            url = f"{self.api.base_url}/v2/stocks/bars"
            params = {
                "symbols": ",".join(symbols),  # Comma-separated list
                "timeframe": "1Min",            # 1-minute bars
                "start": start_api,
                "end": end_api,
                "limit": 10000,                 # Max per page
                "feed": self.config.FEED,
                "adjustment": "all"
            }
            
            self.logger.debug(f"[{batch_id}] Requesting {len(symbols)} symbols from {start_api} to {end_api}")
            
            all_data = []
            page_token = None
            page_count = 0
            max_pages = 1000  # Safety limit
            
            while page_count < max_pages:
                if page_token:
                    params["page_token"] = page_token
                
                response = await self.api.session.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract bars from response
                    bars = data.get("bars", {})
                    if not bars:
                        self.logger.debug(f"[{batch_id}] No bars data in response")
                        break
                    
                    # Parse bars into records
                    for symbol, symbol_bars in bars.items():
                        for bar in symbol_bars:
                            all_data.append({
                                "symbol": symbol,
                                "timestamp": bar.get("t"),
                                "open": bar.get("o"),
                                "high": bar.get("h"),
                                "low": bar.get("l"),
                                "close": bar.get("c"),
                                "volume": bar.get("v")
                            })
                    
                    # Check for next page
                    page_token = data.get("next_page_token")
                    page_count += 1
                    
                    if not page_token:
                        break  # No more pages
                        
                    self.logger.debug(f"[{batch_id}] Page {page_count}: {len(all_data)} records so far")
                    
                elif response.status_code == 429:
                    # Rate limit hit
                    self.logger.warning(f"[{batch_id}] Rate limit hit, backing off...")
                    self.consecutive_failures += 1
                    self.current_rate_delay = min(self.current_rate_delay * 2, 5.0)
                    await asyncio.sleep(5)
                    continue
                
                elif response.status_code == 422:
                    # Invalid request (bad symbols, etc) - silently skip
                    self.logger.debug(f"[{batch_id}] Invalid symbols in batch (422) - skipping")
                    return pd.DataFrame()  # Empty result, will skip gracefully
            
                elif response.status_code == 400:
                    # Symbol doesn't exist in Alpaca - silently skip
                    self.logger.debug(f"[{batch_id}] Symbol(s) not found (400) - skipping")
                    return pd.DataFrame()  # Empty result, will skip gracefully
            
            if page_count >= max_pages:
                self.logger.warning(f"[{batch_id}] Hit max pages limit ({max_pages})")
            
            # Convert to DataFrame
            if all_data:
                df = pd.DataFrame(all_data)
                self.logger.debug(f"[{batch_id}] Fetched {len(df):,} records")
                
                # Dynamic rate adjustment - speed up on success
                self.consecutive_successes += 1
                self.consecutive_failures = 0
                if self.consecutive_successes >= 5:
                    self.current_rate_delay = max(self.current_rate_delay * 0.8, 0.1)
                    self.consecutive_successes = 0
                
                return df
            else:
                self.logger.debug(f"[{batch_id}] No data returned")
                return None
                
        except asyncio.TimeoutError:
            self.logger.error(f"[{batch_id}] Request timeout")
            self.consecutive_failures += 1
            self.current_rate_delay = min(self.current_rate_delay * 1.5, 5.0)
            return None
            
        except Exception as e:
            self.logger.error(f"[{batch_id}] Fetch error: {e}", exc_info=True)
            self.consecutive_failures += 1
            return None
