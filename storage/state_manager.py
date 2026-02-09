"""
State manager for tracking download progress and completion status.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Set, List
from datetime import datetime


class StateManager:
    """Track download and split progress with automatic resume capability."""
    
    def __init__(self, state_file: str, logger: logging.Logger, output_dir: str = None, temp_dir: str = None):
        """Initialize state manager."""
        self.state_file = Path(state_file)
        self.output_dir = Path(output_dir) if output_dir else None
        self.temp_dir = Path(temp_dir) if temp_dir else None
        self.logger = logger
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        
        # ✅ CRITICAL: Validate state against filesystem with DAY-LEVEL granularity
        self._validate_and_sync_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        # Define default state structure
        default_state = {
            'batches': {},
            'completed_months': {},  # Track which months are fully downloaded
            'split_months': {},      # Track which months are split
            'daily_files': {},       # Track individual daily files
            'missing_days': {},      # NEW: Track missing days per month that need extraction
            'start_time': None,
            'last_update': None
        }
        
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    # Merge loaded state with default to ensure all keys exist
                    for key in default_state:
                        if key not in state:
                            state[key] = default_state[key]
                    self.logger.info(f"✓ Loaded state: {len(state.get('batches', {}))} batches tracked")
                    return state
            except Exception as e:
                self.logger.warning(f"Failed to load state file: {e}")
                # Fall through to return default state
        
        # Return default state structure
        return default_state
    
    def _validate_and_sync_state(self):
        """
        Validate state against filesystem reality with DAY-LEVEL granularity.
        For each month with a master file, identify missing daily files.
        Also validates file integrity (detects corruption).
        
        IMPORTANT: If corrupt files are found, they are renamed (not deleted)
        and execution is stopped for manual investigation.
        """
        from utils.trading_calendar import TradingCalendar
        import pyarrow.parquet as pq
        
        # Initialize trading calendar
        calendar = TradingCalendar(logger=self.logger)
        
        # Step 1: Scan for actual daily files AND validate them
        actual_daily_files = {}
        daily_files_by_month = {}  # Group by month for easier lookup
        corrupt_daily_files = []
        
        if self.output_dir and self.output_dir.exists():
            self.logger.info(f"🔍 Validating state against filesystem (day-level granularity + corruption check)...")
            
            # Find all daily parquet files
            existing_files = list(self.output_dir.glob("**/????????.parquet"))
            
            if existing_files:
                self.logger.info(f"   Found {len(existing_files)} daily files, validating integrity...")
                
                for file_path in existing_files:
                    date_str = file_path.stem  # YYYYMMDD
                    month_str = date_str[:6]   # YYYYMM
                    
                    # ✅ VALIDATE FILE INTEGRITY
                    try:
                        # Quick validation: try to read parquet metadata
                        metadata = pq.read_metadata(file_path)
                        num_rows = metadata.num_rows
                        file_size_mb = file_path.stat().st_size / (1024 * 1024)
                        
                        # Check if file is suspiciously small (likely corrupt or empty)
                        if file_size_mb < 0.001:  # Less than 1KB
                            raise ValueError(f"File too small ({file_size_mb:.6f} MB)")
                        
                        # File is valid
                        actual_daily_files[date_str] = {
                            'path': str(file_path),
                            'detected_at': datetime.now().isoformat(),
                            'size_mb': file_size_mb,
                            'rows': num_rows
                        }
                        
                        # Group by month
                        if month_str not in daily_files_by_month:
                            daily_files_by_month[month_str] = set()
                        daily_files_by_month[month_str].add(date_str)
                        
                    except Exception as e:
                        # File is corrupted
                        error_msg = str(e)[:100]
                        self.logger.error(
                            f"   ❌ CORRUPT DAILY FILE: {file_path.name} - {error_msg}"
                        )
                        corrupt_daily_files.append((file_path, error_msg))
                
                # Handle corrupt files - RENAME AND STOP
                if corrupt_daily_files:
                    self.logger.error(
                        f"\n{'='*80}\n"
                        f"❌ CORRUPTION DETECTED: Found {len(corrupt_daily_files)} corrupt daily files!\n"
                        f"{'='*80}"
                    )
                    
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    renamed_files = []
                    
                    for corrupt_file, reason in corrupt_daily_files:
                        try:
                            # Rename with .CORRUPT suffix and timestamp
                            new_name = f"{corrupt_file.stem}.CORRUPT_{timestamp}{corrupt_file.suffix}"
                            new_path = corrupt_file.parent / new_name
                            corrupt_file.rename(new_path)
                            
                            self.logger.error(f"   ✓ Renamed: {corrupt_file.name} → {new_name}")
                            self.logger.error(f"      Reason: {reason}")
                            renamed_files.append(str(new_path))
                            
                        except Exception as e:
                            self.logger.error(f"   ✗ Could not rename {corrupt_file.name}: {e}")
                    
                    # STOP EXECUTION
                    self.logger.error(
                        f"\n{'='*80}\n"
                        f"⚠️  EXECUTION STOPPED - MANUAL INTERVENTION REQUIRED\n"
                        f"{'='*80}\n"
                        f"Corrupt daily files have been renamed with .CORRUPT_{timestamp} suffix.\n"
                        f"Please investigate these files:\n"
                    )
                    for renamed in renamed_files:
                        self.logger.error(f"   - {renamed}")
                    
                    self.logger.error(
                        f"\nActions you can take:\n"
                        f"  1. Inspect the corrupt files to understand the issue\n"
                        f"  2. Delete the .CORRUPT files if they're unrecoverable\n"
                        f"  3. Re-run the pipeline to re-download/re-split missing data\n"
                        f"\n{'='*80}"
                    )
                    
                    raise RuntimeError(
                        f"Detected {len(corrupt_daily_files)} corrupt daily files. "
                        f"Files have been renamed with .CORRUPT_{timestamp} suffix. "
                        f"Please investigate and remove corrupt files before continuing."
                    )
                
                valid_count = len(actual_daily_files)
                if valid_count > 0:
                    self.logger.info(f"   ✓ {valid_count} valid daily files")
                else:
                    self.logger.info(f"   No valid daily files found")
            else:
                self.logger.info(f"   No daily files found in {self.output_dir}")
        
        # Step 2: Update state with actual files
        self.state['daily_files'] = actual_daily_files
        
        # Step 3: Scan for master files, check corruption, and check day-by-day
        if self.temp_dir and self.temp_dir.exists():
            master_files = list(self.temp_dir.glob("*_master.*"))
            
            if master_files:
                self.logger.info(f"🔍 Analyzing {len(master_files)} master files (checking corruption + missing days)...")
                
                total_missing_days = 0
                months_needing_split = 0
                months_complete = 0
                corrupt_masters = []
                
                for file_path in master_files:
                    month_str = file_path.stem.replace('_master', '')
                    
                    if len(month_str) == 6 and month_str.isdigit():
                        file_size_mb = file_path.stat().st_size / (1024 * 1024)
                        
                        # Check for empty files
                        if file_size_mb == 0:
                            self.logger.error(f"   ❌ {month_str}: Empty master file (0 bytes)")
                            corrupt_masters.append((file_path, month_str, "Empty file (0 bytes)"))
                            continue
                        
                        # ✅ VALIDATE MASTER FILE INTEGRITY
                        file_format = file_path.suffix[1:]  # .arrow -> arrow
                        is_valid = False
                        corruption_reason = None
                        
                        try:
                            if file_format == 'arrow':
                                import pyarrow as pa
                                import pyarrow.ipc as ipc
                                
                                with pa.memory_map(str(file_path), 'r') as source:
                                    reader = ipc.open_file(source)
                                    num_batches = reader.num_record_batches
                                    
                                    # Check we have data
                                    if num_batches == 0:
                                        raise ValueError("No record batches in file")
                                    
                                    # Quick check: ensure we can read first batch
                                    _ = reader.get_batch(0)
                                    
                                    # Verify schema has required columns
                                    schema = reader.schema
                                    required_cols = ['symbol', 'timestamp']
                                    missing_cols = [col for col in required_cols if col not in schema.names]
                                    if missing_cols:
                                        raise ValueError(f"Missing required columns: {missing_cols}")
                            
                            elif file_format == 'parquet':
                                metadata = pq.read_metadata(file_path)
                                num_rows = metadata.num_rows
                                
                                if num_rows == 0:
                                    raise ValueError("No rows in file")
                                
                                # Try reading a small sample
                                sample = pq.read_table(file_path, columns=['symbol'], use_threads=False)
                                if len(sample) == 0:
                                    raise ValueError("Cannot read data from file")
                            
                            elif file_format == 'csv':
                                import pandas as pd
                                df_sample = pd.read_csv(file_path, nrows=5)
                                
                                if len(df_sample) == 0:
                                    raise ValueError("Empty CSV file")
                                
                                required_cols = ['symbol', 'timestamp']
                                missing_cols = [col for col in required_cols if col not in df_sample.columns]
                                if missing_cols:
                                    raise ValueError(f"Missing required columns: {missing_cols}")
                            
                            is_valid = True
                            
                        except Exception as e:
                            corruption_reason = str(e)[:100]
                            self.logger.error(
                                f"   ❌ {month_str}: Corrupt master file ({file_format}) - {corruption_reason}"
                            )
                            corrupt_masters.append((file_path, month_str, corruption_reason))
                            continue
                        
                        # File is valid, continue with day-level checks
                        if is_valid:
                            # Get all trading days for this month
                            year = month_str[:4]
                            month = month_str[4:6]
                            
                            # Get first and last day of month
                            month_start = f"{year}{month}01"
                            if month == "12":
                                month_end = f"{year}{month}31"
                            else:
                                next_month = f"{int(month)+1:02d}"
                                month_end = f"{year}{next_month}01"
                            
                            # Get trading days for this month
                            try:
                                trading_days, _ = calendar.get_trading_days_with_skipped(
                                    month_start, month_end
                                )
                            except:
                                self.logger.warning(f"Could not get trading days for {month_str}, skipping")
                                continue
                            
                            # Convert to set for quick lookup
                            expected_days = set(trading_days)
                            existing_days = daily_files_by_month.get(month_str, set())
                            
                            # Find missing days
                            missing_days = expected_days - existing_days
                            
                            if missing_days:
                                # Month has missing days - needs partial split
                                self.state['completed_months'][month_str] = {
                                    'downloaded_at': datetime.now().isoformat(),
                                    'symbol_count': 0,
                                    'record_count': 0,
                                    'status': 'downloaded',
                                    'detected_from_filesystem': True,
                                    'master_file': str(file_path),
                                    'size_mb': file_size_mb,
                                    'validated': True
                                }
                                
                                self.state['missing_days'][month_str] = sorted(list(missing_days))
                                
                                if month_str in self.state['split_months']:
                                    del self.state['split_months'][month_str]
                                
                                total_missing_days += len(missing_days)
                                months_needing_split += 1
                                
                                self.logger.info(
                                    f"   ⚠️  {month_str}: missing {len(missing_days)}/{len(expected_days)} days "
                                    f"({file_size_mb:.1f} MB master, validated ✓)"
                                )
                                
                            else:
                                # Month is complete!
                                if month_str not in self.state['split_months']:
                                    self.state['split_months'][month_str] = {
                                        'split_at': datetime.now().isoformat(),
                                        'dates_written': len(expected_days),
                                        'rows_written': 0,
                                        'status': 'split',
                                        'detected_from_filesystem': True
                                    }
                                
                                if month_str in self.state['completed_months']:
                                    self.state['completed_months'][month_str]['status'] = 'split_complete'
                                
                                if month_str in self.state.get('missing_days', {}):
                                    del self.state['missing_days'][month_str]
                                
                                months_complete += 1
                                
                                self.logger.debug(
                                    f"   ✓ {month_str}: complete with {len(expected_days)} days (validated ✓)"
                                )
                
                # Handle corrupt master files - RENAME AND STOP
                if corrupt_masters:
                    self.logger.error(
                        f"\n{'='*80}\n"
                        f"❌ CORRUPTION DETECTED: Found {len(corrupt_masters)} corrupt master files!\n"
                        f"{'='*80}"
                    )
                    
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    renamed_files = []
                    
                    for corrupt_file, month_str, reason in corrupt_masters:
                        try:
                            # Rename with .CORRUPT suffix and timestamp
                            new_name = f"{corrupt_file.stem}.CORRUPT_{timestamp}{corrupt_file.suffix}"
                            new_path = corrupt_file.parent / new_name
                            corrupt_file.rename(new_path)
                            
                            self.logger.error(f"   ✓ Renamed: {corrupt_file.name} → {new_name}")
                            self.logger.error(f"      Month: {month_str}")
                            self.logger.error(f"      Reason: {reason}")
                            renamed_files.append((month_str, str(new_path)))
                            
                            # Remove from state
                            if month_str in self.state['completed_months']:
                                del self.state['completed_months'][month_str]
                            if month_str in self.state['split_months']:
                                del self.state['split_months'][month_str]
                            if month_str in self.state.get('missing_days', {}):
                                del self.state['missing_days'][month_str]
                        
                        except Exception as e:
                            self.logger.error(f"   ✗ Could not rename {corrupt_file.name}: {e}")
                    
                    # Save state (removes corrupt months from tracking)
                    self._save_state()
                    
                    # STOP EXECUTION
                    self.logger.error(
                        f"\n{'='*80}\n"
                        f"⚠️  EXECUTION STOPPED - MANUAL INTERVENTION REQUIRED\n"
                        f"{'='*80}\n"
                        f"Corrupt master files have been renamed with .CORRUPT_{timestamp} suffix.\n"
                        f"These months will be re-downloaded on next run.\n"
                        f"\nCorrupt files:\n"
                    )
                    for month, renamed_path in renamed_files:
                        self.logger.error(f"   - {month}: {renamed_path}")
                    
                    self.logger.error(
                        f"\nActions you can take:\n"
                        f"  1. Inspect the corrupt files to understand the issue\n"
                        f"  2. Delete the .CORRUPT files if they're unrecoverable\n"
                        f"  3. Re-run the pipeline to re-download the affected months\n"
                        f"\n{'='*80}"
                    )
                    
                    raise RuntimeError(
                        f"Detected {len(corrupt_masters)} corrupt master files. "
                        f"Files have been renamed with .CORRUPT_{timestamp} suffix. "
                        f"Affected months will be re-downloaded on next run."
                    )
                
                if months_needing_split > 0:
                    self.logger.info(
                        f"✓ Found {months_needing_split} months needing split "
                        f"({total_missing_days} missing days total)"
                    )
                
                if months_complete > 0:
                    self.logger.info(f"✓ Verified {months_complete} months are complete")
        
        # Step 4: Check for phantom splits
        state_split_months = set(self.state.get('split_months', {}).keys())
        
        for month in list(state_split_months):
            master_exists = False
            if self.temp_dir and self.temp_dir.exists():
                for ext in ['arrow', 'parquet', 'csv']:
                    master_file = self.temp_dir / f"{month}_master.{ext}"
                    if master_file.exists():
                        master_exists = True
                        break
            
            existing_days = daily_files_by_month.get(month, set())
            if not master_exists and not existing_days:
                self.logger.warning(
                    f"⚠️  Month {month} marked as split but no files found - clearing status"
                )
                del self.state['split_months'][month]
        
        # Save updated state
        self._save_state()
        self.logger.info(f"✓ State validation complete - no corruption detected")
    
    def get_missing_days_for_month(self, month: str) -> List[str]:
        """
        Get list of missing days for a month.
        
        Args:
            month: Month in YYYYMM format
            
        Returns:
            List of missing dates in YYYYMMDD format
        """
        return self.state.get('missing_days', {}).get(month, [])
    
    def _save_state(self):
        """Save state to file."""
        try:
            self.state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    def is_batch_completed(self, batch_id: str) -> bool:
        """Check if batch is completed."""
        return self.state['batches'].get(batch_id, {}).get('status') == 'completed'
    
    def mark_batch_started(self, batch_id: str, metadata: Dict[str, Any]):
        """Mark batch as started."""
        self.state['batches'][batch_id] = {
            'status': 'in_progress',
            'started_at': datetime.now().isoformat(),
            'metadata': metadata
        }
        self._save_state()
    
    def mark_batch_completed(self, batch_id: str, records: int, symbols: int):
        """Mark batch as completed."""
        if batch_id in self.state['batches']:
            self.state['batches'][batch_id].update({
                'status': 'completed',
                'completed_at': datetime.now().isoformat(),
                'records': records,
                'symbols': symbols
            })
        self._save_state()
    
    def mark_batch_failed(self, batch_id: str, error: str):
        """Mark batch as failed."""
        if batch_id in self.state['batches']:
            self.state['batches'][batch_id].update({
                'status': 'failed',
                'failed_at': datetime.now().isoformat(),
                'error': error
            })
        self._save_state()
    
    def mark_month_downloaded(self, month: str, symbol_count: int, record_count: int):
        """Mark a month as fully downloaded."""
        self.state['completed_months'][month] = {
            'downloaded_at': datetime.now().isoformat(),
            'symbol_count': symbol_count,
            'record_count': record_count,
            'status': 'downloaded'
        }
        self._save_state()
    
    def mark_month_split(self, month: str, dates_written: int, rows_written: int):
        """Mark a month as split into daily files."""
        self.state['split_months'][month] = {
            'split_at': datetime.now().isoformat(),
            'dates_written': dates_written,
            'rows_written': rows_written,
            'status': 'split'
        }
        if month in self.state['completed_months']:
            self.state['completed_months'][month]['status'] = 'split_complete'
        
        if month in self.state.get('missing_days', {}):
            del self.state['missing_days'][month]
        
        self._save_state()
    
    def is_month_downloaded(self, month: str) -> bool:
        """Check if month is fully downloaded."""
        return month in self.state['completed_months']
    
    def is_month_split(self, month: str) -> bool:
        """Check if month is split into daily files."""
        return month in self.state['split_months']
    
    def needs_download(self, month: str) -> bool:
        """Check if month needs to be downloaded."""
        return not self.is_month_downloaded(month) and not self.is_month_split(month)
    
    def needs_split(self, month: str) -> bool:
        """Check if month needs to be split (fully or partially)."""
        has_missing_days = month in self.state.get('missing_days', {})
        return (self.is_month_downloaded(month) and not self.is_month_split(month)) or has_missing_days
    
    def get_months_needing_download(self, all_months: List[str]) -> List[str]:
        """Get list of months that need downloading."""
        return [m for m in all_months if self.needs_download(m)]
    
    def get_months_needing_split(self) -> List[str]:
        """Get list of months that need splitting (includes partial splits)."""
        downloaded = set(self.state['completed_months'].keys())
        split = set(self.state['split_months'].keys())
        needs_full_split = downloaded - split
        
        needs_partial_split = set(self.state.get('missing_days', {}).keys())
        
        return sorted(needs_full_split | needs_partial_split)
    
    def get_work_summary(self, all_months: List[str]) -> Dict[str, Any]:
        """
        Get summary of what work needs to be done.
        
        Args:
            all_months: List of all month labels (YYYYMM) in date range
            
        Returns:
            Dict with counts of completed/pending work
        """
        needs_download = self.get_months_needing_download(all_months)
        needs_split = self.get_months_needing_split()
        completed = [m for m in all_months if self.is_month_split(m)]
        
        total_missing_days = sum(
            len(days) for days in self.state.get('missing_days', {}).values()
        )
        
        return {
            'total_months': len(all_months),
            'completed_months': len(completed),
            'needs_download': len(needs_download),
            'needs_split': len(needs_split),
            'missing_days_count': total_missing_days,
            'download_list': needs_download,
            'split_list': needs_split,
            'completion_pct': (len(completed) / len(all_months) * 100) if all_months else 0
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        batches = self.state['batches']
        completed = sum(1 for b in batches.values() if b.get('status') == 'completed')
        failed = sum(1 for b in batches.values() if b.get('status') == 'failed')
        total_records = sum(b.get('records', 0) for b in batches.values() 
                          if b.get('status') == 'completed')
        total_symbols = sum(b.get('symbols', 0) for b in batches.values() 
                          if b.get('status') == 'completed')
        
        # ✅ COUNT ACTUAL DAILY FILES ON DISK
        actual_daily_files_count = 0
        if self.output_dir and self.output_dir.exists():
            actual_daily_files_count = len(list(self.output_dir.glob("**/????????.parquet")))
        
        return {
            'completed_batches': completed,
            'failed_batches': failed,
            'total_records': total_records,
            'total_symbols': total_symbols,
            'months_downloaded': len(self.state['completed_months']),
            'months_split': len(self.state['split_months']),
            'daily_files': actual_daily_files_count  # ✅ Live count from filesystem
        }
    
    def reset(self):
        """Reset all state."""
        self.state = {
            'batches': {},
            'completed_months': {},
            'split_months': {},
            'daily_files': {},
            'missing_days': {},
            'start_time': None,
            'last_update': None
        }
        self._save_state()
        self.logger.info("State reset")
