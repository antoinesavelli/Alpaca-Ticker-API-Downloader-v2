"""
Parquet writer for saving data to parquet files.
UPDATED: Focus on consolidated daily files (one file per day with all symbols).
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from config.settings import Config
from utils.helpers import calculate_file_size_mb
import asyncio


class ParquetWriter:
    """Write data to parquet files - ONE consolidated file per day."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize parquet writer with configuration."""
        self.config = config
        self.logger = logger
        self.parquet_settings = config.get_parquet_settings()
    
    def _get_date_folder_path(self, base_dir: str, date: str) -> Path:
        """
        Create folder path structure: base_dir/YYYY/MM
        
        Args:
            base_dir: Base output directory
            date: Date in YYYYMMDD format
            
        Returns:
            Path object with year/month structure
        """
        # Parse date string YYYYMMDD
        year = date[:4]
        month = date[4:6]
        
        # Create path structure: YYYY/MM (removed day folder)
        date_path = Path(base_dir) / year / month
        return date_path
    
    def write_date_parquet(self, date: str, df: pd.DataFrame, output_dir: str, overwrite: bool = False) -> Optional[str]:
        """
        Write all data for a date to a single consolidated parquet file.
        
        REQUIRED columns: symbol, timestamp, open, high, low, close, volume
        OPTIONAL columns: cumulative_volume, count, vwap
        """
        if df is None or len(df) == 0:
            self.logger.warning(f"No data to write for date {date}")
            return None
        
        try:
            # Create year/month directory structure: YYYY/MM/
            output_path = self._get_date_folder_path(output_dir, date)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Sort data by symbol and timestamp
            df = df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
            
            # REQUIRED columns
            required_cols = [
                'symbol',      # str - Stock ticker
                'timestamp',   # datetime64 - Bar timestamp (US/Eastern)
                'open',        # float64 - Opening price
                'high',        # float64 - High price
                'low',         # float64 - Low price
                'close',       # float64 - Closing price
                'volume'       # int64 - Trading volume
            ]
            
            # OPTIONAL columns
            optional_cols = [
                'cumulative_volume',  # int64 - Running total volume for the day
                'count',              # int64 - Number of trades in the bar
                'vwap'                # float64 - Volume-weighted average price
            ]
            
            # Check for missing REQUIRED columns
            missing_required = [col for col in required_cols if col not in df.columns]
            if missing_required:
                self.logger.error(f"Missing REQUIRED columns: {missing_required}")
                self.logger.error(f"Available columns: {df.columns.tolist()}")
                return None
            
            # Log which optional columns are present
            present_optional = [col for col in optional_cols if col in df.columns]
            missing_optional = [col for col in optional_cols if col not in df.columns]
            
            if missing_optional:
                self.logger.debug(f"Optional columns not present: {missing_optional}")
            
            # Data quality validation per spec
            initial_len = len(df)
            
            # MUST have valid data: open > 0, close > 0, volume >= 0
            df = df[(df['open'] > 0) & (df['close'] > 0) & (df['volume'] >= 0)]
            
            # Remove nulls: symbol.isna(), timestamp.isna()
            df = df.dropna(subset=['symbol', 'timestamp'])
            
            if len(df) < initial_len:
                removed = initial_len - len(df)
                self.logger.warning(f"Removed {removed} invalid rows (quality validation)")
            
            if len(df) == 0:
                self.logger.warning(f"No valid data remaining after quality checks for {date}")
                return None
            
            # Ensure timestamp is datetime64 with US/Eastern timezone
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure US/Eastern timezone per spec
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('US/Eastern')
            elif str(df['timestamp'].dt.tz) not in ['US/Eastern', 'America/New_York']:
                df['timestamp'] = df['timestamp'].dt.tz_convert('US/Eastern')
            
            # Ensure correct data types for REQUIRED columns
            df['symbol'] = df['symbol'].astype(str)
            df['open'] = df['open'].astype('float64')
            df['high'] = df['high'].astype('float64')
            df['low'] = df['low'].astype('float64')
            df['close'] = df['close'].astype('float64')
            df['volume'] = df['volume'].astype('int64')
            
            # Ensure correct data types for OPTIONAL columns (if present)
            if 'cumulative_volume' in df.columns:
                df['cumulative_volume'] = df['cumulative_volume'].astype('int64')
            if 'count' in df.columns:
                df['count'] = df['count'].astype('int64')
            if 'vwap' in df.columns:
                df['vwap'] = df['vwap'].astype('float64')
            
            # Select columns to write (required + present optional)
            columns_to_write = required_cols + present_optional
            df = df[columns_to_write]
            
            # Define output file path: YYYY/MM/YYYYMMDD.parquet
            file_path = output_path / f"{date}.parquet"
            
            # Check if file exists and overwrite is disabled
            if file_path.exists() and not overwrite:
                self.logger.info(f"File already exists, skipping: {file_path}")
                return None
            
            # Write parquet file with compression
            df.to_parquet(
                file_path,
                engine='pyarrow',
                compression=self.parquet_settings['compression'],
                index=False
            )
            
            # Log success
            file_size = calculate_file_size_mb(str(file_path))
            self.logger.info(
                f"✓ Wrote {date}.parquet: {len(df):,} rows, "
                f"{df['symbol'].nunique()} symbols, {file_size:.2f} MB, "
                f"columns: {len(columns_to_write)}"
            )
            
            return str(file_path)
        
        except Exception as e:
            self.logger.error(f"Failed to write parquet for {date}: {e}", exc_info=True)
            return None
    
    def read_date_parquet(self, date: str, output_dir: str) -> Optional[pd.DataFrame]:
        """
        Read consolidated parquet file for a specific date.
        
        Args:
            date: Date in YYYYMMDD format
            output_dir: Directory containing parquet files
            
        Returns:
            DataFrame with data, or None if file doesn't exist
        """
        # Use date folder structure
        date_path = self._get_date_folder_path(output_dir, date)
        file_path = date_path / f"{date}.parquet"
        
        if not file_path.exists():
            return None
        
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            self.logger.error(f"Failed to read parquet for {date}: {e}")
            return None
    
    async def write_async(self, df: pd.DataFrame, symbol: str, output_dir: str, overwrite: bool = False) -> Optional[str]:
        """
        DEPRECATED: This method writes individual symbol files.
        Use write_date_parquet() instead for consolidated daily files.
        
        Kept for backward compatibility only.
        """
        self.logger.warning(f"write_async() is deprecated. Use write_date_parquet() for consolidated files.")
        
        if df is None or len(df) == 0:
            self.logger.warning(f"No data to write for {symbol}")
            return None
        
        try:
            # Get date from data
            date = df['date'].iloc[0]  # Format: YYYYMMDD
            
            # Create year/month directory structure
            output_path = self._get_date_folder_path(output_dir, date)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Prepare file path
            file_path = output_path / f"{symbol}_{date}.parquet"
            
            # Check if file exists and overwrite is disabled
            if file_path.exists() and not overwrite:
                self.logger.warning(f"File already exists, skipping: {file_path}")
                return None
            
            # Run the actual write operation in a thread pool since it's I/O bound
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: df.to_parquet(
                    file_path,
                    engine='pyarrow',
                    compression=self.parquet_settings['compression'],
                    index=False
                )
            )
            
            # Log success
            file_size = calculate_file_size_mb(str(file_path))
            year = date[:4]
            month = date[4:6]
            self.logger.info(
                f"Wrote {year}/{month}/{symbol}_{date}.parquet: {len(df)} rows, {file_size:.2f} MB"
            )
            
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Failed to write parquet for {symbol}: {e}")
            return None

    async def read_async(self, date: str, output_dir: str) -> Optional[pd.DataFrame]:
        """Async version of read operation with year/month folder structure."""
        date_path = self._get_date_folder_path(output_dir, date)
        file_path = date_path / f"{date}.parquet"
        
        if not file_path.exists():
            return None
        
        try:
            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(None, pd.read_parquet, file_path)
            return df
        except Exception as e:
            self.logger.error(f"Failed to read parquet for {date}: {e}")
            return None
    
    def append_to_metadata_index(self, date: str, symbol_count: int, 
                                 row_count: int, file_size_mb: float,
                                 metadata_dir: str) -> bool:
        """
        Append entry to metadata index.
        
        Args:
            date: Date in YYYYMMDD format
            symbol_count: Number of symbols in file
            row_count: Number of rows in file
            file_size_mb: File size in MB
            metadata_dir: Directory for metadata files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            metadata_path = Path(metadata_dir)
            metadata_path.mkdir(parents=True, exist_ok=True)
            
            index_file = metadata_path / "parquet_index.parquet"
            
            # Create new entry
            new_entry = pd.DataFrame([{
                'date': date,
                'symbol_count': symbol_count,
                'row_count': row_count,
                'file_size_mb': file_size_mb,
                'created_at': pd.Timestamp.now()
            }])
            
            # Append or create index
            if index_file.exists():
                existing = pd.read_parquet(index_file)
                # Remove existing entry for this date if present
                existing = existing[existing['date'] != date]
                updated = pd.concat([existing, new_entry], ignore_index=True)
            else:
                updated = new_entry
            
            # Sort by date
            updated = updated.sort_values('date')
            
            # Write updated index
            updated.to_parquet(index_file, index=False)
            
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to update metadata index: {e}")
            return False
    
    def get_available_dates(self, metadata_dir: str) -> list:
        """
        Get list of available dates from metadata index.
        
        Args:
            metadata_dir: Directory for metadata files
            
        Returns:
            List of dates in YYYYMMDD format
        """
        try:
            index_file = Path(metadata_dir) / "parquet_index.parquet"
            
            if not index_file.exists():
                return []
            
            df = pd.read_parquet(index_file)
            return df['date'].tolist()
        
        except Exception as e:
            self.logger.error(f"Failed to read metadata index: {e}")
            return []
    
    def organize_existing_files(self, source_dir: str, dry_run: bool = True) -> Dict[str, int]:
        """
        Organize existing parquet files into YYYY/MM structure and delete non-trading day files.
        
        Args:
            source_dir: Directory containing flat parquet files
            dry_run: If True, only simulate the changes without actually moving/deleting files
            
        Returns:
            Dictionary with statistics about the operation
        """
        from utils.trading_calendar import TradingCalendar
        import shutil
        import re
        
        source_path = Path(source_dir)
        if not source_path.exists():
            self.logger.error(f"Source directory does not exist: {source_dir}")
            return {"error": "Source directory not found"}
        
        # Initialize trading calendar
        calendar = TradingCalendar(logger=self.logger)
        
        # Statistics
        stats = {
            "files_found": 0,
            "files_moved": 0,
            "files_deleted_non_trading": 0,
            "files_skipped_invalid": 0,
            "files_already_organized": 0,
            "errors": 0
        }
        
        # Pattern to match parquet files: SYMBOL_YYYYMMDD.parquet or YYYYMMDD.parquet
        pattern = re.compile(r'^(.+?)_(\d{8})\.parquet$|^(\d{8})\.parquet$')
        
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}Scanning {source_dir} for parquet files...")
        
        # Get all parquet files in source directory (flat structure only)
        parquet_files = [f for f in source_path.iterdir() if f.is_file() and f.suffix == '.parquet']
        stats["files_found"] = len(parquet_files)
        
        self.logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        for file_path in parquet_files:
            try:
                filename = file_path.name
                match = pattern.match(filename)
                
                if not match:
                    self.logger.warning(f"Skipping file with invalid name format: {filename}")
                    stats["files_skipped_invalid"] += 1
                    continue
                
                # Extract date from filename
                if match.group(2):  # SYMBOL_YYYYMMDD.parquet format
                    symbol = match.group(1)
                    date = match.group(2)
                else:  # YYYYMMDD.parquet format
                    date = match.group(3)
                    symbol = None
                
                # Validate date format
                try:
                    datetime.strptime(date, '%Y%m%d')
                except ValueError:
                    self.logger.warning(f"Invalid date in filename: {filename}")
                    stats["files_skipped_invalid"] += 1
                    continue
                
                # Check if date is a trading day
                is_trading = calendar.is_trading_day(date)
                
                if not is_trading:
                    # Delete file for non-trading day
                    if dry_run:
                        self.logger.info(f"[DRY RUN] Would delete non-trading day file: {filename}")
                    else:
                        file_path.unlink()
                        self.logger.info(f"Deleted non-trading day file: {filename}")
                    stats["files_deleted_non_trading"] += 1
                    continue
                
                # Create destination folder structure
                dest_folder = self._get_date_folder_path(source_dir, date)
                dest_file = dest_folder / filename
                
                # Check if file is already in correct location
                if file_path == dest_file:
                    self.logger.debug(f"File already organized: {filename}")
                    stats["files_already_organized"] += 1
                    continue
                
                # Move file to organized structure
                if dry_run:
                    self.logger.info(f"[DRY RUN] Would move: {filename} -> {dest_folder.relative_to(source_path)}/{filename}")
                else:
                    # Create destination folder
                    dest_folder.mkdir(parents=True, exist_ok=True)
                    
                    # Check if destination file already exists
                    if dest_file.exists():
                        self.logger.warning(f"Destination file already exists, skipping: {dest_file}")
                        stats["files_skipped_invalid"] += 1
                        continue
                    
                    # Move file
                    shutil.move(str(file_path), str(dest_file))
                    self.logger.info(f"Moved: {filename} -> {dest_folder.relative_to(source_path)}/{filename}")
                
                stats["files_moved"] += 1
                
            except Exception as e:
                self.logger.error(f"Error processing {file_path.name}: {e}", exc_info=True)
                stats["errors"] += 1
        
        # Summary
        self.logger.info("=" * 60)
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}Organization Summary:")
        self.logger.info(f"  Files found:                {stats['files_found']}")
        self.logger.info(f"  Files moved:                {stats['files_moved']}")
        self.logger.info(f"  Non-trading day deleted:    {stats['files_deleted_non_trading']}")
        self.logger.info(f"  Already organized:          {stats['files_already_organized']}")
        self.logger.info(f"  Skipped (invalid):          {stats['files_skipped_invalid']}")
        self.logger.info(f"  Errors:                     {stats['errors']}")
        self.logger.info("=" * 60)
        
        if dry_run:
            self.logger.info("This was a DRY RUN. No files were actually moved or deleted.")
            self.logger.info("Run with dry_run=False to perform the actual operation.")
        
        return stats
    
    def validate_organized_structure(self, base_dir: str) -> Dict[str, any]:
        """
        Validate the organized folder structure and report statistics.
        
        Args:
            base_dir: Base directory containing organized files
            
        Returns:
            Dictionary with validation results
        """
        from utils.trading_calendar import TradingCalendar
        
        base_path = Path(base_dir)
        calendar = TradingCalendar(logger=self.logger)
        
        stats = {
            "total_files": 0,
            "valid_trading_days": 0,
            "invalid_trading_days": 0,
            "dates_found": set(),
            "symbols_found": set(),
            "issues": []
        }
        
        self.logger.info(f"Validating structure in {base_dir}...")
        
        # Scan all parquet files recursively
        for file_path in base_path.rglob("*.parquet"):
            stats["total_files"] += 1
            
            # Extract date from path (YYYY/MM structure)
            parts = file_path.relative_to(base_path).parts
            if len(parts) >= 3:  # YYYY/MM/file.parquet
                year, month = parts[0], parts[1]
                # Extract day from filename
                filename = file_path.stem  # without .parquet
                if len(filename) == 8 and filename.isdigit():  # YYYYMMDD.parquet
                    date_str = filename
                elif '_' in filename:  # SYMBOL_YYYYMMDD.parquet
                    date_str = filename.split('_')[-1]
                else:
                    continue
                
                # Validate date
                if calendar.is_trading_day(date_str):
                    stats["valid_trading_days"] += 1
                    stats["dates_found"].add(date_str)
                else:
                    stats["invalid_trading_days"] += 1
                    stats["issues"].append(f"Non-trading day file: {file_path.relative_to(base_path)}")
                
                # Extract symbol from filename if present
                if '_' in filename:
                    symbol = filename.rsplit('_', 1)[0]
                    stats["symbols_found"].add(symbol)
        
        # Convert sets to sorted lists for reporting
        stats["dates_found"] = sorted(list(stats["dates_found"]))
        stats["symbols_found"] = sorted(list(stats["symbols_found"]))
        stats["unique_dates"] = len(stats["dates_found"])
        stats["unique_symbols"] = len(stats["symbols_found"])
        
        # Report
        self.logger.info("=" * 60)
        self.logger.info("Validation Results:")
        self.logger.info(f"  Total files:           {stats['total_files']}")
        self.logger.info(f"  Valid trading days:    {stats['valid_trading_days']}")
        self.logger.info(f"  Invalid trading days:  {stats['invalid_trading_days']}")
        self.logger.info(f"  Unique dates:          {stats['unique_dates']}")
        self.logger.info(f"  Unique symbols:        {stats['unique_symbols']}")
        
        if stats["issues"]:
            self.logger.warning(f"  Found {len(stats['issues'])} issues:")
            for issue in stats["issues"][:10]:  # Show first 10
                self.logger.warning(f"    - {issue}")
            if len(stats["issues"]) > 10:
                self.logger.warning(f"    ... and {len(stats['issues']) - 10} more")
        
        self.logger.info("=" * 60)
        
        return stats