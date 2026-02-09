"""
File splitter - converts master file into daily parquet files.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, List
from config.settings import Config
from storage.parquet_writer import ParquetWriter
import time


class FileSplitter:
    """Split master file into daily parquet files."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize file splitter."""
        self.config = config
        self.logger = logger
        self.writer = ParquetWriter(config, logger)
    
    def split_master_to_daily(self, master_file: str, output_dir: str, specific_dates: List[str] = None):
        """
        Split using Arrow for FAST reads/writes.
        
        Args:
            master_file: Path to master file
            output_dir: Output directory for daily files
            specific_dates: Optional list of specific dates (YYYYMMDD) to extract.
                           If None, extracts all dates.
        """
        import pyarrow as pa
        import pyarrow.ipc as ipc
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
        
        if specific_dates:
            self.logger.info(
                f"Reading master file: {master_file} (extracting {len(specific_dates)} specific dates)"
            )
        else:
            self.logger.info(f"Reading master file: {master_file} (extracting all dates)")
        
        # Read the file and immediately close the memory map
        try:
            # Read as Arrow IPC file
            with pa.memory_map(master_file, 'r') as source:
                reader = ipc.open_file(source)
                table = reader.read_all()
            # Memory map is now closed after exiting context
            
        except (OSError, pa.ArrowInvalid) as e:
            self.logger.error(f"Failed to read master file (corrupted): {e}")
            self.logger.error(f"The file {master_file} is corrupted and must be deleted")
            self.logger.error(f"Delete it with: del \"{master_file}\"")
            raise RuntimeError(f"Corrupted master file: {master_file}") from e
        
        self.logger.info(f"Loaded {len(table):,} rows from master file")
        self.logger.debug(f"Schema: {table.schema.names}")
        
        # Check if 'date' column exists, if not create it from 'timestamp'
        if 'date' not in table.schema.names:
            if 'timestamp' not in table.schema.names:
                raise ValueError(
                    f"Neither 'date' nor 'timestamp' column found. "
                    f"Available columns: {table.schema.names}"
                )
            
            self.logger.info("Creating 'date' column from 'timestamp'...")
            
            # Extract date from timestamp
            timestamps = table['timestamp']
            
            # Handle different timestamp formats
            try:
                if pa.types.is_string(timestamps.type):
                    # Parse string timestamps
                    # Try common formats
                    try:
                        # Format: "2024-01-15T09:30:00-05:00" or similar
                        dates = pc.strftime(
                            pc.cast(timestamps, pa.timestamp('us')),
                            format='%Y%m%d'
                        )
                    except:
                        # Fallback: extract first 10 chars and format
                        dates = pc.replace_substring(
                            pc.utf8_slice_codeunits(timestamps, 0, 10),
                            '-', ''
                        )
                elif pa.types.is_timestamp(timestamps.type):
                    # Already timestamp type
                    dates = pc.strftime(timestamps, format='%Y%m%d')
                else:
                    raise ValueError(f"Unsupported timestamp type: {timestamps.type}")
                
                # Add date column to table
                table = table.append_column('date', dates)
                self.logger.info(f"✓ Created 'date' column")
                
            except Exception as e:
                self.logger.error(f"Failed to create date column: {e}")
                # Show sample timestamp for debugging
                sample = timestamps[0].as_py() if len(timestamps) > 0 else "N/A"
                self.logger.error(f"Sample timestamp: {sample}")
                raise
        
        # Get unique dates (filter to specific dates if requested)
        dates_array = pc.unique(table['date']).to_pylist()
        
        if specific_dates:
            # Filter to only requested dates
            dates_list = sorted([d for d in dates_array if d in specific_dates])
            self.logger.info(
                f"Filtering to {len(dates_list)} requested dates "
                f"(found {len(dates_array)} total in master)"
            )
        else:
            dates_list = sorted(dates_array)
            self.logger.info(f"Splitting into {len(dates_list)} daily files...")
        
        dates_written = 0
        total_rows = 0
        
        for date in dates_list:
            try:
                # Filter using PyArrow (vectorized)
                date_mask = pc.equal(table['date'], date)
                date_table = table.filter(date_mask)
                
                if len(date_table) > 0:
                    # Extract YYYY and MM from date (YYYYMMDD format)
                    year = date[:4]
                    month = date[4:6]
                    
                    # Convert Arrow table to pandas for optional column handling
                    df = date_table.to_pandas()
                    
                    # Recalculate cumulative_volume per symbol per day (OPTIONAL)
                    if 'volume' in df.columns and 'symbol' in df.columns:
                        df = df.sort_values(['symbol', 'timestamp'])
                        df['cumulative_volume'] = df.groupby('symbol')['volume'].cumsum()
                    
                    # Ensure optional columns have correct types if present
                    if 'count' in df.columns:
                        df['count'] = df['count'].fillna(0).astype('int64')
                    if 'vwap' in df.columns:
                        df['vwap'] = df['vwap'].astype('float64')
                    if 'cumulative_volume' in df.columns:
                        df['cumulative_volume'] = df['cumulative_volume'].astype('int64')
                    
                    # Convert back to Arrow
                    date_table = pa.Table.from_pandas(df, preserve_index=False)
                    
                    # Create folder structure: YYYY/MM/YYYYMMDD.parquet
                    output_path = Path(output_dir) / year / month / f"{date}.parquet"
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    pq.write_table(
                        date_table, 
                        output_path, 
                        compression=self.config.PARQUET_COMPRESSION
                    )
                    
                    dates_written += 1
                    total_rows += len(date_table)
                    
                    # Log with column info
                    col_names = date_table.schema.names
                    self.logger.debug(
                        f"  ✓ [{date}] {len(date_table):,} rows → {year}/{month}/{date}.parquet "
                        f"({len(col_names)} columns)"
                    )
                
            except Exception as e:
                self.logger.error(f"Failed to write date {date}: {e}")
                continue
        
        self.logger.info(
            f"✓ Split complete: {dates_written} dates, {total_rows:,} total rows"
        )
        
        # IMPORTANT: Clear table reference to release memory and file locks
        del table
        if 'date_table' in locals():
            del date_table
        
        return {
            'dates_written': dates_written,
            'total_rows': total_rows
        }
