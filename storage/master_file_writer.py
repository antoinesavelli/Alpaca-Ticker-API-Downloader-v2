"""
Master file writer for streaming bulk downloads.
Uses Arrow IPC format for fast, low-memory streaming writes.
"""
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import logging
from pathlib import Path
from typing import Optional


class MasterFileWriter:
    """
    Streaming writer for master data file - OPTIMIZED.
    """
    
    def __init__(self, file_path: str, file_format: str, logger: logging.Logger):
        """
        Initialize master file writer.
        
        Args:
            file_path: Path to master file
            file_format: Format ('arrow', 'csv', 'parquet')
            logger: Logger instance
        """
        self.file_path = Path(file_path)
        self.file_format = file_format.lower()
        self.logger = logger
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.total_records = 0
        
        # For arrow streaming
        self.arrow_writer = None
        self.arrow_file = None
        self.arrow_schema = None
        
        # IMPORTANT: Arrow IPC files cannot be appended to!
        # We must write everything in one session and close properly
        if self.file_format == 'arrow':
            # Always open in write mode (not append)
            # Arrow IPC requires proper header/footer structure
            self.arrow_file = open(self.file_path, 'wb')
            
        self.logger.info(f"Master file writer initialized: {self.file_path}")
        self.logger.info(f"Format: {self.file_format}")
    
    def append_data(self, df: pd.DataFrame) -> bool:
        """
        Append data to master file (streaming).
        
        Args:
            df: DataFrame to append
            
        Returns:
            bool: True if successful
        """
        if df is None or len(df) == 0:
            return True
        
        try:
            if self.file_format == 'arrow':
                return self._append_arrow_streaming(df)
            elif self.file_format == 'csv':
                return self._append_csv(df)
            elif self.file_format == 'parquet':
                return self._append_parquet(df)
            else:
                raise ValueError(f"Unsupported format: {self.file_format}")
        except Exception as e:
            self.logger.error(f"Failed to append data: {e}", exc_info=True)
            return False
    
    def _append_arrow_streaming(self, df: pd.DataFrame) -> bool:
        """TRUE streaming append using Arrow IPC RecordBatchFileWriter."""
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
            
            # Initialize writer on first write
            if self.arrow_writer is None:
                self.arrow_schema = table.schema
                # Use RecordBatchFileWriter for proper format
                self.arrow_writer = ipc.new_file(self.arrow_file, self.arrow_schema)
                self.logger.debug(f"Initialized Arrow writer with schema: {self.arrow_schema}")
            
            # Validate schema matches
            if table.schema != self.arrow_schema:
                self.logger.warning(f"Schema mismatch detected, attempting to cast...")
                table = table.cast(self.arrow_schema)
            
            # Write table (streaming)
            self.arrow_writer.write_table(table)
            
            self.total_records += len(df)
            self.logger.debug(f"Streamed {len(df):,} rows (total: {self.total_records:,})")
            return True
            
        except Exception as e:
            self.logger.error(f"Arrow streaming error: {e}", exc_info=True)
            return False
    
    def _append_csv(self, df: pd.DataFrame) -> bool:
        """Append to CSV file."""
        try:
            # Check if file exists to determine if we need header
            write_header = not self.file_path.exists() or self.file_path.stat().st_size == 0
            
            df.to_csv(
                self.file_path,
                mode='a',
                header=write_header,
                index=False
            )
            
            self.total_records += len(df)
            self.logger.debug(f"Appended {len(df):,} rows to CSV (total: {self.total_records:,})")
            return True
            
        except Exception as e:
            self.logger.error(f"CSV append error: {e}")
            return False
    
    def _append_parquet(self, df: pd.DataFrame) -> bool:
        """Append to Parquet (via re-read - not ideal but works)."""
        try:
            import pyarrow.parquet as pq
            
            table = pa.Table.from_pandas(df, preserve_index=False)
            
            if self.file_path.exists():
                # Read existing and concatenate
                existing = pq.read_table(self.file_path)
                table = pa.concat_tables([existing, table])
            
            # Write combined table
            pq.write_table(table, self.file_path)
            
            self.total_records += len(df)
            self.logger.debug(f"Appended {len(df):,} rows to Parquet (total: {self.total_records:,})")
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet append error: {e}")
            return False
    
    def close(self):
        """Close the writer and file handles - CRITICAL for Arrow IPC format!"""
        try:
            if self.arrow_writer:
                # MUST close writer to finalize IPC format (writes footer)
                self.arrow_writer.close()
                self.arrow_writer = None
                self.logger.debug("Arrow writer closed and finalized")
            if self.arrow_file:
                self.arrow_file.close()
                self.arrow_file = None
                self.logger.debug("Arrow file handle closed")
            
            if self.total_records > 0:
                self.logger.info(f"✓ Master file complete: {self.total_records:,} total records")
                
        except Exception as e:
            self.logger.error(f"Error closing writer: {e}", exc_info=True)
    
    def __del__(self):
        """Cleanup on destruction."""
        self.close()

    def read_all(self) -> Optional[pd.DataFrame]:
        """Read entire master file."""
        if not self.file_path.exists():
            return None
        
        try:
            if self.file_format == 'arrow':
                # Use IPC reader for Arrow files
                with pa.memory_map(str(self.file_path), 'r') as source:
                    reader = ipc.open_file(source)
                    table = reader.read_all()
                    return table.to_pandas()
            elif self.file_format == 'csv':
                return pd.read_csv(self.file_path)
            elif self.file_format == 'parquet':
                return pd.read_parquet(self.file_path)
            else:
                raise ValueError(f"Unsupported format: {self.file_format}")
                
        except Exception as e:
            self.logger.error(f"Failed to read master file: {e}", exc_info=True)
            return None
    
    def get_size_mb(self) -> float:
        """Get file size in MB."""
        if self.file_path.exists():
            return self.file_path.stat().st_size / (1024 * 1024)
        return 0.0
    
    def delete(self):
        """Delete master file."""
        try:
            self.close()  # Ensure closed before deleting
            if self.file_path.exists():
                self.file_path.unlink()
                self.logger.info(f"Deleted master file: {self.file_path}")
        except Exception as e:
            self.logger.error(f"Error deleting master file: {e}")
