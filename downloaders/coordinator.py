"""
Download coordinator - OPTIMIZED with concurrent pipeline processing.
Each symbol flows through fetch→parse→validate pipeline independently.
"""
import pandas as pd
import json
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import time
import uuid
import traceback
from collections import deque

from config.settings import Config
from connectors.alpaca_api import AlpacaAPI  # ✅ FIXED: Changed from ThetaDataAPI
from processors.data_parser import DataParser
from storage.parquet_writer import ParquetWriter
from utils.helpers import get_trading_dates, calculate_file_size_mb

# ✅ Progress bar support
try:
    from tqdm.asyncio import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Fallback: no-op progress bar
    class tqdm:
        def __init__(self, *args, **kwargs):
            self.total = kwargs.get('total', 0)
            self.n = 0
        def update(self, n=1):
            self.n += n
        def set_postfix_str(self, s):
            pass
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass


class DownloadCoordinator:
    """Coordinate download - Concurrent pipeline: fetch→parse→validate per symbol."""
    
    def __init__(self, config: Config, api: AlpacaAPI, parser: DataParser,  # ✅ FIXED: Changed type hint
                 writer: ParquetWriter, logger: logging.Logger):
        """Initialize coordinator with dependencies."""
        self.config = config
        self.api = api
        self.parser = parser
        self.writer = writer
        self.logger = logger
        
        from utils.trading_calendar import TradingCalendar
        self.trading_calendar = TradingCalendar(logger=logger)

    async def download_date_range(self, start_date: str, end_date: str,
                                symbols: List[str], output_dir: str) -> Dict:
        """
        OPTIMIZED: Concurrent pipeline processing for maximum throughput.
        """
        try:
            download_start_time = time.time()
        
            sorted_symbols = sorted(symbols)
            dates, skipped_dates = self.trading_calendar.get_trading_days_with_skipped(start_date, end_date)
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"OPTIMIZED CONCURRENT PIPELINE")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Strategy: Concurrent fetch→parse→validate per symbol")
            self.logger.info(f"Date range: {start_date} to {end_date}")
            self.logger.info(f"Trading days: {len(dates)}")
            self.logger.info(f"Symbols: {len(sorted_symbols)}")
            self.logger.info(f"Output: ONE parquet per day")
            self.logger.info(f"Concurrency: {self.config.MAX_CONCURRENT_REQUESTS} symbols")
            if HAS_TQDM:
                self.logger.info(f"Progress: Real-time stats with ETA enabled ✓")
            else:
                self.logger.warning(f"Progress: Install 'tqdm' for real-time stats")
        
            if skipped_dates:
                self.logger.info(f"\nSkipped dates:")
                for date, reason in skipped_dates:
                    self.logger.info(f"  ⊗ {date}: {reason}")
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"STARTING DOWNLOADS")
            self.logger.info(f"{'='*60}")
        
            results = {
                "dates_processed": 0,
                "dates_successful": 0,
                "dates_failed": 0,
                "total_symbols_downloaded": 0,
                "total_records": 0,
                "total_file_size_mb": 0,
                "total": len(sorted_symbols) * len(dates),
                "successful": 0,
                "failed": 0,
                "no_data": 0,
                "skipped": 0
            }
        
            for date_idx, date in enumerate(dates, 1):
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"Date {date_idx}/{len(dates)}: {date}")
                self.logger.info(f"{'='*60}")
            
                date_result = await self._process_day_concurrent(date, sorted_symbols, output_dir)
            
                results["dates_processed"] += 1
                if date_result["success"]:
                    results["dates_successful"] += 1
                    results["total_symbols_downloaded"] += date_result["symbols_count"]
                    results["total_records"] += date_result["records"]
                    results["total_file_size_mb"] += date_result["file_size_mb"]
                    results["successful"] += date_result["symbols_count"]
                    
                    if date_result.get("skipped"):
                        results["skipped"] += date_result["symbols_count"]
                else:
                    results["dates_failed"] += 1
                    results["failed"] += len(sorted_symbols)
            
                progress_pct = (date_idx / len(dates)) * 100
                self.logger.info(f"Progress: {date_idx}/{len(dates)} ({progress_pct:.1f}%)")

            total_duration = time.time() - download_start_time
        
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"COMPLETED")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Duration: {total_duration:.2f}s ({total_duration/60:.2f} min)")
            self.logger.info(f"Days successful: {results['dates_successful']}")
            self.logger.info(f"Total records: {results['total_records']:,}")
            self.logger.info(f"Total size: {results['total_file_size_mb']:.2f} MB")
            
            # ✅ Calculate and display throughput stats
            if total_duration > 0:
                symbols_per_sec = results["total_symbols_downloaded"] / total_duration
                records_per_sec = results["total_records"] / total_duration
                mb_per_sec = results["total_file_size_mb"] / total_duration
                
                self.logger.info(f"\nTHROUGHPUT:")
                self.logger.info(f"  Symbols/sec: {symbols_per_sec:.2f}")
                self.logger.info(f"  Records/sec: {records_per_sec:,.0f}")
                self.logger.info(f"  MB/sec: {mb_per_sec:.2f}")
            
            self.logger.info(f"{'='*60}\n")
        
            return results
        
        except asyncio.CancelledError:
            self.logger.warning("Download cancelled by user or system")
            raise
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            return self._build_error_summary(symbols, str(e))

    def _format_eta(self, seconds: float) -> str:
        """Format ETA in human-readable format."""
        if seconds < 0 or seconds > 86400:  # More than 24 hours
            return "N/A"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours}h{minutes:02d}m{secs:02d}s"
        elif minutes > 0:
            return f"{minutes}m{secs:02d}s"
        else:
            return f"{secs}s"

    async def _process_day_concurrent(self, date: str, symbols: List[str], 
                                      output_dir: str) -> Dict:
        """
        OPTIMIZED: Process all symbols for a day using concurrent pipeline.
        Each symbol goes through: fetch → parse → validate independently.
        """
        year, month = date[:4], date[4:6]
        date_path = Path(output_dir) / year / month
        file_path = date_path / f"{date}.parquet"
        
        try:
            # Check if file exists
            if file_path.exists():
                self.logger.info(f"[{date}] File exists, skipping")
                try:
                    df = pd.read_parquet(file_path)
                    file_size = file_path.stat().st_size / (1024 * 1024)
                    return {
                        "success": True,
                        "date": date,
                        "symbols_count": df['symbol'].nunique(),
                        "records": len(df),
                        "file_size_mb": file_size,
                        "skipped": True
                    }
                except Exception as e:
                    self.logger.warning(f"[{date}] Error reading existing file: {e}")
        
            # ✅ CONCURRENT PIPELINE: Process all symbols in parallel
            self.logger.info(f"[{date}] Starting concurrent pipeline for {len(symbols)} symbols...")
            pipeline_start = time.time()
            
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_REQUESTS)
            rate_limit_delay = getattr(self.config, 'RATE_LIMIT_DELAY', 0.0)
            
            # Track statistics
            stats = {
                "downloaded": 0,
                "parsed": 0,
                "validated": 0,
                "skipped": 0,
                "failed": 0,
                "total_bytes": 0,
                "total_records": 0
            }
            stats_lock = asyncio.Lock()
            
            # Collect valid dataframes
            valid_dataframes = []
            df_lock = asyncio.Lock()
            
            # ✅ Create progress bar with ETA
            pbar = tqdm(
                total=len(symbols),
                desc=f"[{date}] Processing",
                unit="symbol",
                ncols=140,  # Wider to fit ETA
                disable=not HAS_TQDM,
                leave=False,
                dynamic_ncols=True  # Auto-adjust width
            )
            
            async def process_symbol_pipeline(symbol: str):
                """Complete pipeline for one symbol: fetch → parse → validate."""
                async with semaphore:
                    try:
                        # STAGE 1: Check if should skip
                        if hasattr(self.parser, 'null_tracker') and self.parser.null_tracker:
                            should_skip, reason = self.parser.null_tracker.should_skip_symbol(symbol, date)
                            if should_skip:
                                async with stats_lock:
                                    stats["skipped"] += 1
                                    pbar.update(1)
                                    # Update progress bar with stats + ETA
                                    elapsed = time.time() - pipeline_start
                                    completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                                    rate = completed / elapsed if elapsed > 0 else 0
                                    remaining = len(symbols) - completed
                                    eta_seconds = remaining / rate if rate > 0 else 0
                                    eta_str = self._format_eta(eta_seconds)
                                    
                                    pbar.set_postfix_str(
                                        f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                        f"{rate:.1f}/s | ETA: {eta_str}"
                                    )
                                self.logger.debug(f"[{symbol}] [{date}] Skipped: {reason}")
                                return
                        
                        # STAGE 2: Fetch data from API
                        fetch_start = time.monotonic()
                        raw_data = await self.api.fetch_market_data(
                            symbol,
                            date,
                            start_time=self.config.START_TIME,
                            end_time=self.config.END_TIME
                        )
                        fetch_duration = time.monotonic() - fetch_start
                        
                        if not raw_data:
                            async with stats_lock:
                                stats["failed"] += 1
                                pbar.update(1)
                                # Update stats + ETA
                                elapsed = time.time() - pipeline_start
                                completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                                rate = completed / elapsed if elapsed > 0 else 0
                                remaining = len(symbols) - completed
                                eta_seconds = remaining / rate if rate > 0 else 0
                                eta_str = self._format_eta(eta_seconds)
                                
                                pbar.set_postfix_str(
                                    f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                    f"{rate:.1f}/s | ETA: {eta_str}"
                                )
                            self.logger.debug(f"[{symbol}] [{date}] No data returned")
                            return
                        
                        async with stats_lock:
                            stats["downloaded"] += 1
                        
                        # Rate limiting after fetch
                        if rate_limit_delay > 0:
                            await asyncio.sleep(rate_limit_delay)
                        
                        # STAGE 3: Parse data (offload to thread to avoid blocking event loop)
                        parse_start = time.monotonic()
                        try:
                            parsed_df = await asyncio.to_thread(
                                self.parser.parse_market_data,
                                raw_data,
                                symbol
                            )
                        except Exception as parse_error:
                            async with stats_lock:
                                stats["failed"] += 1
                                pbar.update(1)
                                elapsed = time.time() - pipeline_start
                                completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                                rate = completed / elapsed if elapsed > 0 else 0
                                remaining = len(symbols) - completed
                                eta_seconds = remaining / rate if rate > 0 else 0
                                eta_str = self._format_eta(eta_seconds)
                                
                                pbar.set_postfix_str(
                                    f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                    f"{rate:.1f}/s | ETA: {eta_str}"
                                )
                            self.logger.debug(f"[{symbol}] [{date}] Parse error: {parse_error}")
                            return
                        
                        parse_duration = time.monotonic() - parse_start
                        
                        if parsed_df is None or len(parsed_df) == 0:
                            async with stats_lock:
                                stats["failed"] += 1
                                pbar.update(1)
                                elapsed = time.time() - pipeline_start
                                completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                                rate = completed / elapsed if elapsed > 0 else 0
                                remaining = len(symbols) - completed
                                eta_seconds = remaining / rate if rate > 0 else 0
                                eta_str = self._format_eta(eta_seconds)
                                
                                pbar.set_postfix_str(
                                    f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                    f"{rate:.1f}/s | ETA: {eta_str}"
                                )
                            self.logger.debug(f"[{symbol}] [{date}] Parse returned empty")
                            return
                        
                        async with stats_lock:
                            stats["parsed"] += 1
                        
                        # STAGE 4: Validation happens in parser.parse_market_data
                        # If we got here, data is valid
                        async with stats_lock:
                            stats["validated"] += 1
                            stats["total_records"] += len(parsed_df)
                            # Estimate size
                            stats["total_bytes"] += len(parsed_df) * 100  # Rough estimate: 100 bytes/record
                        
                        # Add to collection
                        async with df_lock:
                            valid_dataframes.append(parsed_df)
                        
                        # ✅ Update progress bar with detailed stats + ETA
                        async with stats_lock:
                            pbar.update(1)
                            elapsed = time.time() - pipeline_start
                            completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                            rate = completed / elapsed if elapsed > 0 else 0
                            remaining = len(symbols) - completed
                            eta_seconds = remaining / rate if rate > 0 else 0
                            eta_str = self._format_eta(eta_seconds)
                            mb = stats["total_bytes"] / (1024 * 1024)
                            
                            pbar.set_postfix_str(
                                f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                f"{rate:.1f}/s | {stats['total_records']:,} rec | {mb:.1f}MB | ETA: {eta_str}"
                            )
                        
                        total_duration = fetch_duration + parse_duration
                        self.logger.debug(
                            f"[{symbol}] [{date}] ✓ Complete "
                            f"(fetch: {fetch_duration:.2f}s, parse: {parse_duration:.2f}s, "
                            f"records: {len(parsed_df)})"
                        )
                        
                    except asyncio.CancelledError:
                        self.logger.debug(f"[{symbol}] [{date}] Cancelled")
                        raise
                    except Exception as e:
                        async with stats_lock:
                            stats["failed"] += 1
                            pbar.update(1)
                            elapsed = time.time() - pipeline_start
                            completed = stats["downloaded"] + stats["skipped"] + stats["failed"]
                            rate = completed / elapsed if elapsed > 0 else 0
                            remaining = len(symbols) - completed
                            eta_seconds = remaining / rate if rate > 0 else 0
                            eta_str = self._format_eta(eta_seconds)
                            
                            pbar.set_postfix_str(
                                f"✓{stats['validated']} ⊘{stats['skipped']} ✗{stats['failed']} | "
                                f"{rate:.1f}/s | ETA: {eta_str}"
                            )
                        self.logger.debug(f"[{symbol}] [{date}] Pipeline error: {type(e).__name__}")
            
            # ✅ Run pipeline for all symbols concurrently
            tasks = [asyncio.create_task(process_symbol_pipeline(sym)) for sym in symbols]
            
            try:
                # Wait for all pipelines to complete
                await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                self.logger.warning(f"[{date}] Pipeline cancelled, cleaning up tasks")
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            finally:
                # Close progress bar
                pbar.close()
            
            pipeline_duration = time.time() - pipeline_start
            
            # ✅ Calculate and log throughput statistics
            symbols_per_sec = (stats["downloaded"] + stats["skipped"] + stats["failed"]) / pipeline_duration if pipeline_duration > 0 else 0
            records_per_sec = stats["total_records"] / pipeline_duration if pipeline_duration > 0 else 0
            mb_per_sec = (stats["total_bytes"] / (1024 * 1024)) / pipeline_duration if pipeline_duration > 0 else 0
            
            self.logger.info(
                f"[{date}] Pipeline complete in {pipeline_duration:.2f}s - "
                f"Downloaded: {stats['downloaded']}, "
                f"Parsed: {stats['parsed']}, "
                f"Valid: {stats['validated']}, "
                f"Skipped: {stats['skipped']}, "
                f"Failed: {stats['failed']}"
            )
            
            self.logger.info(
                f"[{date}] Throughput: {symbols_per_sec:.1f} sym/s, "
                f"{records_per_sec:,.0f} rec/s, {mb_per_sec:.2f} MB/s"
            )
            
            if not valid_dataframes:
                self.logger.warning(f"[{date}] No valid data after pipeline")
                return {
                    "success": False,
                    "date": date,
                    "symbols_count": 0,
                    "records": 0,
                    "file_size_mb": 0,
                    "error": "No valid data"
                }
            
            # FINAL STAGE: Combine and write
            self.logger.info(f"[{date}] Combining {len(valid_dataframes)} dataframes...")
            write_start = time.time()
            
            combined_df = pd.concat(valid_dataframes, ignore_index=True)
            combined_df = combined_df.sort_values(['symbol', 'timestamp'])
            
            # Offload parquet write to thread
            file_path_str = await asyncio.to_thread(
                self.writer.write_date_parquet,
                date,
                combined_df,
                output_dir,
                overwrite=False
            )
            
            write_duration = time.time() - write_start
            
            if file_path_str:
                file_size = Path(file_path_str).stat().st_size / (1024 * 1024)
                total_duration = pipeline_duration + write_duration
                
                self.logger.info(
                    f"[{date}] ✓ Complete in {total_duration:.2f}s "
                    f"(Pipeline: {pipeline_duration:.1f}s, Write: {write_duration:.1f}s, "
                    f"Symbols: {stats['validated']}, Records: {len(combined_df):,})"
                )
                
                return {
                    "success": True,
                    "date": date,
                    "symbols_count": combined_df['symbol'].nunique(),
                    "records": len(combined_df),
                    "file_size_mb": file_size,
                    "skipped": False
                }
            else:
                return {
                    "success": False,
                    "date": date,
                    "symbols_count": 0,
                    "records": 0,
                    "file_size_mb": 0,
                    "error": "Write failed"
                }
                
        finally:
            # Perform daily reset of MCP client
            try:
                if hasattr(self.api, "reset") and callable(getattr(self.api, "reset")):
                    await self.api.reset(force_reconnect=False)
            except Exception as ex:
                self.logger.debug(f"[{date}] MCP client daily reset failed: {ex}")

    def _build_error_summary(self, symbols: List[str], error_message: str) -> Dict:
        """Build error summary."""
        return {
            "total": len(symbols),
            "successful": 0,
            "failed": len(symbols),
            "no_data": 0,
            "total_records": 0,
            "dates_processed": 0,
            "dates_successful": 0,
            "dates_failed": 0,
            "error": error_message
        }
