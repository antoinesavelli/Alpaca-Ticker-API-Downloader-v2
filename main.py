"""
Main entry point for Alpaca data pipeline.
Supports both standard and bulk download modes.
"""
import asyncio
import logging
from pathlib import Path
from config.settings import Config
from connectors.alpaca_api import AlpacaAPI
from processors.data_parser import DataParser
from storage.parquet_writer import ParquetWriter
from downloaders.coordinator import DownloadCoordinator
from downloaders.bulk_downloader import BulkDownloader
from utils.logger import setup_logger
import pandas as pd


async def main():
    """Main execution function."""
    config = Config()
    
    try:
        config.validate()
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return
    
    logger = setup_logger(
        'alpaca_pipeline',
        config.LOG_FILE,
        config.LOG_LEVEL
    )
    
    logger.info("="*80)
    logger.info("ALPACA MARKET DATA PIPELINE")
    logger.info("="*80)
    
    # Check for split-only mode (environment variable or command line arg)
    import os
    split_only_mode = os.getenv('SPLIT_ONLY', 'false').lower() == 'true'
    
    if split_only_mode:
        logger.info("MODE: SPLIT-ONLY (processing existing master files)")
        
        # Initialize minimal components
        api = AlpacaAPI(config, logger)
        parser = DataParser(logger, output_dir=config.OUTPUT_DIR, config=config)
        
        try:
            bulk_downloader = BulkDownloader(config, api, parser, logger)
            summary = await bulk_downloader.split_all_pending()
            
            logger.info("\n" + "="*80)
            logger.info("SPLIT-ONLY SUMMARY")
            logger.info("="*80)
            logger.info(f"Months processed: {summary['months_split']}")
            logger.info("="*80)
            
        finally:
            await api.close()
        
        return
    
    # Normal download mode continues...
    logger.info(f"Feed: {config.FEED} (SIP = 100% market coverage)")
    logger.info(f"Date Range: {config.START_DATE} to {config.END_DATE}")
    logger.info(f"Timeframe: {config.TIMEFRAME}")
    
    # Determine mode
    if config.BULK_DOWNLOAD_MODE:
        logger.info(f"Mode: BULK DOWNLOAD (optimized)")
        logger.info(f"  Symbols per batch: {config.SYMBOLS_PER_BATCH}")
        logger.info(f"  Days per batch: {config.DAYS_PER_BATCH}")
        logger.info(f"  Master format: {config.MASTER_FILE_FORMAT}")
    else:
        logger.info(f"Mode: STANDARD (per-symbol downloads)")
    
    # Load symbols
    try:
        symbols_df = pd.read_csv(config.SYMBOL_FILE)
        if 'symbol' in symbols_df.columns:
            symbols = symbols_df['symbol'].tolist()
        elif 'Symbol' in symbols_df.columns:
            symbols = symbols_df['Symbol'].tolist()
        else:
            symbols = symbols_df.iloc[:, 0].tolist()
        
        symbols = [str(s).strip().upper() for s in symbols if pd.notna(s)]
        logger.info(f"Loaded {len(symbols)} symbols from {config.SYMBOL_FILE}")
        
        if len(symbols) > 10:
            logger.info(f"First 10: {', '.join(symbols[:10])}")
        
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        return
    
    # Initialize components
    api = AlpacaAPI(config, logger)
    parser = DataParser(logger, output_dir=config.OUTPUT_DIR, config=config)
    
    # Check connection
    logger.info("\nChecking Alpaca API connection...")
    
    if not await api.check_terminal_running():
        logger.error("❌ Alpaca API is not accessible")
        logger.error("Check:")
        logger.error("  1. ALPACA_API_KEY is set correctly")
        logger.error("  2. ALPACA_SECRET_KEY is set correctly")
        logger.error("  3. Internet connection is active")
        logger.error("  4. Alpaca subscription includes SIP data feed")
        return
    
    logger.info("✓ Alpaca API connection verified")
    
    # Choose download mode
    try:
        if config.BULK_DOWNLOAD_MODE:
            # BULK MODE - Optimized for maximum API efficiency
            bulk_downloader = BulkDownloader(config, api, parser, logger)
            
            summary = await bulk_downloader.download_bulk(
                symbols=symbols,
                start_date=config.START_DATE,
                end_date=config.END_DATE
            )
            
            logger.info("\n" + "="*80)
            logger.info("BULK DOWNLOAD SUMMARY")
            logger.info("="*80)
            logger.info(f"Duration: {summary.get('duration_seconds', 0)/60:.2f} minutes")
            logger.info(f"Batches completed: {summary['completed_batches']}")
            logger.info(f"Batches failed: {summary['failed_batches']}")
            logger.info(f"Total records: {summary['total_records']:,}")
            logger.info(f"Total symbols: {summary['total_symbols']}")
            logger.info("="*80)
            
        else:
            # STANDARD MODE - One symbol at a time
            writer = ParquetWriter(config, logger)
            coordinator = DownloadCoordinator(config, api, parser, writer, logger)
            
            summary = await coordinator.download_date_range(
                start_date=config.START_DATE,
                end_date=config.END_DATE,
                symbols=symbols,
                output_dir=config.OUTPUT_DIR
            )
            
            logger.info("\n" + "="*80)
            logger.info("DOWNLOAD SUMMARY")
            logger.info("="*80)
            logger.info(f"Dates processed: {summary['dates_processed']}")
            logger.info(f"Records: {summary['total_records']:,}")
            logger.info(f"Size: {summary['total_file_size_mb']:.2f} MB")
            logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Download interrupted by user")
        logger.info("Progress has been saved - you can resume by running again")
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
    finally:
        await api.close()
        logger.info("\n✓ Pipeline completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
    
    input("\nPress Enter to exit...")