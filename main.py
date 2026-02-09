"""
Main entry point for Alpaca data pipeline.
Supports both standard and bulk download modes with exchange symbol loading.
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
from downloaders.symbol_loader import SymbolLoader
from utils.logger import setup_logger


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
    
    # Initialize API
    api = AlpacaAPI(config, logger)
    
    # Load symbols based on configuration
    try:
        symbol_loader = SymbolLoader(logger, api_headers=api.headers)
        
        if config.SYMBOL_SOURCE == 'exchanges':
            logger.info(f"\n{'='*60}")
            logger.info(f"SYMBOL LOADING: EXCHANGE API")
            logger.info(f"{'='*60}")
            symbols = await symbol_loader.load_all_exchange_symbols(
                exchanges=config.EXCHANGES,
                min_avg_volume=config.MIN_AVG_VOLUME
            )
        else:  # csv
            logger.info(f"\n{'='*60}")
            logger.info(f"SYMBOL LOADING: CSV FILE")
            logger.info(f"{'='*60}")
            symbols = symbol_loader.load_symbols(config.SYMBOL_FILE)
            logger.info(f"Loaded {len(symbols)} symbols from {config.SYMBOL_FILE}")
            if len(symbols) > 10:
                logger.info(f"First 10: {', '.join(symbols[:10])}")
        
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        await api.close()
        return
    
    # Verify API connectivity
    try:
        api_ok = await api.check_terminal_running()
        if not api_ok:
            logger.error("Alpaca API not accessible. Exiting.")
            await api.close()
            return
    except Exception as e:
        logger.error(f"API connectivity check failed: {e}")
        await api.close()
        return
    
    # Initialize components
    parser = DataParser(logger, output_dir=config.OUTPUT_DIR, config=config)
    
    try:
        if config.BULK_DOWNLOAD_MODE:
            # Bulk download mode
            bulk_downloader = BulkDownloader(config, api, parser, logger)
            # FIXED: Changed download_all() to download_bulk()
            summary = await bulk_downloader.download_bulk(
                symbols=symbols,
                start_date=config.START_DATE,
                end_date=config.END_DATE
            )
            
            logger.info("\n" + "="*80)
            logger.info("BULK DOWNLOAD SUMMARY")
            logger.info("="*80)
            logger.info(f"Total records: {summary.get('total_records', 0):,}")
            logger.info(f"Months split: {summary.get('months_split', 0)}")
            logger.info(f"Daily files: {summary.get('daily_files', 0)}")
            logger.info("="*80)
        else:
            # Standard mode (legacy)
            writer = ParquetWriter(logger)
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
            logger.info(f"Dates successful: {summary['dates_successful']}")
            logger.info(f"Total records: {summary['total_records']:,}")
            logger.info("="*80)
    
    finally:
        await api.close()


if __name__ == "__main__":
    asyncio.run(main())