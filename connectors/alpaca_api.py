"""
Alpaca API connector for historical stock data.
Documentation: https://docs.alpaca.markets/reference/stockbars
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from config.settings import Config
import httpx


class AlpacaAPI:
    """
    Alpaca Market Data API connector.
    Implements: https://docs.alpaca.markets/reference/stockbars
    """
    
    def __init__(self, config: Config, logger: logging.Logger):
        """Initialize Alpaca API connector."""
        self.config = config
        self.logger = logger
        self.base_url = config.ALPACA_BASE_URL
        
        # Build headers with API credentials per Alpaca documentation
        self.headers = {
            "APCA-API-KEY-ID": config.ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": config.ALPACA_SECRET_KEY,
            "accept": "application/json"
        }
        
        # Configure HTTP client with granular timeouts
        limits = httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=30.0
        )
        
        # Use granular timeouts instead of single timeout value
        timeout = httpx.Timeout(
            connect=10.0,      # 10s to establish connection
            read=300.0,        # 5 minutes to read response (for large paginated data)
            write=10.0,        # 10s to send request
            pool=10.0          # 10s to get connection from pool
        )
        
        self.session = httpx.AsyncClient(
            timeout=timeout,   # Use granular timeout object
            limits=limits,
            headers=self.headers,
            http2=True
        )
        
        self.logger.info(f"Alpaca API initialized: {self.base_url}")
        self.logger.info(f"Feed: {config.FEED} (SIP = 100% market coverage)")
        self.logger.info(f"Timeout: connect=10s, read=300s, write=10s, pool=10s")
        
    async def check_terminal_running(self) -> bool:
        """
        Verify Alpaca API is accessible.
        Tests connection using a lightweight bars request.
        
        Returns:
            bool: True if API is accessible
        """
        try:
            # Test with a simple bars request (market data API doesn't have /clock endpoint)
            # Use a known symbol like SPY with minimal data to test connection
            test_url = f"{self.base_url}/v2/stocks/bars"
            params = {
                "symbols": "SPY",
                "timeframe": "1Day",
                "start": "2024-01-01",
                "end": "2024-01-02",
                "limit": 1,
                "feed": self.config.FEED
            }
            
            response = await self.session.get(test_url, params=params)
            
            if response.status_code == 200:
                self.logger.info(f"✓ Alpaca API connection verified")
                self.logger.info(f"  Feed: {self.config.FEED} (SIP = 100% market coverage)")
                return True
            elif response.status_code == 401:
                self.logger.error("❌ Alpaca API authentication failed")
                self.logger.error("   Check your ALPACA_API_KEY and ALPACA_SECRET_KEY")
                return False
            elif response.status_code == 403:
                self.logger.error("❌ Alpaca API access forbidden")
                self.logger.error("   Your subscription may not support SIP data feed")
                self.logger.error("   Try setting FEED to 'iex' in config/settings.py")
                return False
            else:
                self.logger.error(f"❌ Alpaca API returned status {response.status_code}")
                self.logger.error(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Alpaca API connection failed: {e}")
            return False

    async def close(self):
        """Close HTTP session and cleanup resources."""
        if self.session:
            try:
                await self.session.aclose()
                self.logger.debug("HTTP session closed")
            except Exception as e:
                self.logger.debug(f"Error closing session: {e}")

    async def reset(self, force_reconnect: bool = False):
        """
        Reset connection (useful between batches).
        
        Args:
            force_reconnect: If True, recreate the HTTP session
        """
        if force_reconnect:
            await self.close()
            
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30.0
            )
            
            timeout = httpx.Timeout(
                connect=10.0,
                read=300.0,
                write=10.0,
                pool=10.0
            )
            
            self.session = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers=self.headers,
                http2=True
            )
            
            self.logger.debug("HTTP session reconnected")