"""
Diagnostic utilities for Alpaca API troubleshooting.
"""
import socket
import requests
import psutil
import logging
import json
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any
import os


class ErrorTracer:
    """Trace and log errors with detailed context."""
    
    @staticmethod
    def get_root_cause(exception: Exception) -> Exception:
        """Get the root cause of an exception chain."""
        root_cause = exception
        while getattr(root_cause, '__cause__', None):
            root_cause = root_cause.__cause__
        return root_cause
    
    @staticmethod
    def format_exception(exception: Exception) -> str:
        """Format exception with full traceback."""
        return traceback.format_exc()
    
    @staticmethod
    def log_exception(logger: logging.Logger, exception: Exception, context: str = ""):
        """Log exception with context and traceback."""
        root_cause = ErrorTracer.get_root_cause(exception)
        logger.error(f"{context}: {type(root_cause).__name__}: {str(root_cause)}")
        logger.debug(f"Traceback: {ErrorTracer.format_exception(exception)}")


class ConnectionDiagnostics:
    """Diagnostic utilities for Alpaca API connection testing."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    def test_port(self, host: str, port: int, timeout: float = 5.0) -> Dict[str, Any]:
        """
        Test if a port is open and accessible.
        
        Args:
            host: Hostname or IP address
            port: Port number
            timeout: Connection timeout in seconds
            
        Returns:
            Dict with 'open' bool and optional 'error' message
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                return {'open': True, 'error': None}
            else:
                return {'open': False, 'error': f"Connection refused (code {result})"}
                
        except socket.timeout:
            return {'open': False, 'error': 'Connection timeout'}
        except Exception as e:
            return {'open': False, 'error': str(e)}
    
    def test_api_endpoint(self, url: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        Test if an API endpoint is accessible.
        
        Args:
            url: API endpoint URL
            timeout: Request timeout in seconds
            
        Returns:
            Dict with 'accessible' bool, 'status_code', 'response', and 'error'
        """
        try:
            response = requests.get(url, timeout=timeout)
            
            return {
                'accessible': response.status_code == 200,
                'status_code': response.status_code,
                'response': response.text[:500],  # First 500 chars
                'error': None
            }
            
        except requests.Timeout:
            return {
                'accessible': False,
                'status_code': None,
                'response': None,
                'error': 'Request timeout'
            }
        except requests.ConnectionError as e:
            return {
                'accessible': False,
                'status_code': None,
                'response': None,
                'error': f'Connection error: {str(e)}'
            }
        except Exception as e:
            return {
                'accessible': False,
                'status_code': None,
                'response': None,
                'error': str(e)
            }
    
    def check_alpaca_connection(self, api_key: str, secret_key: str) -> bool:
        """
        Check Alpaca API connection and credentials.
        
        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
            
        Returns:
            bool: True if connection successful
        """
        try:
            headers = {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": secret_key
            }
            
            # Test clock endpoint
            response = requests.get(
                "https://data.alpaca.markets/v2/clock",
                headers=headers,
                timeout=10.0
            )
            
            if response.status_code == 200:
                self.logger.info("✓ Alpaca API connection successful")
                data = response.json()
                self.logger.info(f"  Market is {'open' if data.get('is_open') else 'closed'}")
                return True
            elif response.status_code == 401:
                self.logger.error("❌ Alpaca authentication failed - invalid credentials")
                return False
            else:
                self.logger.error(f"❌ Alpaca API error: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Alpaca connection test failed: {e}")
            return False