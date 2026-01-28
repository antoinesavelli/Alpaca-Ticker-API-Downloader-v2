"""
Trading calendar utilities for US stock market.
Uses pandas_market_calendars for accurate holiday detection.
"""
from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
import logging

try:
    import pandas_market_calendars as mcal
    HAS_MARKET_CALENDARS = True
except ImportError:
    HAS_MARKET_CALENDARS = False


class TradingCalendar:
    """Handle trading day calculations with holiday support."""
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize trading calendar.
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger
        
        if not HAS_MARKET_CALENDARS:
            if self.logger:
                self.logger.warning(
                    "pandas_market_calendars not installed. "
                    "Only weekends will be filtered. "
                    "Install with: pip install pandas-market-calendars"
                )
            self.nyse = None
        else:
            # Get NYSE calendar (most common for US stocks)
            self.nyse = mcal.get_calendar('NYSE')
    
    def is_trading_day(self, date: str) -> bool:
        """
        Check if a date is a trading day.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            True if trading day, False otherwise
        """
        dt = datetime.strptime(date, '%Y%m%d')
        
        # Always filter weekends
        if dt.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # If market calendars available, check holidays
        if self.nyse is not None:
            try:
                schedule = self.nyse.schedule(start_date=dt, end_date=dt)
                return len(schedule) > 0
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error checking holiday for {date}: {e}")
                # Fallback: if error, assume it's a trading day (weekday)
                return True
        
        # Without market calendars, weekdays are assumed trading days
        return True
    
    def get_skip_reason(self, date: str) -> Tuple[bool, str]:
        """
        Check if a date should be skipped and provide reason.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Tuple of (should_skip, reason)
            - should_skip: True if not a trading day
            - reason: "weekend" or "holiday" or empty string if trading day
        """
        dt = datetime.strptime(date, '%Y%m%d')
        
        # Check if weekend
        if dt.weekday() >= 5:  # Saturday=5, Sunday=6
            day_name = dt.strftime('%A')
            return True, f"weekend ({day_name})"
        
        # Check if holiday (only if market calendar available)
        if self.nyse is not None:
            try:
                schedule = self.nyse.schedule(start_date=dt, end_date=dt)
                if len(schedule) == 0:
                    return True, "market holiday"
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error checking holiday for {date}: {e}")
        
        return False, ""
    
    def get_trading_days_with_skipped(self, start_date: str, end_date: str) -> Tuple[List[str], List[Tuple[str, str]]]:
        """
        Get list of trading days and list of skipped days with reasons.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            Tuple of (trading_days, skipped_days)
            - trading_days: List of trading dates in YYYYMMDD format
            - skipped_days: List of (date, reason) tuples for skipped dates
        """
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        trading_days = []
        skipped_days = []
        
        current = start
        while current <= end:
            date_str = current.strftime('%Y%m%d')
            should_skip, reason = self.get_skip_reason(date_str)
            
            if should_skip:
                skipped_days.append((date_str, reason))
            else:
                trading_days.append(date_str)
            
            current += timedelta(days=1)
        
        if self.logger:
            self.logger.info(
                f"Trading days: {len(trading_days)}, "
                f"Skipped days: {len(skipped_days)} "
                f"({sum(1 for _, r in skipped_days if 'weekend' in r)} weekends, "
                f"{sum(1 for _, r in skipped_days if 'holiday' in r)} holidays)"
            )
        
        return trading_days, skipped_days
    
    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """
        Get list of trading days between start and end (inclusive).
        Excludes weekends and market holidays.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            List of trading dates in YYYYMMDD format
        """
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        if self.nyse is not None:
            try:
                # Use market calendar for accurate trading days
                schedule = self.nyse.schedule(start_date=start, end_date=end)
                trading_days = [d.strftime('%Y%m%d') for d in schedule.index]
                
                if self.logger:
                    total_days = (end - start).days + 1
                    weekend_days = sum(1 for d in pd.date_range(start, end) if d.weekday() >= 5)
                    holiday_days = total_days - weekend_days - len(trading_days)
                    self.logger.info(
                        f"Trading days: {len(trading_days)} "
                        f"(excluded {weekend_days} weekend days, {holiday_days} holidays)"
                    )
                
                return trading_days
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error getting trading days: {e}. Falling back to weekend-only filtering.")
        
        # Fallback: manual weekday filtering only
        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Weekdays only
                dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        if self.logger:
            self.logger.info(f"Trading days (weekend filter only): {len(dates)}")
        
        return dates
    
    def get_previous_trading_day(self, date: str) -> str:
        """
        Get the previous trading day before the given date.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Previous trading day in YYYYMMDD format
        """
        dt = datetime.strptime(date, '%Y%m%d')
        current = dt - timedelta(days=1)
        
        # Go back up to 10 days to find previous trading day
        for _ in range(10):
            date_str = current.strftime('%Y%m%d')
            if self.is_trading_day(date_str):
                return date_str
            current -= timedelta(days=1)
        
        # Fallback: return 1 day before if can't find trading day
        return (dt - timedelta(days=1)).strftime('%Y%m%d')


def is_trading_day(date: str) -> bool:
    """
    Convenience function to check if a date is a trading day.
    
    Args:
        date: Date in YYYYMMDD format
        
    Returns:
        True if trading day, False otherwise
    """
    calendar = TradingCalendar()
    return calendar.is_trading_day(date)


def get_trading_dates(start_date: str, end_date: str) -> List[str]:
    """
    Convenience function to get trading days in a date range.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        List of trading dates in YYYYMMDD format
    """
    calendar = TradingCalendar()
    return calendar.get_trading_days(start_date, end_date)
