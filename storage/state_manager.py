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
    
    def __init__(self, state_file: str, logger: logging.Logger, output_dir: str = None):
        """Initialize state manager."""
        self.state_file = Path(state_file)
        self.output_dir = Path(output_dir) if output_dir else None
        self.logger = logger
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        
        # Auto-detect existing daily files if output_dir provided
        if self.output_dir:
            self._sync_with_filesystem()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        # Define default state structure
        default_state = {
            'batches': {},
            'completed_months': {},  # Track which months are fully downloaded
            'split_months': {},      # Track which months are split
            'daily_files': {},       # Track individual daily files
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
    
    def _sync_with_filesystem(self):
        """
        Sync state with filesystem - detect existing daily files.
        This allows resuming even if state file was lost.
        """
        if not self.output_dir or not self.output_dir.exists():
            return
        
        # Find all existing daily parquet files
        existing_files = list(self.output_dir.glob("????????.parquet"))
        
        if existing_files:
            self.logger.info(f"🔍 Scanning filesystem: found {len(existing_files)} existing daily files")
            
            for file_path in existing_files:
                date_str = file_path.stem  # YYYYMMDD
                month_str = date_str[:6]   # YYYYMM
                
                # Mark daily file as existing
                self.state['daily_files'][date_str] = {
                    'path': str(file_path),
                    'detected_at': datetime.now().isoformat(),
                    'size_mb': file_path.stat().st_size / (1024 * 1024)
                }
                
                # Mark month as split if we have files from it
                if month_str not in self.state['split_months']:
                    self.state['split_months'][month_str] = {
                        'status': 'split',
                        'detected_from_filesystem': True
                    }
            
            self.logger.info(f"✓ Detected {len(self.state['split_months'])} months with existing data")
            self._save_state()
    
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
        # Update completed months status
        if month in self.state['completed_months']:
            self.state['completed_months'][month]['status'] = 'split_complete'
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
        """Check if month needs to be split."""
        return self.is_month_downloaded(month) and not self.is_month_split(month)
    
    def get_months_needing_download(self, all_months: List[str]) -> List[str]:
        """Get list of months that need downloading."""
        return [m for m in all_months if self.needs_download(m)]
    
    def get_months_needing_split(self) -> List[str]:
        """Get list of months that need splitting."""
        downloaded = set(self.state['completed_months'].keys())
        split = set(self.state['split_months'].keys())
        return sorted(downloaded - split)
    
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
        
        return {
            'total_months': len(all_months),
            'completed_months': len(completed),
            'needs_download': len(needs_download),
            'needs_split': len(needs_split),
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
        
        return {
            'completed_batches': completed,
            'failed_batches': failed,
            'total_records': total_records,
            'total_symbols': total_symbols,
            'months_downloaded': len(self.state['completed_months']),
            'months_split': len(self.state['split_months']),
            'daily_files': len(self.state['daily_files'])
        }
    
    def reset(self):
        """Reset all state."""
        self.state = {
            'batches': {},
            'completed_months': {},
            'split_months': {},
            'daily_files': {},
            'start_time': None,
            'last_update': None
        }
        self._save_state()
        self.logger.info("State reset")
