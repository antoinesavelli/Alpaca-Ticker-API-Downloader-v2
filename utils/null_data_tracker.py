"""
Null data tracker - in-memory by default, optional on-disk persistence.

Stops creating folders / JSON files unless persistence is explicitly enabled.
"""
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
import threading


class NullDataTracker:
    """
    Track symbols that produced null/invalid data for a given date.

    By default this class only keeps data in memory and does NOT write any
    files or create folders. To enable on-disk output set `persist=True`
    when constructing the tracker.

    Signature kept simple for backward compatibility:
        NullDataTracker(output_dir: str, logger, persist: bool = False)
    """

    def __init__(self, output_dir: str, logger, persist: bool = False):
        self.logger = logger
        self.output_dir = Path(output_dir) if output_dir else None
        self.persist = bool(persist)
        # dict[date: str] -> list[dict{symbol, reason}]
        self._nulls: Dict[str, List[Dict[str, str]]] = {}
        self._lock = threading.Lock()

    def add_null_symbol(self, symbol: str, date: str, reason: str) -> None:
        """
        Record a null/invalid symbol for `date`. This only appends to memory
        by default. If `persist` is True the tracker will also write a single
        JSON file per date (overwrites previous file for that date).
        """
        if not date:
            return

        entry = {"symbol": str(symbol), "reason": str(reason)}

        with self._lock:
            lst = self._nulls.setdefault(date, [])
            lst.append(entry)

        # Only persist when explicitly enabled
        if self.persist and self.output_dir:
            try:
                # create a simple file per date, do not create nested debug folders
                out_dir = Path(self.output_dir)
                out_dir.mkdir(parents=True, exist_ok=True)
                file_path = out_dir / f"null_symbols_{date}.json"
                # Write the entire list for that date (atomic-ish)
                tmp = file_path.with_suffix(".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self._nulls.get(date, []), f, indent=2)
                tmp.replace(file_path)
                self.logger.debug(f"Saved null symbols for {date} to {file_path}")
            except Exception as e:
                # Log at debug to avoid noisy logs; do not raise
                try:
                    self.logger.debug(f"Failed to persist null symbols for {date}: {e}")
                except Exception:
                    pass

    def should_skip_symbol(self, symbol: str, date: str) -> Tuple[bool, Optional[str]]:
        """
        Decide whether a symbol should be skipped for a given date.

        Returns:
            (should_skip: bool, reason: Optional[str])

        Default behavior: return True if this symbol was already recorded
        as null for the same date (helps avoid repeated requests for known nulls).
        Caller can override behavior by replacing the tracker or by constructing
        the tracker with a wrapper that implements different policy.
        """
        if not date:
            return False, None

        with self._lock:
            entries = self._nulls.get(date, [])
            for e in entries:
                if e.get("symbol") == symbol:
                    return True, e.get("reason")
        return False, None

    def get_nulls_for_date(self, date: str) -> List[Dict[str, str]]:
        """Return recorded null entries for a given date (empty list if none)."""
        with self._lock:
            return list(self._nulls.get(date, []))

    def clear_date(self, date: str) -> None:
        """Clear recorded null entries for a date."""
        with self._lock:
            if date in self._nulls:
                del self._nulls[date]

    def flush_all_to_disk(self) -> None:
        """
        Persist all tracked nulls to disk. Only performs work when `persist=True`.
        This method writes files directly under `output_dir` as `null_symbols_{date}.json`.
        """
        if not (self.persist and self.output_dir):
            return

        with self._lock:
            for date, entries in self._nulls.items():
                try:
                    out_dir = Path(self.output_dir)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    file_path = out_dir / f"null_symbols_{date}.json"
                    tmp = file_path.with_suffix(".tmp")
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(entries, f, indent=2)
                    tmp.replace(file_path)
                    try:
                        self.logger.debug(f"Flushed null symbols for {date} to {file_path}")
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        self.logger.debug(f"Failed flushing null symbols for {date}: {e}")
                    except Exception:
                        pass
