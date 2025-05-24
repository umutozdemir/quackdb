import os
import json
from threading import RLock

BASE_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
STATS_FILE = os.path.join(BASE_FOLDER, 'stats.json')

class StatsManager:
    """
    Manages persistent workload-driven budgets and per-predicate metrics for SMA indexing.
    Stores JSON with structure:
      {
        "budgets": { "<predicate_key>": float, ... },
        "files": {
            "<predicate_key>": {
                "budget": float,
                "scan_count": int,
                "skipped_count": int,
                "outlier_retrieved_count": int,
                "total_scan_time": float,
                "last_scan_time": float
            },
            ...
        }
      }
    """
    def __init__(self):
        self.lock = RLock()
        self.stats = {"budgets": {}, "files": {}}
        self._load()

    def _load(self):
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, 'r') as f:
                    data = json.load(f)
                    self.stats = data
            except Exception as e:
                print(f"Error loading stats file {STATS_FILE}: {e}")
                self.stats = {"budgets": {}, "files": {}}
        else:
            os.makedirs(BASE_FOLDER, exist_ok=True)
            self.stats = {"budgets": {}, "files": {}}

    def save(self):
        """Persist current stats to disk."""
        with self.lock:
            with open(STATS_FILE, 'w') as f:
                json.dump(self.stats, f, indent=2)

    def get_budget(self, key: str) -> float:
        """Return current budget for a predicate key."""
        with self.lock:
            return float(self.stats.get('budgets', {}).get(key, 0.0))

    def add_budget(self, key: str, amount: float):
        """Adjust budget by amount. In-memory only; call save() after queries."""
        with self.lock:
            b = self.get_budget(key) + amount
            self.stats.setdefault('budgets', {})[key] = b

    def record_scan(self, key: str, scan_time: float, skipped: bool = False, outlier: bool = False):
        """
        Record a file scan event for predicate `key` on `file`:
          - scan_time: seconds of the full DuckDB scan
          - skipped: True if the file was skipped
          - outlier: True if outlier retrieval was used
        Updates in-memory stats; call save() after queries.
        """
        with self.lock:
            fm = self.stats.setdefault('files', {}).setdefault(key, {
                'scan_count': 0,
                'skipped_count': 0,
                'outlier_retrieved_count': 0,
                'total_scan_time': 0.0,
                'last_scan_time': 0.0
            })
            fm['scan_count'] += 1
            fm['total_scan_time'] += scan_time
            fm['last_scan_time'] = scan_time
            if skipped:
                fm['skipped_count'] += 1
            if outlier:
                fm['outlier_retrieved_count'] += 1

# singleton instance
stats_manager = StatsManager()
