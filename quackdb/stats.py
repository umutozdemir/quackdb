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
                "last_scan_time": float,
                "last_parquet_scanned_query_id": int,
                "last_sma_used_query_id": int,
                "construction_count": int,
                "deconstruction_count": int
            },
            ...
        },
        "current_query_id": int
      }
    """
    def __init__(self):
        self.lock = RLock()
        self.stats = {"budgets": {}, "files": {}, "current_query_id": 0}
        self._load()

    def _load(self):
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, 'r') as f:
                    data = json.load(f)
                    self.stats = data
            except Exception as e:
                # print(f"Error loading stats file {STATS_FILE}: {e}")
                self.stats = {"budgets": {}, "files": {}, "current_query_id": 0}
        else:
            os.makedirs(BASE_FOLDER, exist_ok=True)
            self.stats = {"budgets": {}, "files": {}, "current_query_id": 0}

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
            if b < 0:
                b = 0
            self.stats.setdefault('budgets', {})[key] = b

    def get_next_query_id(self) -> int:
        """Get and increment the current query ID."""
        with self.lock:
            query_id = self.stats["current_query_id"]
            self.stats["current_query_id"] += 1
            return query_id

    def record_construction(self, key: str):
        """Record that an index was constructed for the given predicate key."""
        with self.lock:
            fm = self.stats.setdefault('files', {}).setdefault(key, {
                'scan_count': 0,
                'skipped_count': 0,
                'outlier_retrieved_count': 0,
                'total_scan_time': 0.0,
                'last_scan_time': 0.0,
                'last_parquet_scanned_query_id': 0,
                'last_sma_used_query_id': 0,
                'construction_count': 0,
                'deconstruction_count': 0
            })
            fm['construction_count'] = fm.get('construction_count', 0) + 1

    def record_deconstruction(self, key: str):
        """Record that an index was deconstructed for the given predicate key."""
        # print(f"Deconstructing index for {key}")
        with self.lock:
            fm = self.stats.setdefault('files', {}).setdefault(key, {
                'scan_count': 0,
                'skipped_count': 0,
                'outlier_retrieved_count': 0,
                'total_scan_time': 0.0,
                'last_scan_time': 0.0,
                'last_parquet_scanned_query_id': 0,
                'last_sma_used_query_id': 0,
                'construction_count': 0,
                'deconstruction_count': 0
            })
            fm['deconstruction_count'] = fm.get('deconstruction_count', 0) + 1

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
                'last_scan_time': 0.0,
                'last_parquet_scanned_query_id': 0,
                'last_sma_used_query_id': 0,
                'construction_count': 0,
                'deconstruction_count': 0
            })
            fm['scan_count'] += 1
            fm['total_scan_time'] += scan_time
            fm['last_scan_time'] = scan_time
            if skipped or outlier:
                fm['last_sma_used_query_id'] = self.stats["current_query_id"]
            if skipped:
                fm['skipped_count'] += 1
            elif outlier:
                fm['outlier_retrieved_count'] += 1
            else:
                fm['last_parquet_scanned_query_id'] = self.stats["current_query_id"]

# singleton instance
stats_manager = StatsManager()
