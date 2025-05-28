import os, pickle, math, time
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from duckdb import DuckDBPyRelation
from typing import Optional, Dict, Any, List

from .stats import stats_manager

# configure base folder for .sma files
# BASE_FOLDER = os.environ.get('QUACKDB_SMA_FOLDER', os.path.expanduser('~/.quackdb/sma'))
BASE_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
os.makedirs(BASE_FOLDER, exist_ok=True)

# SPA economic model constants
DEPOSIT_FACTOR = 0.1   # fraction of scan time we deposit after a full scan
REINVEST_FACTOR = 0.5  # fraction of time saved we reinvest after a skip
PROBE_PENALTY_FACTOR = 0.5  # fraction of avg scan time charged as probe cost for ineffective indexes
LAST_N_SCANS_TO_KEEP = 5  # number of scans to keep a stale index


def get_sma(
    path: str,
    column: str,
    op: str,
    threshold: float,
    ext: str = ".sma"
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}.parquet_{column}_{op}_{threshold}{ext}")
    # if sma file exists for given predicate, return it
    if os.path.exists(sma_file):
        with open(sma_file, 'rb') as f:
            return pickle.load(f)


def build_sma(
    path: str,
    column: str,
    op: str,
    threshold: float,
    ext: str = ".sma",
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}.parquet_{column}_{op}_{threshold}{ext}")

    # read full table
    tbl = pq.read_table(path)

    # get only given column
    arr: pa.ChunkedArray = tbl[column]

    # flatten & drop nulls
    vals: List[float] = []
    for chunk in arr.chunks:
        for v in chunk.to_numpy():
            if v is not None:
                vals.append(v)
    if not vals:
        return None
    
    # find outliers
    vals.sort()
    n = len(vals)

    def quantile(p: float) -> float:
        idx = p * (n - 1)
        low, high = math.floor(idx), math.ceil(idx)
        return vals[low] + (idx - low) * (vals[high] - vals[low])
    
    q1, q3 = quantile(0.25), quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # identify and filter outlier row indices
    outlier_indices: List[int] = []
    row = 0
    for chunk in arr.chunks:
        py = chunk.to_numpy()
        for i, v in enumerate(py):
            # check null and outlier conditions
            if v is not None and (v < lower_bound or v > upper_bound):
                # check if outlier satisfies the predicate
                if op == '>' and v > threshold:
                    outlier_indices.append(row + i)
                elif op == '<' and v < threshold:
                    outlier_indices.append(row + i)
                elif op == '=' and v == threshold:
                    outlier_indices.append(row + i)
                elif op == '!=' and v != threshold:
                    outlier_indices.append(row + i)
                elif op == '>=' and v >= threshold:
                    outlier_indices.append(row + i)
                elif op == '<=' and v <= threshold:
                    outlier_indices.append(row + i)
        row += len(py)
    
    # extract outlier rows with all columns included
    idx_arr = pa.array(outlier_indices, type=pa.int64())
    outlier_table = tbl.take(idx_arr) 

    stats = {
        "min": vals[0],
        "max": vals[-1],
        "lower_threshold": lower_bound,
        "upper_threshold": upper_bound,
        "outliers": outlier_table,
    }

    # save stats to sma file
    with open(sma_file, 'wb') as f:
        pickle.dump(stats, f)
    
    return stats


def read_parquet_sma(
    paths: List[str],
    projection: Optional[List[str]],
    column: str,
    op: str,
    threshold: float,
    con: 'duckdb.DuckDBPyConnection'
) -> DuckDBPyRelation:
    res = None
    paths_to_scan_fully = []
    
    # Get a new query ID for this query
    query_id = stats_manager.get_next_query_id()

    def avg_scan_time(key):
        fm = stats_manager.stats['files'].get(key, {})
        if fm.get('scan_count', 0):
            return fm['total_scan_time'] / fm['scan_count']
        return 0.0
    
    def build_sma_concurrently(path: str, col: str, op: str, threshold: float):
        try:
            build_sma(path, col, op, threshold)
        except Exception as e:
            print(f"Error building SMA for {path}: {e}")
    
    predicate_key = lambda f: f"{os.path.basename(f)}_{column}_{op}_{threshold}"

    for p in paths:
        print(f"Processing {p}")
        key = predicate_key(p)
        stats = get_sma(p, column, op, threshold)
        if stats is None:
            # can we afford construction cost?
            build_cost = DEPOSIT_FACTOR * avg_scan_time(key)
            if stats_manager.get_budget(key) >= build_cost:
                # pay for it and run build concurrently
                stats_manager.add_budget(key, -build_cost)
                threading.Thread(
                    target=build_sma_concurrently, 
                    args=(p, column, op, threshold),
                    daemon=False
                ).start()
                # Record construction
                stats_manager.record_construction(key)
            # fallback to full scan now
            paths_to_scan_fully.append(p)
            continue
        # file skipping check
        if (
            (op == '>' and threshold > stats['max']) or
            (op == '<' and threshold < stats['min'])
        ):
            print(f"Skipping {p} for {column} {op} {threshold}")
            # reinvest skip benefits - bonus is based on saved scan time
            skip_bonus = REINVEST_FACTOR * avg_scan_time(key)
            stats_manager.record_scan(key, 0.0, skipped=True)
            stats_manager.add_budget(key, skip_bonus)
            continue
        # outlier-only check
        print(f"stats: {stats['upper_threshold']} {stats['lower_threshold']}")
        if (
            (op == '>' and threshold > stats['upper_threshold']) or
            (op == '<' and threshold < stats['lower_threshold'])
        ):
            print(f"Retrieving outliers for {p} for {column} {op} {threshold}")
            # retrieve precomputed full-column outliers table
            out_tbl: pa.Table = stats['outliers']

            # apply projection if specified
            if projection:
                out_tbl = out_tbl.select(projection)
            outliers = con.from_arrow(out_tbl)

            stats_manager.record_scan(key, 0.0, outlier=True)
            # treat like skip - bonus for using outliers instead of full scan
            out_bonus = REINVEST_FACTOR * avg_scan_time(key)
            stats_manager.add_budget(key, out_bonus)
            res = outliers if res is None else res.union(outliers)
            continue
        else:
            # Index exists but cannot skip or use outliers - penalize it
            # The penalty is the cost of probing the index without getting benefits
            print(f"Index exists for {p} but provides no benefit - applying penalty")
            
            # Estimate probe cost as a fraction of average scan time
            probe_cost = PROBE_PENALTY_FACTOR * avg_scan_time(key)
            
            # Apply penalty by reducing budget
            stats_manager.add_budget(key, -probe_cost)
            
            # file is not skipped, add to full scan list
            paths_to_scan_fully.append(p)
         

    if len(paths_to_scan_fully) > 0:
        selected_fields = '*' if not projection else ', '.join(f'"{c}"' for c in projection)
        # scan files that cannot be skipped
        sql = f"SELECT {selected_fields} FROM read_parquet({paths_to_scan_fully}) WHERE \"{column}\" {op} {threshold}"

        start = time.perf_counter()
        rel = con.sql(sql)
        duration = time.perf_counter() - start

        for p in paths_to_scan_fully:
            # print(f"Stat updated for {p}")
            key = predicate_key(p)
            stats_manager.record_scan(key, duration)
            stats_manager.add_budget(key, DEPOSIT_FACTOR * duration)

        res = rel if res is None else res.union(rel)

    # deconstruct stale indexes
    # delete any .sma with B_key<0 or zero skip/outlier in last N scans
    for index, file_stats in stats_manager.stats['files'].items():
        budget = stats_manager.get_budget(index)
        should_deconstruct = False
        
        # Check if budget is negative
        if budget < 0:
            # print(f"Index {index} has negative budget ({budget}), marking for deconstruction")
            should_deconstruct = True
        
        # Check if index hasn't been useful recently
        elif (file_stats['scan_count'] >= LAST_N_SCANS_TO_KEEP and 
            (file_stats['skipped_count'] == 0 and file_stats['outlier_retrieved_count'] == 0)):
            # print(f"Index {index} hasn't provided benefits in {LAST_N_SCANS_TO_KEEP} scans, marking for deconstruction")
            should_deconstruct = True
            # stats_manager.add_budget(index, -budget)
            
        # Check if index hasn't been used recently
        elif (query_id - file_stats['last_sma_used_query_id'] > LAST_N_SCANS_TO_KEEP):
            # print(f"Index {index} hasn't been used in {LAST_N_SCANS_TO_KEEP} queries, marking for deconstruction")
            should_deconstruct = True
            
        if should_deconstruct:
            sma_file = os.path.join(BASE_FOLDER, f"{index}.sma")
            if os.path.exists(sma_file):
                print(f"Deleting stale index {sma_file}")
                os.remove(sma_file)
                stats_manager.record_deconstruction(index)
    
    stats_manager.save()
    return res
