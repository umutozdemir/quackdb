import os, pickle, math
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from duckdb import DuckDBPyRelation
from typing import Optional, Dict, Any, List

# configure base folder for .sma files
# BASE_FOLDER = os.environ.get('QUACKDB_SMA_FOLDER', os.path.expanduser('~/.quackdb/sma'))
BASE_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
os.makedirs(BASE_FOLDER, exist_ok=True)


def get_sma(
    path: str,
    column: str,
    op: str,
    threshold: float,
    ext: str = ".sma"
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}_{column}_{op}_{threshold}{ext}")
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
    sma_file = os.path.join(BASE_FOLDER, f"{base}_{column}_{op}_{threshold}{ext}")

    # read full table
    tbl = pq.read_table(path)

    # get only given column
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
        "outlier_indices": outlier_indices,
        "outliers": outlier_table,
    }

    # save stats to sma file
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
    n_file_completely_skipped = 0
    n_file_read_from_outliers = 0

    res = None
    paths_to_scan_fully = []
    
    def build_sma_concurrently(path: str, col: str, op: str):
        try:
            build_sma(path, col, op, threshold)
        except Exception as e:
            print(f"Error building SMA for {path}: {e}")

    for p in paths:
        stats = get_sma(p, column, op, threshold)
        if stats is None:
            print('Building SMA concurrently for', p)
            # build SMA concurrently
            thread = threading.Thread(target=build_sma_concurrently, args=(p, column, op))
            thread.daemon = False  # Thread wont be killed when program exits
            thread.start()
            
            paths_to_scan_fully.append(p)
            continue
        # file skipping check
        if (
            (op == '>' and threshold > stats['max']) or
            (op == '<' and threshold < stats['min'])
        ):
            print('Skipping', p)
            n_file_completely_skipped += 1
            continue
        # outlier-only check
        if (
            (op == '>' and threshold > stats['upper_threshold']) or
            (op == '<' and threshold < stats['lower_threshold'])
        ):
             # retrieve precomputed full-column outliers table
            out_tbl: pa.Table = stats['outliers']

            # apply projection if specified
            if projection:
                out_tbl = out_tbl.select(projection)
            outliers = con.from_arrow(out_tbl)

            if res is None:
                res = outliers
            else:
                res = res.union(outliers)
            n_file_read_from_outliers += 1
        else:
            paths_to_scan_fully.append(p)
   
    if len(paths_to_scan_fully) == 0:
        return res
    # scan files that cannot be skipped
    p_sql = f'SELECT {", ".join(projection)} FROM read_parquet({paths_to_scan_fully}) WHERE {column} {op} {threshold}'
    if res is None:
        res = con.sql(p_sql)
    else:
        res = res.union(con.sql(p_sql))

    return res
