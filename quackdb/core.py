import os, pickle, math
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from typing import Optional, Dict, Any, List

# configure base folder for .sma files
# BASE_FOLDER = os.environ.get('QUACKDB_SMA_FOLDER', os.path.expanduser('~/.quackdb/sma'))
BASE_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
os.makedirs(BASE_FOLDER, exist_ok=True)


def get_sma(
    path: str,
    column: str,
    ext: str = ".sma"
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}_{column}{ext}")
    # if sma file exists for given column, return it
    if os.path.exists(sma_file):
        with open(sma_file, 'rb') as f:
            return pickle.load(f)


def build_sma(
    path: str,
    column: str,
    iqr_mult: float = 1.5,
    ext: str = ".sma"
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}_{column}{ext}")

    # read only given column
    tbl = pq.read_table(path, columns=[column])
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
        lo, hi = math.floor(idx), math.ceil(idx)
        return vals[lo] + (idx - lo) * (vals[hi] - vals[lo])
    q1, q3 = quantile(0.25), quantile(0.75)
    iqr = q3 - q1
    lo_thr = q1 - iqr_mult * iqr
    hi_thr = q3 + iqr_mult * iqr

    # collect outliers
    outlier_values = []
    row = 0
    for chunk in arr.chunks:
        py = chunk.to_numpy()
        for i, v in enumerate(py):
            if v is not None and (v < lo_thr or v > hi_thr):
                outlier_values.append(v)
        row += len(py)
    
    # Create PyArrow Table for outliers
    outlier_table = pa.table({
        column: outlier_values
    })
    
    stats = {
        "min": vals[0],
        "max": vals[-1],
        "lower_threshold": lo_thr,
        "upper_threshold": hi_thr,
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
    raw_sql: str,
    con: 'duckdb.DuckDBPyConnection'
) -> pa.Table:
    tables: List[pa.Table] = []
    
    def build_sma_concurrently(path: str, col: str):
        try:
            build_sma(path, col)
        except Exception as e:
            print(f"Error building SMA for {path}: {e}")

    for p in paths:
        stats = get_sma(p, column)
        if stats is None:
            print('Building SMA concurrently for', p)
            # Start building SMA in background
            thread = threading.Thread(target=build_sma_concurrently, args=(p, column))
            thread.daemon = False  # Thread will be killed when main program exits
            thread.start()
            
            # Proceed with DuckDB query immediately
            # t = internal_duckdb_query(p, projection, column, op, threshold, con, raw_sql)
            t = con.execute(raw_sql).arrow() 
            if t.num_rows > 0:
                tables.append(t)
            continue
        # file skipping check
        if (
            (op == '>' and threshold > stats['max']) or
            (op == '<' and threshold < stats['min'])
        ):
            print('Skipping', p)
            continue
        # outlier-only check
        if (
            (op == '>' and threshold > stats['upper_threshold']) or
            (op == '<' and threshold < stats['lower_threshold'])
        ):
            print('Outlier detected', p)
            tables.append(stats['outliers'])
        else:
            # t = internal_duckdb_query(p, projection, column, op, threshold, con)
            t = con.execute(raw_sql).arrow() 
            if t.num_rows > 0:
                tables.append(t)

    if not tables:
        return []
    return pa.concat_tables(tables, promote=True)


def internal_duckdb_query(
    path: str,
    projection: Optional[List[str]],
    column: str,
    op: str,
    threshold: float,
    con: 'duckdb.DuckDBPyConnection'
) -> pa.Table:
    sel = '*'
    if projection:
        sel = ', '.join(f'"{c}"' for c in projection)
        sql = (
            f"SELECT {sel}"
            f" FROM read_parquet('{path}')"
            f" WHERE \"{column}\" {op} {threshold}"
        )
    return con.execute(sql).arrow() 