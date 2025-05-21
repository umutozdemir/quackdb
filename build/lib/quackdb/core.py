import os, pickle, math

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from typing import Optional, Dict, Any, List

# configure base folder for .sma files
# BASE_FOLDER = os.environ.get('QUACKDB_SMA_FOLDER', os.path.expanduser('~/.quackdb/sma'))
BASE_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
os.makedirs(BASE_FOLDER, exist_ok=True)


def build_sma(
    path: str,
    column: str,
    iqr_mult: float = 1.5,
    ext: str = ".sma"
) -> Optional[Dict[str, Any]]:
    base = os.path.splitext(os.path.basename(path))[0]
    sma_file = os.path.join(BASE_FOLDER, f"{base}_{column}{ext}")
    if os.path.exists(sma_file):
        with open(sma_file, 'rb') as f:
            return pickle.load(f)
    # read only that column
    tbl = pq.read_table(path, columns=[column])
    arr: pa.ChunkedArray = tbl[column]
    # flatten & drop nulls
    vals: List[float] = []
    for chunk in arr.chunks:
        for v in chunk.to_pylist():
            if v is not None:
                vals.append(v)
    if not vals:
        return None
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
    out: List[int] = []
    row = 0
    for chunk in arr.chunks:
        py = chunk.to_pylist()
        for i, v in enumerate(py):
            if v is not None and (v < lo_thr or v > hi_thr):
                out.append(row + i)
        row += len(py)
    stats = {
        "min": vals[0],
        "max": vals[-1],
        "lower_threshold": lo_thr,
        "upper_threshold": hi_thr,
        "outlier_indices": out,
    }
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
) -> pa.Table:
    import duckdb
    tables: List[pa.Table] = []
    for p in paths:
        stats = build_sma(p, column)
        if stats is None:
            continue
        # skip
        if (
            (op == '>' and threshold > stats['max']) or
            (op == '<' and threshold < stats['min'])
        ):
            continue
        # outlier-only branch
        if (
            (op == '>' and threshold > stats['upper_threshold']) or
            (op == '<' and threshold < stats['lower_threshold'])
        ):
            cols = projection if projection else None
            t = pq.read_table(p, columns=cols)
            idx_arr = pa.array(stats['outlier_indices'], pa.int64())
            tables.append(t.take(idx_arr))
        else:
            sel = '*'
            if projection:
                sel = ', '.join(f'"{c}"' for c in projection)
            sql = (
                f"SELECT {sel}"
                f" FROM read_parquet('{p}')"
                f" WHERE \"{column}\" {op} {threshold}"
            )
            t = con.execute(sql).arrow()
            if t.num_rows > 0:
                tables.append(t)
    if not tables:
        return pa.Table.from_batches([])
    return pa.concat_tables(tables, promote=True)