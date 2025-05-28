import duckdb
import pyarrow as pa
from .core import read_parquet_sma
from .utils import parse_sql

_conn = duckdb.connect()

def sql(query: str) -> pa.Table:
    """
    Run SQL through DuckDB, but intercept Parquet queries to apply SMA skipping/outliers.
    Returns a pyarrow.Table by default.
    """
    parts = parse_sql(query)
    if parts:
        files, proj, col, op, val = parts
        if col and op and val is not None:
            return read_parquet_sma(files, proj, col, op, val, con=_conn)
        else:
            raise ValueError("Query can not be executed")
    else:
        raise ValueError("Query can not be parsed")

# convenience alias
query = sql

def main():
    import sys
    sql_text = ' '.join(sys.argv[1:])
    table = sql(sql_text)
    print(table)

if __name__ == "__main__":
    main()
