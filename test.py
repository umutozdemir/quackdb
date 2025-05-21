import quackdb
import duckdb
import time

# QuackDB query
start_time = time.time()
tbl = quackdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
quackdb_time = time.time() - start_time
print("QuackDB result:")
print(tbl)
print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

# DuckDB query
start_time = time.time()
tbl = duckdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
duckdb_time = time.time() - start_time
print("DuckDB result:")
print(tbl)
print(f"DuckDB query time: {duckdb_time:.3f} seconds")
print(f"QuackDB row count: {len(tbl)}")
print(f"DuckDB row count: {len(tbl)}")
