import quackdb
import duckdb
import time

# QuackDB query
start_time = time.time()
quackdb_res = quackdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
# quackdb_res = quackdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 300400")
quackdb_time = time.time() - start_time
print("QuackDB result:")
print(quackdb_res)
print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

# DuckDB query
start_time = time.time()
duckdb_res = duckdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400").arrow()
# duckdb_res = duckdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 300400").arrow() 
duckdb_time = time.time() - start_time
print("DuckDB result:")
print(duckdb_res)
print(f"DuckDB query time: {duckdb_time:.3f} seconds")
print(f"QuackDB row count: {len(quackdb_res)}")
print(f"DuckDB row count: {len(duckdb_res)}")
