import quackdb
import duckdb
import time


def test_with_outlier_predicate():
    # QuackDB query
    start_time = time.time()
    quackdb_res = quackdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
    quackdb_time = time.time() - start_time
    print("QuackDB result:")
    print(quackdb_res)
    print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

    # DuckDB query
    start_time = time.time()
    duckdb_res = duckdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
    duckdb_time = time.time() - start_time
    print("DuckDB result:")
    print(duckdb_res)
    print(f"DuckDB query time: {duckdb_time:.3f} seconds")
    print(f"QuackDB row count: {len(quackdb_res)}")
    print(f"DuckDB row count: {len(duckdb_res)}")


def test_with_min_max_skipped_predicate():
    # QuackDB query
    start_time = time.time()
    quackdb_res = quackdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 42140000")
    quackdb_time = time.time() - start_time
    print("QuackDB result:")
    print(quackdb_res)
    print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

    # DuckDB query
    start_time = time.time()
    duckdb_res = duckdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 42140000")
    duckdb_time = time.time() - start_time
    print("DuckDB result:")
    print(duckdb_res)
    print(f"DuckDB query time: {duckdb_time:.3f} seconds")
    print(f"QuackDB row count: {len(quackdb_res)}")
    print(f"DuckDB row count: {len(duckdb_res)}")


def test_with_not_skipped_predicate():
    # QuackDB query
    start_time = time.time()
    quackdb_res = quackdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 50000")
    quackdb_time = time.time() - start_time
    print("QuackDB result:")
    print(quackdb_res)
    print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

    # DuckDB query
    start_time = time.time()
    duckdb_res = duckdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 50000")
    duckdb_time = time.time() - start_time
    print("DuckDB result:")
    print(duckdb_res)
    print(f"DuckDB query time: {duckdb_time:.3f} seconds")
    print(f"QuackDB row count: {len(quackdb_res)}")
    print(f"DuckDB row count: {len(duckdb_res)}")


def test_with_multiple_files_and_not_skipped_predicate():
    # QuackDB query
    start_time = time.time()
    quackdb_res = quackdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_*.parquet' WHERE o_totalprice > 50000")
    quackdb_time = time.time() - start_time
    print("QuackDB result:")
    print(quackdb_res)
    print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

    # DuckDB query
    start_time = time.time()
    duckdb_res = duckdb.sql("SELECT o_totalprice, o_orderkey FROM '/Users/umutozdemir/Desktop/theses/data/orders_*.parquet' WHERE o_totalprice > 50000")
    duckdb_time = time.time() - start_time
    print("DuckDB result:")
    print(duckdb_res)
    print(f"DuckDB query time: {duckdb_time:.3f} seconds")
    print(f"QuackDB row count: {len(quackdb_res)}")
    print(f"DuckDB row count: {len(duckdb_res)}")


# test_with_multiple_files_and_not_skipped_predicate()
test_with_outlier_predicate()
# test_with_min_max_skipped_predicate()
# test_with_not_skipped_predicate()