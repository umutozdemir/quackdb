import quackdb
import duckdb
import time


# Calculate outlier thresholds using DuckDB
con = duckdb.connect()
sql = """
WITH stats AS (
    SELECT 
        percentile_cont(0.25) WITHIN GROUP (ORDER BY o_totalprice) as q1,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY o_totalprice) as q3
    FROM read_parquet('/Users/umutozdemir/Desktop/theses/data/orders_1.parquet')
),
thresholds AS (
    SELECT 
        q1,
        q3,
        q3 - q1 as iqr,
        q1 - 1.5 * (q3 - q1) as lower_threshold,
        q3 + 1.5 * (q3 - q1) as upper_threshold
    FROM stats
)
SELECT 
    t.*,
    COUNT(*) as total_rows,
    SUM(CASE WHEN o_totalprice < t.lower_threshold OR o_totalprice > t.upper_threshold THEN 1 ELSE 0 END) as outlier_count,
    ROUND(SUM(CASE WHEN o_totalprice < t.lower_threshold OR o_totalprice > t.upper_threshold THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100, 2) as outlier_percentage
FROM thresholds t
CROSS JOIN read_parquet('/Users/umutozdemir/Desktop/theses/data/orders_1.parquet')
GROUP BY t.q1, t.q3, t.iqr, t.lower_threshold, t.upper_threshold;
"""
result = con.execute(sql).fetchall()
print("\nIQR Analysis for o_totalprice:")
print(f"Q1 (25th percentile): {result[0][0]:.2f}")
print(f"Q3 (75th percentile): {result[0][1]:.2f}")
print(f"IQR: {result[0][2]:.2f}")
print(f"Lower threshold: {result[0][3]:.2f}")
print(f"Upper threshold: {result[0][4]:.2f}")
print(f"Total rows: {result[0][5]:,}")
print(f"Outlier count: {result[0][6]:,}")
print(f"Outlier percentage: {result[0][7]}%")

# Get total row count
total_rows = con.execute("SELECT COUNT(*) FROM read_parquet('/Users/umutozdemir/Desktop/theses/data/orders_1.parquet')").fetchone()[0]
print(f"Total rows in dataset: {total_rows:,}")

# QuackDB query
start_time = time.time()
tbl_quackdb = quackdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
quackdb_time = time.time() - start_time
print("QuackDB result:")
print(tbl_quackdb)
print(f"QuackDB query time: {quackdb_time:.3f} seconds\n")

# DuckDB query
start_time = time.time()
tbl_duckdb = duckdb.sql("SELECT o_totalprice FROM '/Users/umutozdemir/Desktop/theses/data/orders_1.parquet' WHERE o_totalprice > 421400")
duckdb_time = time.time() - start_time
print("DuckDB result:")
print(tbl_duckdb)
print(f"DuckDB query time: {duckdb_time:.3f} seconds")
print(f"QuackDB row count: {len(tbl_quackdb)}")
print(f"DuckDB row count: {len(tbl_duckdb)}")
