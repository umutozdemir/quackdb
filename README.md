# quackdb

*A DuckDB wrapper with parquet skipping index based on min/max and outlier aggregates.*

```python
import quackdb
# simple pass-through:
res = quackdb.sql("SELECT count(*) FROM read_parquet('data.parquet')")
# SMA-powered skip/outliers:
res = quackdb.sql("SELECT * FROM 'data.parquet' WHERE a > 30")