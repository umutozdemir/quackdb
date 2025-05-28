# quackdb

*A DuckDB wrapper with workload-driven parquet skipping index based on min/max and outlier aggregates.*

```python
import quackdb
# simple pass-through:
res = quackdb.sql("SELECT col1, .. coln FROM read_parquet(['data.parquet', ... ])")
