# analysis.py
import json, time, glob, os
import quackdb
import matplotlib.pyplot as plt
from collections import defaultdict

SMA_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
STATS_JSON = os.path.join(SMA_FOLDER, 'stats.json')

# workload
FILES = sorted(glob.glob("/Users/umutozdemir/Desktop/theses/data/yellow_tripdata_202*-*.parquet"))
QUERY_LIST = [
    # skip
    f"SELECT * FROM read_parquet({FILES}) WHERE total_amount > 10000",
    # outlier-only
    f"SELECT total_amount FROM read_parquet({FILES}) WHERE total_amount > 500",
    # full-scan
    f"SELECT total_amount FROM read_parquet({FILES}) WHERE total_amount > 20",
]

# metrics containers
query_ids = []
exec_times = []
skipped_counts = []
built_counts = []
pruned_counts = []
budgets_over_time = defaultdict(list)  # predicate_key -> [budget]

def load_stats():
    with open(STATS_JSON) as f:
        return json.load(f)

def run_and_record(q):
    # measure exec time
    start = time.perf_counter()
    tbl = quackdb.sql(q)
    dur = time.perf_counter() - start
    # load stats.json
    stats = load_stats()
    qid = stats['current_query_id']
    # aggregate metrics
    skip_ct = sum(f['skipped_count'] for f in stats['files'].values())
    build_ct= sum(f['construction_count'] for f in stats['files'].values())
    prune_ct= sum(f['deconstruction_count'] for f in stats['files'].values())
    # record
    query_ids.append(qid)
    exec_times.append(dur)
    skipped_counts.append(skip_ct)
    built_counts.append(build_ct)
    pruned_counts.append(prune_ct)
    # budget trajectories
    for key, b in stats['budgets'].items():
        budgets_over_time[key].append(b)

# run workload
for q in QUERY_LIST:
    run_and_record(q)
# warm the skip‐case sidecars
for _ in range(6):
    run_and_record(QUERY_LIST[0])

# plotting
plt.figure()
plt.plot(query_ids, exec_times, 'o-')    
plt.xlabel('Query ID')
plt.ylabel('Exec Time (s)')
plt.title('Execution Time')
plt.show()

plt.figure()
plt.plot(query_ids, skipped_counts, 'x-')
plt.xlabel('Query ID')
plt.ylabel('Skipped Files')
plt.title('Files Skipped')
plt.show()

plt.figure()
for key, series in budgets_over_time.items():
    plt.plot(query_ids, series, label=key)
plt.xlabel('Query ID')
plt.ylabel('Budget')
plt.title('Budget Evolution')
plt.legend(fontsize='small')
plt.show()

plt.figure()
plt.plot(query_ids, built_counts, '^-', label='Built')
plt.plot(query_ids, pruned_counts, 'v-', label='Pruned')
plt.xlabel('Query ID')
plt.ylabel('Count')
plt.title('Index Builds vs Prunes')
plt.legend()
plt.show()