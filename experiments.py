import json, time, glob, os
import quackdb
import matplotlib.pyplot as plt
from collections import defaultdict
import duckdb


SMA_FOLDER = os.path.expanduser('~/Desktop/theses/data/sma')
STATS_JSON = os.path.join(SMA_FOLDER, 'stats.json')

# workload
FILES = sorted(glob.glob("/Users/umutozdemir/Desktop/theses/data/yellow_tripdata_202*-*.parquet"))


def plot_results(experiment_name):
    # plotting
    plt.figure()
    plt.plot(query_ids, exec_times, 'o-')    
    plt.xlabel('Query ID')
    plt.ylabel('Exec Time (s)')
    plt.title(f'Execution Time {experiment_name}')
    plt.savefig(f'exec_time_{experiment_name}.png')

    plt.figure()
    plt.plot(query_ids, skipped_counts, 'x-', label='Skipped')
    plt.plot(query_ids, outlier_retrieved_counts, 'o-', label='Outlier Retrieved')
    plt.xlabel('Query ID')
    plt.ylabel('Count')
    plt.title(f'File Operations {experiment_name}')
    plt.legend()
    plt.savefig(f'files_skipped_{experiment_name}.png')

    plt.figure()
    for key, series in list(budgets_over_time.items())[:3]:
        plt.plot(query_ids, series, label=key)
    plt.xlabel('Query ID')
    plt.ylabel('Budget')
    plt.title(f'Budget Evolution {experiment_name}')
    plt.legend(fontsize='x-small', bbox_to_anchor=(1.02, 1), loc='best', borderaxespad=0.)
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    plt.savefig(f'budget_evolution_{experiment_name}.png', bbox_inches='tight')

    plt.figure()
    plt.plot(query_ids, built_counts, '^-', label='Built')
    plt.plot(query_ids, pruned_counts, 'v-', label='Pruned')
    plt.xlabel('Query ID')
    plt.ylabel('Count')
    plt.title(f'Index Builds vs Prunes {experiment_name}')
    plt.legend()
    plt.savefig(f'index_builds_vs_prunes_{experiment_name}.png')

skip_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 50000"
outlier_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 500"
full_scan_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 20"

# metrics containers
query_ids = []
exec_times = []
skipped_counts = []
built_counts = []
pruned_counts = []
outlier_retrieved_counts = []
budgets_over_time = defaultdict(list)  # predicate_key -> [budget]

def load_stats():
    with open(STATS_JSON) as f:
        return json.load(f)

def run_and_record(q):
    # measure exec time
    start = time.perf_counter()
    quackdb.sql(q)
    dur = time.perf_counter() - start
    # load stats.json
    stats = load_stats()
    qid = stats['current_query_id']
    # aggregate metrics
    skip_ct = sum(f['skipped_count'] for f in stats['files'].values())
    build_ct= sum(f['construction_count'] for f in stats['files'].values())
    prune_ct= sum(f['deconstruction_count'] for f in stats['files'].values())
    outlier_retrieved_ct = sum(f['outlier_retrieved_count'] for f in stats['files'].values())
    # record
    query_ids.append(qid)
    exec_times.append(dur)
    skipped_counts.append(skip_ct)
    outlier_retrieved_counts.append(outlier_retrieved_ct)
    built_counts.append(build_ct)
    pruned_counts.append(prune_ct)
    # budget trajectories
    for key, b in stats['budgets'].items():
        budgets_over_time[key].append(b)


def experiment_1_skip_predicate():
    skip_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 50000"
    run_and_record(skip_predicate_experiment)

def experiment_2_outlier_predicate():
    outlier_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 500"
    run_and_record(outlier_predicate_experiment)

def experiment_3_full_scan():
    full_scan_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 20"
    run_and_record(full_scan_experiment)

def experiment_1_skip_predicate_duckdb():
    duckdb_exec_times = []
    query_ids = []
    skip_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 50000"
    for i in range(1, 11):
        query_ids.append(i)
        start = time.perf_counter()
        duckdb.sql(skip_predicate_experiment)
        dur = time.perf_counter() - start
        duckdb_exec_times.append(dur)
    # plotting
    plt.figure()
    plt.plot(query_ids, duckdb_exec_times, 'o-')    
    plt.xlabel('Query ID')
    plt.ylabel('Exec Time (s)')
    plt.title('Execution Time DuckDB')
    plt.savefig('exp_1_exec_time_duckdb.png')


def experiment_2_outlier_predicate_duckdb():
    duckdb_exec_times = []
    query_ids = []
    outlier_predicate_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 500"
    for i in range(1, 11):
        query_ids.append(i)
        start = time.perf_counter()
        duckdb.sql(outlier_predicate_experiment)
        dur = time.perf_counter() - start
        duckdb_exec_times.append(dur)
    # plotting
    plt.figure()
    plt.plot(query_ids, duckdb_exec_times, 'o-')    
    plt.xlabel('Query ID')
    plt.ylabel('Exec Time (s)')
    plt.title('Execution Time DuckDB for Outlier Predicate')
    plt.savefig('exp_2_exec_time_duckdb.png')


def experiment_3_full_scan_duckdb():
    duckdb_exec_times = []
    query_ids = []
    full_scan_experiment =  f"SELECT payment_type, total_amount FROM read_parquet({FILES}) WHERE total_amount > 20"
    for i in range(1, 11):
        query_ids.append(i)
        start = time.perf_counter()
        duckdb.sql(full_scan_experiment)
        dur = time.perf_counter() - start
        duckdb_exec_times.append(dur)
    # plotting
    plt.figure()
    plt.plot(query_ids, duckdb_exec_times, 'o-')    
    plt.xlabel('Query ID')
    plt.ylabel('Exec Time (s)')
    plt.title('Execution Time DuckDB for Full Scan')
    plt.savefig('exp_3_exec_time_duckdb.png')


#experiment_3_full_scan_duckdb()

##experiment_2_outlier_predicate_duckdb()
#exit()

# let the system build indexes which are helpful
experiment_3_full_scan()

# Wait for 5 minutes (300 seconds)
import time
time.sleep(300)

for i in range(9):
    experiment_3_full_scan()


time.sleep(120)
plot_results("Experiment-3 Full Scan")

# wait for user input
input("Press Enter to continue...")