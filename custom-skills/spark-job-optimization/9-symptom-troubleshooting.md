# Symptom-First Troubleshooting Guide

**Purpose:** You have a problem RIGHT NOW. Find your symptom, follow the steps, fix it.

Each section is self-contained. Jump directly to your symptom.

| # | Symptom | Jump To |
|---|---------|---------|
| 1 | [My Spark job is slow](#1-my-spark-job-is-slow) | General slowness |
| 2 | [One task takes much longer than others](#2-one-task-takes-much-longer-than-others) | Data skew |
| 3 | [Job fails with OutOfMemoryError](#3-job-fails-with-outofmemoryerror) | OOM diagnosis |
| 4 | [Shuffle spill is high](#4-shuffle-spill-is-high) | Spill reduction |
| 5 | [Job hangs or makes no progress](#5-job-hangs-or-makes-no-progress) | Hangs/deadlocks |
| 6 | [Executors keep dying](#6-executors-keep-dying) | Container killed |
| 7 | [Too many small files after write](#7-too-many-small-files-after-write) | Small files |
| 8 | [Query plan shows CartesianProduct](#8-query-plan-shows-cartesianproduct) | Cartesian joins |
| 9 | [High GC time](#9-high-gc-time) | GC tuning |
| 10 | [Stage keeps retrying / task failures](#10-stage-keeps-retrying--task-failures) | Shuffle failures |
| 11 | [Broadcast join fails](#11-broadcast-join-fails) | Broadcast issues |
| 12 | [Predicate pushdown not working](#12-predicate-pushdown-not-working) | Pushdown blockers |
| 13 | [Job works on small data but fails on large data](#13-job-works-on-small-data-but-fails-on-large-data) | Scaling issues |
| 14 | [Streaming job falling behind](#14-streaming-job-falling-behind) | Streaming lag |

---

## 1. My Spark job is slow

### What You See

- Job takes significantly longer than expected or than it used to
- Spark UI shows stages completing but overall wall-clock time is high
- No errors — it just runs slowly

### Root Causes (Top 5)

1. **Data skew** — one partition has vastly more data than others
2. **Shuffle spill** — data spilling to disk due to insufficient memory
3. **Missing predicate pushdown** — reading far more data than needed
4. **Wrong join strategy** — sort-merge join on a small table that should be broadcast
5. **Too few/many partitions** — tasks are too large or scheduling overhead dominates

### Diagnosis Steps

**Step 1: Check the Spark UI for the slowest stage.**

Open the Spark UI → Stages tab → sort by Duration. The slowest stage is your bottleneck.

**Step 2: Inside that stage, check the task distribution.**

```
Spark UI → Stages → click slowest stage → Summary Metrics
```

Look at the min/median/max task duration. If max >> median, you have skew (go to [Symptom 2](#2-one-task-takes-much-longer-than-others)).

**Step 3: Check for spill.**

In the same stage detail, look at "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)". If disk spill > 0, go to [Symptom 4](#4-shuffle-spill-is-high).

**Step 4: Check the query plan for missing pushdowns.**

```python
df.explain("formatted")
# Look for:
#   - Scan with no PushedFilters
#   - CartesianProduct (go to Symptom 8)
#   - BroadcastNestedLoopJoin (usually a bug)
#   - SortMergeJoin on a small table (should be BroadcastHashJoin)
```

**Step 5: Check partition count.**

```python
# After a shuffle stage
df_after_join.rdd.getNumPartitions()

# Rule of thumb: target 128MB per partition
# Total data size / 128MB = ideal partition count
```

### Fixes

```python
# Fix 1: Enable AQE (handles many issues automatically)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

# Fix 2: Set partition count for non-AQE clusters
total_data_bytes = spark.read.table("my_table").inputFiles().__len__()  # approximate
spark.conf.set("spark.sql.shuffle.partitions", 200)  # adjust based on data size

# Fix 3: Add predicate pushdown
# BAD: filter after read
df = spark.read.table("events").filter(F.col("date") > "2025-01-01")
# GOOD: filter on partition column pushes down to storage
df = spark.read.table("events").where("event_date > '2025-01-01'")

# Fix 4: Force broadcast on small table
from pyspark.sql.functions import broadcast
result = big_df.join(broadcast(small_df), "key")

# Fix 5: Select only needed columns early
# BAD
df = spark.read.table("wide_table")
result = df.join(other, "id").select("id", "name")
# GOOD
df = spark.read.table("wide_table").select("id", "name")
result = df.join(other, "id")
```

> **Deep dive:** See [1-spark-internals-foundation.md](1-spark-internals-foundation.md) for how Spark executes stages and tasks. See [3-spark-ui-guide.md](3-spark-ui-guide.md) for a complete walkthrough of every Spark UI tab.

---

## 2. One task takes much longer than others

### What You See

- Spark UI → Stage detail shows most tasks completing in seconds, but 1-3 tasks run for minutes or hours
- Summary Metrics shows huge gap between median and max duration
- Summary Metrics shows huge gap between median and max "Shuffle Read Size"
- Overall stage duration equals the slowest task duration

### Root Causes

1. **Data skew** — one key has disproportionately many rows (e.g., `null`, `""`, a default value, a power-law key)
2. **Partition skew** — non-uniform partition sizes from upstream processing
3. **Salted partitioning gone wrong** — salt not applied consistently

### Diagnosis Steps

**Step 1: Confirm it is skew, not a slow node.**

```
Spark UI → Stages → click the stage → Task list → sort by Duration
```

Check if the slow task(s) processed significantly more data (Shuffle Read or Input Size) than others. If the slow task read the same amount but is slow, the issue is a slow node, not skew.

**Step 2: Identify the skewed key.**

```python
from pyspark.sql import functions as F

# For join skew: check key distribution
df.groupBy("join_key") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20)

# Check for null keys (common culprit)
df.filter(F.col("join_key").isNull()).count()

# Check percentiles to understand the distribution
df.groupBy("join_key") \
  .count() \
  .select(
    F.percentile_approx("count", [0.5, 0.9, 0.99, 0.999]).alias("percentiles"),
    F.max("count").alias("max_count"),
    F.avg("count").alias("avg_count")
  ).show(truncate=False)
```

**Step 3: Quantify the skew ratio.**

```python
stats = df.groupBy("join_key").count().agg(
    F.max("count").alias("max_count"),
    F.expr("percentile_approx(count, 0.5)").alias("median_count")
).collect()[0]

skew_ratio = stats["max_count"] / stats["median_count"]
print(f"Skew ratio: {skew_ratio:.1f}x")
# If > 10x, you definitely need skew mitigation
```

### Fixes

**Fix 1: AQE skew join (easiest — try this first).**

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

# AQE considers a partition skewed if it is:
#   - Larger than skewedPartitionFactor * median partition size (default: 5x)
#   - AND larger than skewedPartitionThresholdInBytes (default: 256MB)
# Tune these if AQE isn't catching your skew:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 3)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
```

**Fix 2: Filter out null/default keys before the join.**

```python
# Handle nulls separately
non_null_df = df.filter(F.col("join_key").isNotNull())
null_df = df.filter(F.col("join_key").isNull())

joined = non_null_df.join(other_df, "join_key")
# Handle nulls with business logic, then union back
result = joined.unionByName(null_df.withColumn("other_col", F.lit(None)), allowMissingColumns=True)
```

**Fix 3: Key salting for extreme skew.**

```python
import random

NUM_SALTS = 10  # split the hot key into 10 sub-keys

# Salt the skewed side
salted_left = df.withColumn("salt", (F.rand() * NUM_SALTS).cast("int")) \
               .withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))

# Explode the other side to match all salts
from pyspark.sql.types import ArrayType, IntegerType
salt_array = F.array([F.lit(i) for i in range(NUM_SALTS)])
exploded_right = other_df.withColumn("salt", F.explode(salt_array)) \
                         .withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))

# Join on salted key
result = salted_left.join(exploded_right, "salted_key") \
                    .drop("salt", "salted_key")
```

> **Warning:** Salting replicates the right side NUM_SALTS times. Only salt the specific hot keys, not the entire table, to avoid unnecessary data explosion.

**Fix 4: Pre-aggregate before the join.**

```python
# If you're joining then aggregating, aggregate first to reduce skew impact
pre_agg = large_df.groupBy("join_key", "dimension") \
                  .agg(F.sum("metric").alias("metric_sum"))

# Now join the pre-aggregated (much smaller) result
result = pre_agg.join(lookup_df, "join_key")
```

> **Deep dive:** See [5-join-optimization.md](5-join-optimization.md) for join strategy selection and [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for partition-level skew handling.

---

## 3. Job fails with OutOfMemoryError

### What You See

Depending on the OOM type, you will see different errors:

**Driver OOM:**
```
java.lang.OutOfMemoryError: Java heap space
# Usually in the driver logs, or the SparkContext shuts down entirely
```

**Executor OOM (Java heap):**
```
java.lang.OutOfMemoryError: Java heap space
# In executor stderr, task fails and may retry
```

**Executor OOM (GC overhead):**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
# JVM spent >98% of time in GC, recovering <2% heap
```

**Container killed (off-heap / memoryOverhead):**
```
ExecutorLostFailure (executor X exited caused by one of the running tasks)
Reason: Container killed by YARN for exceeding memory limits.
  10.0 GB of 10.0 GB physical memory used.
```

### Root Causes

| OOM Type | Common Cause |
|----------|-------------|
| Driver heap | `collect()`, `toPandas()`, large broadcast variables, too many accumulator updates |
| Executor heap | Skewed partitions, large records, insufficient `spark.executor.memory` |
| GC overhead | Too many small objects, cache pressure, undersized heap |
| Container killed | Off-heap memory (Netty buffers, Python UDFs, native libs) exceeds `memoryOverhead` |

### Diagnosis Steps

**Step 1: Determine if it is Driver or Executor OOM.**

- If the entire job crashes and SparkContext is lost → **Driver OOM**
- If individual tasks fail but the job retries them → **Executor OOM**
- Check the Executors tab in the Spark UI to see which executor(s) failed

**Step 2: For Driver OOM — find the driver-side operation.**

```python
# Common culprits — search your code for these:
df.collect()              # pulls all data to driver
df.toPandas()             # same, plus conversion overhead
df.rdd.reduce(...)        # pulls result to driver
spark.sparkContext.broadcast(large_var)  # broadcasts from driver memory

# Check driver memory setting
spark.conf.get("spark.driver.memory")  # default is often only 1g
```

**Step 3: For Executor OOM — check task input sizes.**

```
Spark UI → Stages → failed stage → Task list → sort by Input Size / Shuffle Read
```

If the failed task processed vastly more data than others, it is skew (go to [Symptom 2](#2-one-task-takes-much-longer-than-others)).

**Step 4: For Container killed — check memoryOverhead.**

```python
# Current settings
print(spark.conf.get("spark.executor.memory"))         # Java heap
print(spark.conf.get("spark.executor.memoryOverhead"))  # Off-heap overhead

# Total container memory = executor.memory + memoryOverhead
# Default memoryOverhead = max(384MB, 0.10 * executor.memory)
```

### Fixes

**Fix Driver OOM:**

```python
# Fix 1: Increase driver memory
# In cluster config or spark-submit:
# --driver-memory 4g
spark.conf.set("spark.driver.memory", "4g")  # must be set before SparkContext starts

# Fix 2: Avoid collecting large results
# BAD
all_rows = df.collect()
# GOOD — write to storage, then read in smaller chunks
df.write.mode("overwrite").parquet("/tmp/results")

# Fix 3: Avoid toPandas on large DataFrames
# BAD
pdf = big_df.toPandas()
# GOOD — use Arrow-optimized toPandas with a limit
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
pdf = big_df.limit(100_000).toPandas()

# Fix 4: For large broadcast variables, let Spark broadcast the table instead
# BAD — manual broadcast of a dict
big_dict = {row["key"]: row["val"] for row in lookup_df.collect()}  # driver OOM risk
# GOOD
result = main_df.join(broadcast(lookup_df), "key")
```

**Fix Executor OOM:**

```python
# Fix 1: Increase executor memory
# In cluster config:
# spark.executor.memory  8g  (up from default)

# Fix 2: Increase parallelism to reduce per-task data
spark.conf.set("spark.sql.shuffle.partitions", 400)  # default is 200

# Fix 3: Reduce per-partition size by repartitioning
df = df.repartition(500, "key_column")

# Fix 4: Reduce cache pressure — unpersist when done
df.cache()
# ... use df ...
df.unpersist()
```

**Fix Container killed (memoryOverhead):**

```python
# Fix 1: Increase memoryOverhead
# In cluster config:
# spark.executor.memoryOverhead  2048   (in MB)

# Fix 2: For PySpark with heavy Python UDFs, allocate more overhead
# spark.executor.memoryOverhead  3072

# Fix 3: For Pandas UDFs / Arrow operations
# spark.executor.pyspark.memory  1g  (separate Python memory pool)
```

> **Deep dive:** See [7-memory-and-spill.md](7-memory-and-spill.md) for the full Spark memory model and tuning formulas.

---

## 4. Shuffle spill is high

### What You See

```
Spark UI → Stages → stage detail:
  Shuffle Spill (Memory): 50.0 GB
  Shuffle Spill (Disk):   12.3 GB
```

- Task durations are longer than expected
- Executor disk I/O is high (check Ganglia or cluster metrics)
- Tasks show yellow/orange in the timeline view (GC time is elevated)

### Root Causes

Spill happens when a task's working data (for sort, aggregation, or join) exceeds its share of execution memory. The data is serialized and written to disk, then read back when needed.

1. **Partitions too large** — too few partitions for the data size
2. **Insufficient executor memory** — not enough memory per task
3. **Skew** — one partition much larger than others (even if average partition size is fine)
4. **Too many cores per executor** — each core gets `executionMemory / cores` share

### Diagnosis Steps

**Step 1: Calculate partition sizes.**

```python
# After a shuffle stage
num_partitions = df.rdd.getNumPartitions()
total_size_bytes = spark.sql("SELECT SUM(size) FROM (DESCRIBE DETAIL my_table)").collect()[0][0]
avg_partition_mb = (total_size_bytes / num_partitions) / (1024 * 1024)
print(f"Avg partition size: {avg_partition_mb:.0f} MB across {num_partitions} partitions")
# Target: 100-200 MB per partition
```

**Step 2: Check execution memory per task.**

```python
executor_memory_mb = 8192  # spark.executor.memory in MB
executor_cores = 4         # spark.executor.cores
memory_fraction = 0.6      # spark.memory.fraction (default)
storage_fraction = 0.5     # spark.memory.storageFraction (default)

# Usable heap ≈ executor_memory * 0.93 (after JVM overhead)
usable_heap = executor_memory_mb * 0.93
unified_memory = usable_heap * memory_fraction
execution_memory_per_task = unified_memory * (1 - storage_fraction) / executor_cores
print(f"Execution memory per task: {execution_memory_per_task:.0f} MB")
# If your average partition size exceeds this, you will spill
```

**Step 3: Check the spill ratio.**

```
Spark UI → Stage detail → Summary Metrics
Look at: Shuffle Spill (Disk) vs Shuffle Spill (Memory)
```

- **Spill (Memory)**: the in-memory size of data before spilling
- **Spill (Disk)**: the serialized (compressed) size written to disk
- A high Memory:Disk ratio (e.g., 10:1) means data compresses well — the actual disk I/O penalty is moderate
- Any spill > 0 means degraded performance, but small spill (<5% of total shuffle) is often tolerable

### Fixes

```python
# Fix 1: Increase partition count (most common fix)
spark.conf.set("spark.sql.shuffle.partitions", 500)
# Or with AQE:
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# Fix 2: Increase executor memory
# In cluster config: spark.executor.memory  16g

# Fix 3: Reduce cores per executor (gives each task more memory)
# Trade parallelism for memory:
# spark.executor.cores  2  (down from 4)

# Fix 4: Filter data earlier to reduce shuffle volume
# BAD: filter after join (shuffles everything)
result = big_df.join(other_df, "key").filter(F.col("date") > "2025-01-01")
# GOOD: filter before join (shuffles less data)
result = big_df.filter(F.col("date") > "2025-01-01").join(other_df, "key")

# Fix 5: Pre-aggregate to reduce data volume before shuffle
# BAD
result = detail_df.join(lookup_df, "key").groupBy("category").sum("amount")
# GOOD — aggregate first, then join the smaller result
pre_agg = detail_df.groupBy("key").agg(F.sum("amount").alias("total"))
result = pre_agg.join(lookup_df, "key").groupBy("category").sum("total")
```

> **Deep dive:** See [7-memory-and-spill.md](7-memory-and-spill.md) for the unified memory model and [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for partition sizing strategies.

---

## 5. Job hangs or makes no progress

### What You See

- Spark UI shows a stage with active tasks but no progress on bytes read/written
- Job tab shows a stage that has been running for much longer than expected
- Executor tab shows executors alive but not processing data
- Driver logs may show no new log lines
- Progress bar (if visible) is stuck

### Root Causes

1. **Driver GC pause** — driver stuck in garbage collection, cannot schedule tasks
2. **Executor loss** — executors died silently, driver waiting for heartbeat timeout
3. **Network issues** — shuffle fetch hanging on unresponsive node
4. **Resource contention** — waiting for cluster resources (YARN queue, K8s pods)
5. **Deadlock in UDF** — user code holding a lock
6. **Large broadcast** — driver stuck serializing/broadcasting a large variable

### Diagnosis Steps

**Step 1: Check if the driver is alive and responsive.**

- Can you access the Spark UI? If yes → driver is alive.
- If the Spark UI is unresponsive → driver may be in a GC pause or OOM. Check driver logs for GC messages.

**Step 2: Check the Executors tab.**

```
Spark UI → Executors tab
```

- Are all executors alive? Look for "Dead" or "Removed" executors.
- If executors are dying, go to [Symptom 6](#6-executors-keep-dying).
- Check "GC Time" column — if any executor shows GC time close to its uptime, it is stuck in GC.

**Step 3: Take a thread dump.**

```
Spark UI → Executors tab → Thread Dump (link in the executor row)
```

Look for:
- Threads in `BLOCKED` or `WAITING` state
- Threads stuck in `shuffle.FetchFailedException` retries
- Threads stuck in UDF code (look for your package names in the stack trace)

**Step 4: Check for resource starvation.**

```
Spark UI → Jobs tab → look for pending stages
```

If stages show "0/200 (0 active, 0 completed)" — no tasks are being scheduled. Check:
- Are there enough executors? Executors tab → count active executors.
- Is dynamic allocation waiting to acquire executors?
- Check cluster manager UI (YARN ResourceManager or K8s dashboard) for pending containers.

**Step 5: Check for shuffle-related hangs.**

```
Spark UI → Stages → active stage → Task list
```

If tasks show progress on "Shuffle Read" but are stuck at a specific byte count — a shuffle service may be unresponsive.

### Fixes

```python
# Fix 1: Increase heartbeat timeouts to avoid false executor death
spark.conf.set("spark.executor.heartbeatInterval", "60s")  # default: 10s
spark.conf.set("spark.network.timeout", "600s")             # default: 120s

# Fix 2: Increase driver memory if driver is GC'ing
# spark.driver.memory  4g

# Fix 3: If broadcast is hanging, reduce broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # default: 10MB
# Or disable auto-broadcast entirely:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Fix 4: If shuffle fetch is hanging, tune retry settings
spark.conf.set("spark.shuffle.io.maxRetries", "5")       # default: 3
spark.conf.set("spark.shuffle.io.retryWait", "10s")      # default: 5s
spark.conf.set("spark.shuffle.io.connectionTimeout", "120s")

# Fix 5: Enable speculation to work around slow nodes
spark.conf.set("spark.speculation", True)
spark.conf.set("spark.speculation.multiplier", "1.5")
spark.conf.set("spark.speculation.quantile", "0.9")
```

> **Deep dive:** See [3-spark-ui-guide.md](3-spark-ui-guide.md) for navigating thread dumps and executor diagnostics. See [4-databricks-diagnostics.md](4-databricks-diagnostics.md) for cluster-level monitoring.

---

## 6. Executors keep dying

### What You See

```
# In driver logs:
Lost executor 3 on 10.0.1.5: Container killed by YARN for exceeding memory limits.
  12.5 GB of 10.0 GB physical memory used.

# Or on Kubernetes:
Lost executor 7 on 10.0.2.3:
  Container exited with a non-zero exit code 137.

# Or Databricks:
Executor 5 removed: Executor heartbeat timed out after 120000 ms
```

- Spark UI → Executors tab shows executors appearing and disappearing
- Stage retries increasing
- Job duration much longer than expected due to recomputation

### Root Causes

1. **memoryOverhead too low** — off-heap memory (Netty shuffle, Python, native libraries) exceeds the container margin
2. **Spot/Preemptible instance eviction** — cloud provider reclaiming the instance
3. **Disk space exhaustion** — local scratch disk full from shuffle files or temp data
4. **Hardware failure** — bad node in the cluster
5. **Heavy Python UDFs** — Python workers consume memory outside JVM heap

### Diagnosis Steps

**Step 1: Check the error message.**

The error message tells you the cause:
- "exceeding memory limits" → memoryOverhead (Step 2)
- "exit code 137" (K8s) → OOM killed by kernel or preempted
- "exit code 143" → SIGTERM, graceful shutdown (preemption or scale-down)
- "heartbeat timed out" → executor process crashed or node became unreachable

**Step 2: For memory limit exceeded — check actual usage.**

```python
# Check your current settings
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")
print(f"Memory overhead: {spark.conf.get('spark.executor.memoryOverhead', 'default (max(384MB, 0.1 * executor.memory))')}")
print(f"PySpark memory: {spark.conf.get('spark.executor.pyspark.memory', 'not set')}")

# Total container = executor.memory + memoryOverhead + pyspark.memory
```

**Step 3: For spot instance preemption — check cluster event logs.**

```
Databricks: Cluster → Event Log → look for "SPOT_INSTANCE_TERMINATION"
AWS EMR: Check EC2 console for Spot interruption notices
```

**Step 4: For disk exhaustion — check local disk usage.**

```
Spark UI → Executors tab → check "Disk Used" column
```

Shuffle files, cached RDD blocks, and temp files all consume local disk. If executors on specific nodes keep dying, the node's disk may be full.

**Step 5: Check if it is always the same node.**

```
Spark UI → Executors tab → look at Host column for failed executors
```

If the same host keeps losing executors → bad node. Decommission it.

### Fixes

```python
# Fix 1: Increase memoryOverhead
# spark.executor.memoryOverhead  2048   (MB)
# For PySpark-heavy workloads:
# spark.executor.memoryOverhead  3072

# Fix 2: Handle spot instance preemption
# Use a mix of on-demand and spot instances
# On Databricks: use "first on demand" + spot with fallback
# Enable graceful decommissioning:
spark.conf.set("spark.decommission.enabled", True)

# Fix 3: Increase local disk (Databricks)
# Use storage-optimized instance types (i3, i3en) for shuffle-heavy jobs
# Or increase EBS volume size in cluster configuration

# Fix 4: Reduce shuffle data to reduce disk pressure
spark.conf.set("spark.shuffle.compress", True)         # default: true
spark.conf.set("spark.shuffle.spill.compress", True)   # default: true

# Fix 5: External shuffle service (keeps shuffle files when executors die)
spark.conf.set("spark.shuffle.service.enabled", True)
# On Databricks, this is enabled by default

# Fix 6: For Python UDF memory — allocate explicit Python memory
# spark.executor.pyspark.memory  1g
```

> **Warning:** When executors die mid-shuffle, all shuffle data on that executor is lost and must be recomputed. This can cascade — use external shuffle service and graceful decommissioning to mitigate.

> **Deep dive:** See [4-databricks-diagnostics.md](4-databricks-diagnostics.md) for cluster event log analysis and [7-memory-and-spill.md](7-memory-and-spill.md) for memory architecture.

---

## 7. Too many small files after write

### What You See

```python
# Check file count and average size
detail = spark.sql("DESCRIBE DETAIL my_table").select("numFiles", "sizeInBytes").collect()[0]
num_files = detail["numFiles"]
total_bytes = detail["sizeInBytes"]
avg_file_mb = (total_bytes / num_files) / (1024 * 1024)
print(f"Files: {num_files}, Avg size: {avg_file_mb:.1f} MB")
# Problem: avg_file_mb < 32 MB or num_files is in the thousands for a small table
```

- Downstream reads are slow due to high overhead per file (metadata, file open/close)
- Cloud storage listing calls are expensive with thousands of files
- Delta table `DESCRIBE HISTORY` shows many small commits

### Root Causes

1. **High partition count at write time** — each task writes one file per partition
2. **Frequent appends** — many small batch writes instead of fewer large writes
3. **Partitioned writes with high cardinality** — `partitionBy("high_cardinality_col")` creates files per partition value × task
4. **Streaming microbatches** — each trigger writes small files

### Diagnosis Steps

**Step 1: Check file size distribution.**

```python
from pyspark.sql import functions as F

files_df = spark.createDataFrame(
    [(f,) for f in spark.read.table("my_table").inputFiles()],
    ["file_path"]
)
# On Databricks with Delta, you can use the Delta log directly:
file_stats = spark.sql("""
    SELECT
        COUNT(*) as num_files,
        ROUND(AVG(size) / 1048576, 1) as avg_mb,
        ROUND(MIN(size) / 1048576, 1) as min_mb,
        ROUND(MAX(size) / 1048576, 1) as max_mb,
        ROUND(SUM(size) / 1073741824, 2) as total_gb
    FROM (DESCRIBE DETAIL my_table)
""")
file_stats.show()
```

**Step 2: Check partition count before write.**

```python
df_to_write.rdd.getNumPartitions()
# If this is 200 (the default shuffle partition count) and your data is 1 GB,
# that's ~5 MB per file — too small
```

### Fixes

**Fix 1: Databricks optimizeWrite (best for Delta tables).**

```python
# Automatically right-sizes files at write time
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
# Or per-table:
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)")
```

**Fix 2: Databricks Auto Compaction.**

```python
# Automatically runs OPTIMIZE after writes
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
# Or per-table:
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES (delta.autoOptimize.autoCompact = true)")
```

**Fix 3: Run OPTIMIZE manually.**

```sql
-- Compact small files into ~1 GB target files
OPTIMIZE my_table;

-- With Z-ORDER for query performance
OPTIMIZE my_table ZORDER BY (date, customer_id);
```

**Fix 4: Coalesce before writing.**

```python
# Reduce partition count to produce fewer, larger files
target_files = max(1, total_data_bytes // (128 * 1024 * 1024))  # ~128 MB per file
df.coalesce(target_files).write.mode("overwrite").saveAsTable("my_table")
```

> **Warning:** `coalesce()` does not trigger a shuffle — it merges partitions on existing executors, which can cause data skew. Use `repartition()` if you need an even distribution. But `repartition()` adds a shuffle, so for write-only purposes, `coalesce()` is usually preferred.

**Fix 5: For partitioned writes — limit the partition cardinality.**

```python
# BAD: partitioning by a high-cardinality column
df.write.partitionBy("user_id").saveAsTable("events")  # millions of directories!

# GOOD: partition by a low-cardinality time column
df.write.partitionBy("event_date").saveAsTable("events")
# With Delta, use Liquid Clustering instead of partitioning:
spark.sql("""
    CREATE TABLE events CLUSTER BY (event_date, region)
    AS SELECT * FROM events_raw
""")
```

> **Deep dive:** See [8-io-and-storage.md](8-io-and-storage.md) for file format sizing, Delta OPTIMIZE internals, and Liquid Clustering.

---

## 8. Query plan shows CartesianProduct

### What You See

```python
df.explain("formatted")
# Output includes:
# +- CartesianProduct
# or
# +- BroadcastNestedLoopJoin Cross, BuildRight
```

- Query runs extremely slowly or OOMs
- Task count explodes (N × M rows for two tables of size N and M)

### Root Causes

1. **Missing join condition** — joining two DataFrames without specifying the key
2. **Non-equi join** — join condition uses `<`, `>`, `!=` instead of `==` (cannot use hash/sort-merge)
3. **Cross join** — intentional or accidental `CROSS JOIN`
4. **Implicit cross join** — comma-separated tables in SQL `FROM` without `WHERE`

> **Warning:** A CartesianProduct is almost always a bug. Even with two tables of 100K rows each, the result is 10 billion rows. This is rarely what you want.

### Diagnosis Steps

**Step 1: Check the explain plan to confirm.**

```python
df.explain("formatted")
# Look for CartesianProduct or BroadcastNestedLoopJoin with joinType=Cross
```

**Step 2: Trace back to find the problematic join.**

```python
# Check your join condition
# BAD — missing key
result = df1.join(df2)
result = df1.crossJoin(df2)

# BAD — non-equi join (may produce CartesianProduct)
result = df1.join(df2, df1["timestamp"] < df2["timestamp"])

# SUSPICIOUS — join on boolean or low-cardinality column
result = df1.join(df2, "is_active")  # only True/False = almost a cross join
```

**Step 3: Estimate the result size.**

```python
left_count = df1.count()
right_count = df2.count()
print(f"Cross join would produce: {left_count * right_count:,} rows")
# If this number is > 1 billion, it will almost certainly fail
```

### Fixes

**Fix 1: Add the correct join condition.**

```python
# BAD
result = orders.join(customers)
# GOOD
result = orders.join(customers, orders["customer_id"] == customers["id"])
```

**Fix 2: For non-equi joins — restructure the query.**

```python
# BAD — range join produces CartesianProduct
result = events.join(windows,
    (events["ts"] >= windows["start"]) & (events["ts"] < windows["end"])
)

# GOOD — use a bucketing/rounding approach to add an equi-join column
events_bucketed = events.withColumn("time_bucket", F.date_trunc("hour", "ts"))
windows_bucketed = windows.withColumn("time_bucket", F.date_trunc("hour", "start"))
result = events_bucketed.join(windows_bucketed, "time_bucket") \
    .filter((events_bucketed["ts"] >= windows_bucketed["start"]) &
            (events_bucketed["ts"] < windows_bucketed["end"]))
```

**Fix 3: If the cross join is intentional, make it explicit and filter early.**

```python
# If you actually need a cross join (rare), be explicit:
spark.conf.set("spark.sql.crossJoin.enabled", True)

# Always filter aggressively on both sides BEFORE the cross join
small_a = df1.filter(F.col("status") == "active")  # reduce to smallest possible set
small_b = df2.filter(F.col("region") == "US")
result = small_a.crossJoin(small_b)
```

> **Deep dive:** See [5-join-optimization.md](5-join-optimization.md) for join strategy selection and how Spark chooses between BroadcastHashJoin, SortMergeJoin, and CartesianProduct. See [2-reading-query-plans.md](2-reading-query-plans.md) for how to read join nodes in explain output.

---

## 9. High GC time

### What You See

```
Spark UI → Stages → stage detail → Summary Metrics:
  GC Time: min=2.5s, median=8.1s, max=45.3s
  Task Duration: min=5.0s, median=12.0s, max=50.0s
```

- GC time is **>10% of task time** (this is the threshold for concern)
- Task timeline shows red bars (GC) dominating green bars (compute)
- Executors tab shows high "GC Time" relative to "Task Time"

### Root Causes

1. **Executor heap too small** for the data being processed per task
2. **Cache pressure** — too much data cached, leaving no room for execution
3. **Object-heavy operations** — UDFs creating many small objects
4. **Large records** — individual rows with huge strings or arrays
5. **Wrong GC algorithm** — default GC not ideal for large heaps

### Diagnosis Steps

**Step 1: Quantify GC overhead per task.**

```
Spark UI → Stages → click stage → Summary Metrics
Calculate: GC Time / Task Duration
```

| GC Ratio | Assessment |
|----------|-----------|
| < 5% | Normal, no action needed |
| 5-10% | Borderline, monitor but not urgent |
| 10-20% | Problematic, tune GC or increase memory |
| > 20% | Severe, job is spending more time on GC than useful work |

**Step 2: Check if cache is the cause.**

```
Spark UI → Storage tab
```

If you see large cached RDDs/DataFrames consuming most of the available storage memory, they may be crowding out execution memory.

**Step 3: Check executor GC logs (if verbose GC is enabled).**

```python
# Enable verbose GC logging
# spark.executor.extraJavaOptions  -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
```

### Fixes

**Fix 1: Switch to G1GC (recommended for heaps > 4GB).**

```python
# In cluster config:
# spark.executor.extraJavaOptions  -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16m

# For driver too:
# spark.driver.extraJavaOptions  -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
```

**Fix 2: Increase executor memory to reduce GC pressure.**

```python
# Give more headroom
# spark.executor.memory  16g  (up from 8g)

# Alternatively, reduce cores per executor to give each task more memory
# spark.executor.cores  2  (down from 4)
```

**Fix 3: Reduce object creation — eliminate UDFs where possible.**

```python
# BAD — Python UDF creates many Python objects
@udf(StringType())
def clean_name(name):
    return name.strip().lower().replace("  ", " ")

df = df.withColumn("clean", clean_name("name"))

# GOOD — built-in functions, no object creation overhead
df = df.withColumn("clean", F.lower(F.trim(F.regexp_replace("name", "\\s+", " "))))
```

**Fix 4: Evict unnecessary caches.**

```python
# Check what's cached
for (id, rdd) in spark.sparkContext._jsc.sc().getPersistentRDDs().items():
    print(f"RDD {id}: {rdd.name()}")

# Unpersist DataFrames you no longer need
old_df.unpersist()

# Or clear all caches
spark.catalog.clearCache()
```

**Fix 5: Reduce per-task data size.**

```python
# Increase partition count = less data per task = less GC pressure
spark.conf.set("spark.sql.shuffle.partitions", 500)

# Or repartition your input
df = df.repartition(500)
```

> **Deep dive:** See [7-memory-and-spill.md](7-memory-and-spill.md) for the unified memory model, storage vs execution memory, and GC tuning recipes.

---

## 10. Stage keeps retrying / task failures

### What You See

```
# In Spark UI → Stages tab:
Stage 5 (attempt 3)  — multiple attempts of the same stage

# In logs:
FetchFailedException: Failed to connect to host:port
org.apache.spark.shuffle.FetchFailedException:
  Missing an output location for shuffle 3 partition 47

# Or:
TaskKilled (another attempt succeeded)
ExecutorLostFailure (executor X exited)
```

- Stages show multiple attempts in the Spark UI
- Individual tasks show "FAILED" status then retry
- Job eventually succeeds (after retries) or fails after max retries exceeded

### Root Causes

1. **FetchFailedException** — shuffle file lost because the executor that wrote it died
2. **Network timeouts** — shuffle fetch timed out due to slow network or overloaded executor
3. **Executor OOM during shuffle** — executor died mid-shuffle, losing shuffle output
4. **Disk full** — local disk ran out of space for shuffle files
5. **Speculative execution conflict** — speculative task completed, original killed

### Diagnosis Steps

**Step 1: Read the error message in failed tasks.**

```
Spark UI → Stages → click the failed stage → Task list → sort by Status (FAILED first)
Click the "Errors" column to see the full stack trace.
```

**Step 2: For FetchFailedException — find the lost executor.**

The error message includes which executor's shuffle output was missing. Check:
```
Spark UI → Executors → look for Dead/Removed executors
```
If an executor died → its shuffle data was lost → all tasks that need to read that shuffle data fail.

**Step 3: Check retry configuration.**

```python
print(spark.conf.get("spark.stage.maxConsecutiveAttempts"))  # default: 4
print(spark.conf.get("spark.task.maxFailures"))               # default: 4
```

### Fixes

**Fix 1: Enable external shuffle service (prevents data loss when executors die).**

```python
spark.conf.set("spark.shuffle.service.enabled", True)
# On Databricks, this is enabled by default.
# The external shuffle service stores shuffle files independently of executors.
```

**Fix 2: Increase network and fetch timeouts.**

```python
spark.conf.set("spark.shuffle.io.maxRetries", "10")          # default: 3
spark.conf.set("spark.shuffle.io.retryWait", "30s")          # default: 5s
spark.conf.set("spark.shuffle.io.connectionTimeout", "300s") # default: 120s
spark.conf.set("spark.network.timeout", "600s")              # default: 120s
```

**Fix 3: Reduce shuffle data to reduce pressure on shuffle service.**

```python
# Compress shuffle data (default: true, but verify)
spark.conf.set("spark.shuffle.compress", True)

# Filter and select before shuffles
df = df.select("needed_col1", "needed_col2") \
       .filter(F.col("date") > "2025-01-01")
# THEN join/aggregate (triggers shuffle)
```

**Fix 4: Increase task retry limits for transient failures.**

```python
# Allow more task failures before giving up
spark.conf.set("spark.task.maxFailures", "8")  # default: 4

# Allow more stage attempts
spark.conf.set("spark.stage.maxConsecutiveAttempts", "8")  # default: 4
```

**Fix 5: Address the underlying executor instability.**

If executors keep dying, solve that first — see [Symptom 6](#6-executors-keep-dying).

> **Deep dive:** See [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for shuffle architecture and external shuffle service details.

---

## 11. Broadcast join fails

### What You See

```
# Timeout error:
org.apache.spark.sql.errors.QueryExecutionErrors:
  Could not execute broadcast in 300 sec.
  You can increase the timeout or disable broadcast join by setting
  spark.sql.broadcastTimeout or spark.sql.autoBroadcastJoinThreshold

# Or driver OOM when broadcasting:
java.lang.OutOfMemoryError: Java heap space
# (during BroadcastExchangeExec)
```

- Query plan shows `BroadcastHashJoin` but the "broadcast" table is actually large
- Spark UI shows the broadcast exchange stage taking very long or failing

### Root Causes

1. **Table larger than expected** — statistics are stale or wrong, Spark thinks the table is small
2. **Broadcast timeout too short** — the table fits in memory but takes >300s to broadcast to all executors
3. **Driver OOM** — the broadcast table is collected to the driver first, then broadcast. If it exceeds driver memory, OOM.
4. **Explicit broadcast hint on a large table** — code uses `broadcast()` on a table that is actually large

### Diagnosis Steps

**Step 1: Check the actual size of the table being broadcast.**

```python
# Check what Spark thinks the table size is (from stats)
spark.sql("DESCRIBE EXTENDED my_lookup_table").show(100, truncate=False)

# Check the actual size
spark.sql("ANALYZE TABLE my_lookup_table COMPUTE STATISTICS").show()
actual_size = spark.sql("DESCRIBE DETAIL my_lookup_table").select("sizeInBytes").collect()[0][0]
print(f"Actual size: {actual_size / (1024*1024):.0f} MB")

# Check the broadcast threshold
print(f"Broadcast threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
# Default: 10MB. If actual size > this, Spark shouldn't auto-broadcast.
# But stale stats can cause Spark to misjudge.
```

**Step 2: Check if a broadcast hint is forcing the broadcast.**

```python
# Search your code for explicit broadcast hints:
# broadcast(df)
# /*+ BROADCAST(table) */
# /*+ BROADCASTJOIN(table) */
# /*+ MAPJOIN(table) */
```

**Step 3: Verify the size after filters.**

```python
# The table might be small in storage but large after joins/transformations
# Check the size of what is actually being broadcast:
broadcast_candidate = spark.read.table("lookup").filter("active = true")
print(f"Rows: {broadcast_candidate.count()}")
# Estimate in-memory size (very rough):
sample_size = broadcast_candidate.limit(1000).toPandas().memory_usage(deep=True).sum()
estimated_total = (sample_size / 1000) * broadcast_candidate.count()
print(f"Estimated in-memory size: {estimated_total / (1024**2):.0f} MB")
```

### Fixes

**Fix 1: Increase broadcast timeout.**

```python
# If the table does fit but is slow to broadcast
spark.conf.set("spark.sql.broadcastTimeout", "600")  # seconds, default: 300
```

**Fix 2: Disable auto-broadcast and let Spark use SortMergeJoin.**

```python
# Turn off auto broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Or reduce the threshold to only broadcast very small tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "5MB")
```

**Fix 3: Increase driver memory if the broadcast table legitimately fits.**

```python
# spark.driver.memory  8g  (to accommodate broadcast + driver overhead)
```

**Fix 4: Remove incorrect broadcast hints.**

```python
# BAD — forcing broadcast on a large table
result = big_df.join(broadcast(also_big_df), "key")

# GOOD — let Spark decide or use SortMergeJoin
result = big_df.join(also_big_df, "key")
```

**Fix 5: Refresh statistics so Spark makes correct decisions.**

```sql
-- Recompute table statistics
ANALYZE TABLE my_lookup_table COMPUTE STATISTICS;
ANALYZE TABLE my_lookup_table COMPUTE STATISTICS FOR ALL COLUMNS;
```

**Fix 6: Pre-filter the broadcast table to reduce its size.**

```python
# If you only need certain rows from the lookup table
small_lookup = lookup_df.filter(F.col("region") == "US")
result = big_df.join(broadcast(small_lookup), "key")
```

> **Deep dive:** See [5-join-optimization.md](5-join-optimization.md) for broadcast join internals, size thresholds, and when to prefer SortMergeJoin.

---

## 12. Predicate pushdown not working

### What You See

```python
df = spark.read.table("events").filter(F.col("event_date") == "2025-03-01")
df.explain("formatted")

# Expected: PushedFilters: [EqualTo(event_date, 2025-03-01)]
# Actual:   PushedFilters: []   ← pushdown is not happening!
```

- Scan reads far more data than expected (check "Rows Output" in Spark UI SQL tab)
- Job is slow because it reads the entire table instead of just the relevant partition/files

### Root Causes

1. **UDF in filter** — Spark cannot push down UDFs to storage
2. **Type mismatch** — filter column type does not match the storage type
3. **Non-pushdownable operator** — `LIKE` with leading wildcard, `IN` with too many values, complex expressions
4. **Column transformation** — applying a function to the column in the filter
5. **Wrong column name** — case sensitivity issues or column not in the source schema
6. **Data source does not support pushdown** — some data sources (CSV, JSON) have limited pushdown

### Diagnosis Steps

**Step 1: Check the physical plan for PushedFilters.**

```python
df.explain("formatted")
# Look for the Scan node (FileScan parquet, FileScan delta, etc.)
# Check PushedFilters: [...] — this shows what was pushed down
# Check PartitionFilters: [...] — this shows partition pruning
```

**Step 2: Isolate which filter is not pushing down.**

```python
# Test each filter individually
df1 = spark.read.table("events").filter(F.col("event_date") == "2025-03-01")
df1.explain("formatted")  # Does this push down?

df2 = spark.read.table("events").filter(F.col("status") == "active")
df2.explain("formatted")  # Does this push down?
```

**Step 3: Check for type mismatches.**

```python
# Check the schema
spark.read.table("events").printSchema()
# event_date: date   ← but you might be filtering with a string

# Check filter type
# If event_date is a DATE column, filtering with a string may or may not push down
# depending on the implicit cast behavior
```

### Fixes

**Fix 1: Remove UDFs from filter predicates.**

```python
# BAD — UDF blocks pushdown
@udf(BooleanType())
def is_recent(date_val):
    return date_val > datetime(2025, 1, 1)

df = spark.read.table("events").filter(is_recent("event_date"))

# GOOD — built-in expression pushes down
df = spark.read.table("events").filter(F.col("event_date") > "2025-01-01")
```

**Fix 2: Don't apply functions to the filtered column.**

```python
# BAD — function on the column prevents pushdown
df = spark.read.table("events").filter(F.year("event_date") == 2025)

# GOOD — compare against values instead
df = spark.read.table("events").filter(
    (F.col("event_date") >= "2025-01-01") & (F.col("event_date") < "2026-01-01")
)
```

**Fix 3: Fix type mismatches.**

```python
# BAD — comparing string column to integer
df = spark.read.table("events").filter(F.col("zip_code") == 90210)

# GOOD — match the type
df = spark.read.table("events").filter(F.col("zip_code") == "90210")
```

**Fix 4: Use partition columns for filtering.**

```python
# Delta/Parquet tables partitioned by event_date get partition pruning for free
df = spark.read.table("events").filter(F.col("event_date") == "2025-03-01")
# This reads only the event_date=2025-03-01 partition directory — not a pushdown,
# but even better: the data is never read at all.

# Check if partition pruning happened:
df.explain("formatted")
# Look for: PartitionFilters: [isnotnull(event_date), (event_date = 2025-03-01)]
```

**Fix 5: For LIKE with leading wildcard — restructure if possible.**

```python
# BAD — leading wildcard cannot push down
df = df.filter(F.col("email").like("%@gmail.com"))

# GOOD — add a derived column and filter on it
# (at write time)
df = df.withColumn("email_domain", F.split("email", "@").getItem(1))
# (at read time)
df = spark.read.table("users").filter(F.col("email_domain") == "gmail.com")
```

**Fix 6: Use ANALYZE TABLE to ensure statistics are up to date.**

```sql
ANALYZE TABLE events COMPUTE STATISTICS FOR ALL COLUMNS;
```

> **Deep dive:** See [8-io-and-storage.md](8-io-and-storage.md) for pushdown mechanics across file formats and [2-reading-query-plans.md](2-reading-query-plans.md) for reading Scan nodes in query plans.

---

## 13. Job works on small data but fails on large data

### What You See

- Job runs fine on a sample or dev dataset
- Same job fails with OOM, extreme slowness, or shuffle errors on production data
- Often works in notebooks during development but fails in production pipelines

### Root Causes

1. **Auto-broadcast threshold** — small data triggers efficient BroadcastHashJoin; large data falls back to SortMergeJoin (or worse, the table is still broadcast and causes OOM)
2. **Default partition count (200)** — fine for 1 GB, catastrophic for 1 TB
3. **Driver-side operations** — `collect()`, `toPandas()`, `count()` that are fast on small data
4. **Data skew** — not apparent in small uniform samples, severe in real data
5. **Hardcoded configs** — settings tuned for dev cluster don't scale to prod data
6. **Broadcast join flip** — table was under 10 MB in dev, 10 GB in prod

### Diagnosis Steps

**Step 1: Check which operations worked at small scale but fail at large scale.**

```python
# Identify driver-side operations in your code
# These are safe on small data but dangerous at scale:
df.collect()              # pulls all data to driver
df.toPandas()             # same + conversion overhead
df.count()                # OK but causes full scan
df.describe().show()      # triggers full scan
df.distinct().count()     # full shuffle + driver collection
```

**Step 2: Check if join strategies changed.**

```python
# At small scale (< 10MB table), Spark uses BroadcastHashJoin
# At large scale, it falls back to SortMergeJoin — which is fine
# But if you have a broadcast() hint, it will try to broadcast even at large scale → OOM

# Check your plan:
df.explain("formatted")
# Look for BroadcastHashJoin on a table that is now large
```

**Step 3: Check partition count relative to data size.**

```python
num_partitions = df.rdd.getNumPartitions()
# If this is still 200 but your data grew from 1 GB to 500 GB:
# 500 GB / 200 partitions = 2.5 GB per partition — way too big
```

### Fixes

**Fix 1: Scale partition count with data size.**

```python
# Make partition count dynamic
data_size_gb = spark.sql("SELECT sizeInBytes / 1073741824 FROM (DESCRIBE DETAIL my_table)").collect()[0][0]
target_partitions = max(200, int(data_size_gb * 8))  # ~128 MB per partition
spark.conf.set("spark.sql.shuffle.partitions", target_partitions)

# Or just use AQE (handles this automatically):
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
```

**Fix 2: Remove or guard driver-side operations.**

```python
# BAD — works on 1K rows, OOMs on 1B rows
result = df.collect()

# GOOD — sample first if you need to inspect
result = df.limit(1000).collect()

# GOOD — write to storage instead of collecting
df.write.mode("overwrite").saveAsTable("results")
```

**Fix 3: Make broadcast decisions dynamic.**

```python
# BAD — hardcoded broadcast that breaks at scale
result = big_df.join(broadcast(lookup_df), "key")

# GOOD — let Spark decide based on actual table size
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
result = big_df.join(lookup_df, "key")  # Spark will broadcast only if lookup < 10MB

# GOOD — guard the broadcast
lookup_size = spark.sql("DESCRIBE DETAIL lookup_table").select("sizeInBytes").collect()[0][0]
if lookup_size < 100 * 1024 * 1024:  # < 100 MB
    result = big_df.join(broadcast(lookup_df), "key")
else:
    result = big_df.join(lookup_df, "key")  # SortMergeJoin
```

**Fix 4: Replace hardcoded configs with data-aware configs.**

```python
# BAD — settings that worked for dev data
spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.executor.memory", "2g")

# GOOD — settings that scale
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
# Use cluster autoscaling and right-sized instance types
```

**Fix 5: Test with representative data volumes.**

```python
# Generate a production-scale sample for testing
sample_df = prod_df.sample(fraction=0.01)  # 1% sample
print(f"Sample size: {sample_df.count():,} rows")
# Run your pipeline on the sample to catch scaling issues
```

> **Deep dive:** See [1-spark-internals-foundation.md](1-spark-internals-foundation.md) for how Spark's execution model changes with data scale.

---

## 14. Streaming job falling behind

### What You See

- **Batch duration exceeds trigger interval**: each microbatch takes longer to process than the trigger allows
- Streaming metrics show increasing `latency` and growing `inputRowsPerSecond` vs declining `processedRowsPerSecond`
- Spark UI → Structured Streaming tab shows increasing batch processing time
- Checkpoint directory grows as pending offsets accumulate
- Event hubs / Kafka topic lag increases

### Root Causes

1. **Processing too slow for the trigger interval** — microbatch takes 60s but trigger is every 30s
2. **State store growth** — stateful operations (`groupBy().count()`, sessionization) with unbounded state
3. **Watermark too aggressive or missing** — old state never evicted
4. **Insufficient resources** — not enough executors or cores for the input rate
5. **Upstream data burst** — temporary spike in input data rate

### Diagnosis Steps

**Step 1: Check batch processing time vs trigger interval.**

```
Spark UI → Structured Streaming tab → look at "Input Rate" vs "Processing Rate"
```

If processing rate < input rate consistently, the job is falling behind.

```python
# Check your trigger interval
# In your streaming query:
query = df.writeStream \
    .trigger(processingTime="30 seconds") \  # Is this realistic?
    .start()

# Check recent batch durations
query.recentProgress  # Returns list of recent batch progress reports
for p in query.recentProgress[-5:]:
    print(f"Batch {p['batchId']}: "
          f"input={p['numInputRows']}, "
          f"duration={p['batchDuration']}ms, "
          f"processedRowsPerSecond={p.get('processedRowsPerSecond', 'N/A')}")
```

**Step 2: Check state store size (for stateful operations).**

```python
# Check state store metrics in recent progress
for p in query.recentProgress[-3:]:
    state_ops = p.get("stateOperators", [])
    for op in state_ops:
        print(f"State rows: {op.get('numRowsTotal', 'N/A')}, "
              f"Memory: {op.get('memoryUsedBytes', 'N/A')} bytes, "
              f"Rows dropped by watermark: {op.get('numRowsDroppedByWatermark', 'N/A')}")
```

If `numRowsTotal` keeps growing and `numRowsDroppedByWatermark` is 0, your state is unbounded.

**Step 3: Check for watermark issues.**

```python
# Is watermark defined?
# Look in your streaming pipeline for:
df.withWatermark("event_time", "1 hour")
# If there's no withWatermark() call on a stateful operation, state grows forever.
```

### Fixes

**Fix 1: Increase trigger interval to match processing capacity.**

```python
# If batches take 45s, set trigger to at least 60s
query = df.writeStream \
    .trigger(processingTime="60 seconds") \
    .start()
```

**Fix 2: Add or fix watermark to bound state growth.**

```python
# BAD — no watermark, state grows forever
result = df.groupBy(
    F.window("event_time", "10 minutes"),
    "user_id"
).count()

# GOOD — watermark enables state cleanup
result = df \
    .withWatermark("event_time", "1 hour") \
    .groupBy(
        F.window("event_time", "10 minutes"),
        "user_id"
    ).count()
# State for windows older than watermark (1 hour) will be evicted.
```

**Fix 3: Scale resources to match input rate.**

```python
# Increase executor count or cores
# Use Databricks autoscaling clusters:
# Min workers: enough for baseline load
# Max workers: enough for peak load

# Increase maxOffsetsPerTrigger to control batch size
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
# For Kafka source:
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", 100000) \  # limit rows per batch
    .load()
```

**Fix 4: Optimize the microbatch processing.**

```python
# All the batch optimization techniques apply within each microbatch:
# - Push filters early
# - Use broadcast joins for lookups
# - Minimize shuffles
# - Use Delta for output (optimized for streaming writes)

# Use Trigger.AvailableNow for catch-up processing
query = df.writeStream \
    .trigger(availableNow=True) \
    .start()
# Processes all available data in optimized batches, then stops.
```

**Fix 5: Use RocksDB state store for large state.**

```python
# Default state store is in-memory (HashMap) — limited by executor memory
# RocksDB state store spills to disk, handles much larger state
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)
```

> **Deep dive:** For comprehensive streaming diagnosis and tuning, see the **spark-structured-streaming** skill if available. See [3-spark-ui-guide.md](3-spark-ui-guide.md) for the Structured Streaming UI tab.

---

## Quick Reference: Symptom → Most Likely Fix

| Symptom | First Thing To Try |
|---------|-------------------|
| Job is slow | Enable AQE, check for skew and missing pushdowns |
| One slow task | AQE skew join, check for null keys |
| OOM | Identify driver vs executor, increase memory or fix `collect()` |
| High spill | Increase `shuffle.partitions` or executor memory |
| Job hangs | Check executor tab, take thread dump |
| Executors dying | Increase `memoryOverhead`, check for spot eviction |
| Small files | Enable `optimizeWrite`, run `OPTIMIZE` |
| CartesianProduct | Add missing join condition |
| High GC | Switch to G1GC, increase memory, reduce cache |
| Stage retries | Enable external shuffle service, increase timeouts |
| Broadcast fails | Disable auto-broadcast or increase driver memory |
| No pushdown | Remove UDF from filter, fix type mismatch |
| Fails at scale | Use AQE, remove `collect()`, dynamic partition count |
| Streaming lag | Increase trigger interval, add watermark, scale cluster |
