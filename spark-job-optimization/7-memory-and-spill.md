# 7. Memory Management and Spill

This file covers everything about how Spark uses memory, why spill happens, how to diagnose memory-related issues, and practical techniques for fixing them. Memory misconfiguration is the most common root cause of executor OOMs, excessive GC, and slow jobs due to spill.

---

## 1. Executor Memory Layout

Every Spark executor runs inside a container (YARN, Kubernetes, or Databricks-managed). The total container footprint is larger than just the JVM heap.

### Complete Memory Architecture

```
+------------------------------------------------------------------+
|                    Container Total Memory                         |
|           (what YARN / K8s / Databricks allocates)                |
|                                                                   |
|  +------------------------------------------------------------+  |
|  |                     JVM Heap                                |  |
|  |              (spark.executor.memory)                        |  |
|  |                                                             |  |
|  |  +-------------------------------------------------------+ |  |
|  |  | Reserved Memory (300 MB)                               | |  |
|  |  | (Spark internal objects, metadata, safety margin)      | |  |
|  |  +-------------------------------------------------------+ |  |
|  |                                                             |  |
|  |  +-------------------------------------------------------+ |  |
|  |  | User Memory  (1 - spark.memory.fraction) * Usable     | |  |
|  |  |                                                        | |  |
|  |  | - Your data structures (hash maps, arrays)             | |  |
|  |  | - RDD internal metadata                                | |  |
|  |  | - UDF state                                            | |  |
|  |  | Default: 40% of (heap - 300MB)                         | |  |
|  |  +-------------------------------------------------------+ |  |
|  |                                                             |  |
|  |  +-------------------------------------------------------+ |  |
|  |  | Unified Spark Memory  (spark.memory.fraction) * Usable | |  |
|  |  | Default: 60% of (heap - 300MB)                         | |  |
|  |  |                                                        | |  |
|  |  |  +-------------------------+-------------------------+ | |  |
|  |  |  | Execution Memory        | Storage Memory          | | |  |
|  |  |  |                         |                         | | |  |
|  |  |  | - Shuffles              | - Cached DataFrames     | | |  |
|  |  |  | - Joins                 | - Broadcast variables   | | |  |
|  |  |  | - Sorts                 | - Accumulator results   | | |  |
|  |  |  | - Aggregations          | - Unroll memory         | | |  |
|  |  |  |                         |                         | | |  |
|  |  |  | Initial: 50% of Spark  | Initial: 50% of Spark   | | |  |
|  |  |  | (spark.memory.          | (spark.memory.           | | |  |
|  |  |  |  storageFraction=0.5)   |  storageFraction=0.5)    | | |  |
|  |  |  +-------------------------+-------------------------+ | |  |
|  |  +-------------------------------------------------------+ |  |
|  +------------------------------------------------------------+  |
|                                                                   |
|  +------------------------------------------------------------+  |
|  |                  Off-Heap Memory                            |  |
|  |          (spark.executor.memoryOverhead)                    |  |
|  |                                                             |  |
|  | - JVM overhead (thread stacks, class metadata, JIT)        |  |
|  | - Network I/O buffers (Netty)                               |  |
|  | - PySpark Python worker processes                           |  |
|  | - Arrow serialization buffers                               |  |
|  | - Direct byte buffers for shuffle                           |  |
|  | - Off-heap storage (if spark.memory.offHeap.enabled=true)   |  |
|  |                                                             |  |
|  | Default: max(384MB, 0.10 * spark.executor.memory)           |  |
|  | On Databricks: typically 10% of executor memory             |  |
|  +------------------------------------------------------------+  |
+------------------------------------------------------------------+
```

### Calculating Actual Memory Regions

For an executor with `spark.executor.memory = 10g`:

```
Total Heap            = 10 GB (10,240 MB)
Reserved Memory       = 300 MB
Usable Memory         = 10,240 - 300 = 9,940 MB

Spark Memory (0.6)    = 9,940 * 0.6 = 5,964 MB
  Execution (initial) = 5,964 * 0.5 = 2,982 MB
  Storage (initial)   = 5,964 * 0.5 = 2,982 MB

User Memory (0.4)     = 9,940 * 0.4 = 3,976 MB

Off-Heap Overhead     = max(384, 10,240 * 0.1) = 1,024 MB

Container Total       = 10,240 + 1,024 = 11,264 MB
```

### Dynamic Occupancy (Unified Memory Manager)

Since Spark 1.6, execution and storage share a unified region and can borrow from each other. The key rules:

| Scenario | Behavior |
|----------|----------|
| Execution needs more memory, storage has free space | Execution borrows from storage |
| Storage needs more memory, execution has free space | Storage borrows from execution |
| Execution needs memory back, storage is using it | Storage entries are **evicted** (spilled to disk) to make room |
| Storage needs memory back, execution is using it | Storage **cannot** evict execution -- it must wait or spill itself |

> **Key insight:** Execution memory has priority. It can always reclaim space from storage by evicting cached data, but storage can never force execution to release memory. This is because dropping execution data mid-computation would mean restarting the task, while cached data can always be recomputed.

```
Execution pressure increases -->

  +-------------------+-------------------+
  |  Execution  30%   |  Storage    70%   |   Storage dominant (lots of caching)
  +-------------------+-------------------+

  +-----------------------------+---------+
  |  Execution  70%             | Stor 30%|   Execution reclaims from storage
  +-----------------------------+---------+

  +---------------------------------------+
  |  Execution  100%                      |   Extreme: all cached data evicted
  +---------------------------------------+
```

### Configuration Reference

| Config | Default | Description |
|--------|---------|-------------|
| `spark.executor.memory` | `1g` | JVM heap size per executor |
| `spark.executor.memoryOverhead` | `max(384MB, 0.10 * executor.memory)` | Off-heap memory (container padding) |
| `spark.memory.fraction` | `0.6` | Fraction of (heap - 300MB) for Spark's unified memory |
| `spark.memory.storageFraction` | `0.5` | Initial split within Spark memory for storage (soft boundary) |
| `spark.memory.offHeap.enabled` | `false` | Enable off-heap memory for Tungsten |
| `spark.memory.offHeap.size` | `0` | Off-heap memory size if enabled |
| `spark.driver.memory` | `1g` | JVM heap for the driver |
| `spark.driver.memoryOverhead` | `max(384MB, 0.10 * driver.memory)` | Off-heap memory for the driver |

> **Photon:** Photon uses native (off-heap) memory for its vectorized execution engine. The memory model is fundamentally different -- Photon manages its own memory pool outside the JVM heap. This means GC pressure is significantly reduced, but you must ensure `spark.executor.memoryOverhead` is sized generously (Databricks auto-configures this for Photon-enabled clusters). Photon's memory usage appears in the off-heap region, not in JVM heap metrics.

---

## 2. Understanding Spill

### What Is Spill?

Spill occurs when an operator (sort, aggregation, join, shuffle) cannot fit its intermediate data in execution memory and must write partial results to disk. The data is serialized, written to local disk, and later read back and deserialized when needed.

```
Normal operation:                    Spill:

+------------------+                 +------------------+
|  Execution       |                 |  Execution       |
|  Memory          |                 |  Memory (FULL)   |
|                  |                 |  +-----------+   |
|  [sort buffer]   |                 |  |  partial  |---|---> serialize
|  [hash table]    |                 |  |  data     |   |        |
|                  |                 |  +-----------+   |        v
+------------------+                 +------------------+   +---------+
                                                            | Local   |
                                                            | Disk    |
                                                            | (spill  |
                                                            |  files) |
                                                            +---------+
```

### When Does Spill Occur?

| Operation | Why It Spills | What Gets Written to Disk |
|-----------|--------------|--------------------------|
| **Sort** | Sort buffer exceeds memory allocation | Sorted runs of data |
| **Aggregation** | Hash map for `groupBy` / `agg` grows too large | Partial aggregation results |
| **Sort-Merge Join** | Sort buffers for either side of the join | Sorted partitions |
| **Shuffle Write** | Shuffle map output exceeds memory | Serialized shuffle blocks |
| **Shuffle Read** | Fetched shuffle data exceeds memory | Serialized shuffle blocks |
| **Window Functions** | Window frame buffer exceeds memory | Buffered rows per partition |

### Two Spill Metrics

Spark UI reports two distinct spill numbers:

| Metric | What It Measures | Example |
|--------|-----------------|---------|
| **Spill (Memory)** | The size of data in memory *before* serialization when the spill decision was made. Represents the logical size of the in-memory data structures. | 4.0 GB |
| **Spill (Disk)** | The size of data *after* serialization, written to disk. Smaller than memory due to serialization + compression. | 1.2 GB |

The ratio between memory spill and disk spill tells you about compression effectiveness:

```
Spill (Memory) = 4.0 GB
Spill (Disk)   = 1.2 GB
Ratio          = 3.3x compression

High ratio (> 3x): Data compresses well, spill cost is mainly serialization overhead
Low ratio  (~ 1x): Data is already dense/binary, spill is expensive per byte
```

### Why Spill Is Expensive

Spill adds four costs to a task:

1. **Serialization CPU** -- Converting Java objects to byte arrays
2. **Disk I/O** -- Writing serialized bytes to local SSD/HDD
3. **Deserialization CPU** -- Reconstructing objects when reading back
4. **Merge overhead** -- Sorted spill files must be merged (external sort)

```
Task without spill:    [compute ██████████████████████████] 30s

Task with spill:       [compute ███][spill][compute ███][spill][merge █████] 90s
                                      ↑ ser + disk I/O    ↑ deser + merge
```

Even with fast NVMe SSDs, spill typically makes tasks 2-10x slower. With remote or spinning disks, it can be 10-100x slower.

### Detecting Spill in Spark UI

**Step 1: Stage Details Page**

Navigate to Stages > click a stage > scroll to the task table. Look for:

```
+----------+--------+----------+---------+---------+----------------+----------------+
| Task ID  | Status | Duration | GC Time | Input   | Spill (Memory) | Spill (Disk)   |
+----------+--------+----------+---------+---------+----------------+----------------+
| 142      | SUCCESS| 45s      | 8s      | 128 MB  | 2.1 GB         | 680 MB         |  <-- spilling
| 143      | SUCCESS| 3s       | 0.2s    | 128 MB  | 0              | 0              |  <-- no spill
| 144      | SUCCESS| 120s     | 35s     | 128 MB  | 8.5 GB         | 2.4 GB         |  <-- heavy spill + skew?
+----------+--------+----------+---------+---------+----------------+----------------+
```

**Step 2: Stage Summary Metrics**

At the top of the stage details, check the aggregated summary:

```
Shuffle Spill (Memory): 45.2 GB    <-- total across all tasks
Shuffle Spill (Disk):   12.8 GB    <-- total written to disk
```

**Step 3: SQL Tab**

In the SQL tab, click on a query. The DAG shows spill metrics per operator node:

```
+-- SortMergeJoin
    spill size: 8.5 GB
    +-- Sort
        spill size: 3.2 GB
```

> **Rule of thumb:** If spill (disk) is more than 2x the executor memory, the job is significantly memory-constrained and will benefit from either more memory or better partitioning.

---

## 3. Diagnosing Memory Issues

### Step-by-Step Diagnostic Process

Follow this checklist when you suspect memory problems:

#### Step 1: Check Executor Tab for Memory Usage

```
Spark UI → Executors Tab

Look for:
- Memory Used vs. Memory Available (storage memory utilization)
- Disk Used (indicates active spill)
- Tasks column (active vs. completed vs. failed)
- Any dead executors? (indicates OOM kills)
```

If executors are being lost and re-acquired, this typically means they are being killed by the container manager (YARN/K8s) for exceeding memory limits.

#### Step 2: Check Stage Details for Spill Metrics

```
Spark UI → Stages → [your stage] → Summary Metrics

+--------------------+------+------+--------+------+------+
| Metric             | Min  | 25th | Median | 75th | Max  |
+--------------------+------+------+--------+------+------+
| Duration           | 2s   | 3s   | 4s     | 6s   | 180s |  <-- Max >> Median = skew
| Spill (Memory)     | 0    | 0    | 0      | 1 GB | 12GB |  <-- Concentrated in few tasks
| Spill (Disk)       | 0    | 0    | 0    | 300MB | 3 GB |
| Input Size         | 64MB | 128M | 130MB  | 135M | 2 GB |  <-- Skewed input
+--------------------+------+------+--------+------+------+
```

Large variance between median and max indicates data skew causing selective spill.

#### Step 3: Check GC Time in Task Metrics

```
Spark UI → Stages → [your stage] → Task Metrics

GC Time column:
- Healthy:     GC Time < 5% of Task Duration
- Concerning:  GC Time 5-10% of Task Duration
- Problematic: GC Time > 10% of Task Duration
- Critical:    GC Time > 30% of Task Duration (thrashing)
```

#### Step 4: Check Driver Memory

```
Spark UI → Executors → Driver row

Look for:
- High memory usage on the driver
- Driver storage memory full (broadcast variables cached)
- Driver log for OOM stack traces
```

Common driver memory issues:
```python
# DANGEROUS: pulls all data to driver
df.collect()                    # Entire DataFrame into driver memory
df.toPandas()                   # Entire DataFrame as Pandas in driver
spark.sql("SELECT *").show()    # show() is safe (only 20 rows default)

# SAFE alternatives
df.take(100)                    # Only 100 rows
df.limit(1000).toPandas()       # Bounded conversion
df.write.parquet("/output")     # Write to storage, never touches driver
```

#### Step 5: Identify OOM Patterns

Check the executor stderr logs or the driver log for these stack traces:

| Error Message | Likely Cause |
|--------------|-------------|
| `java.lang.OutOfMemoryError: Java heap space` | JVM heap exhausted (execution or user memory) |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` | GC consuming >98% CPU time, reclaiming <2% memory |
| `java.lang.OutOfMemoryError: Direct buffer memory` | Off-heap NIO buffers exhausted (shuffle/network) |
| `Container killed by YARN for exceeding memory limits` | Total process memory > container allocation |
| `Cannot allocate memory (errno=12)` | OS-level OOM, entire node under pressure |
| `ExecutorLostFailure (executor X exited caused by one of the running tasks)` | Executor killed, often by OOM killer |
| `Python worker exited unexpectedly (crashed)` | Python process ran out of memory |

---

## 4. GC Tuning

### How GC Impacts Spark

Garbage collection pauses directly affect task execution time. During GC, all computation on that executor stops. In Spark:

- Each task produces many short-lived objects (rows being processed)
- Cached data creates long-lived objects in the old generation
- Shuffle buffers create medium-lived objects
- High object churn leads to frequent minor GC
- Large heaps lead to long major GC pauses

### Reading GC Metrics in Spark UI

```
Spark UI → Stages → [stage] → Summary Metrics → GC Time

Also available:
- Executor tab → "Total GC Time" column per executor
- Task-level metrics show individual task GC time
- Environment tab → "spark.executor.extraJavaOptions" shows current GC config
```

### GC Algorithm Recommendations

| GC Algorithm | Best For | Key Characteristics |
|-------------|---------|-------------------|
| **G1GC** | Heaps > 4GB (recommended for most Spark workloads) | Concurrent marking, region-based, predictable pause targets |
| **ParallelGC** | Heaps < 4GB, batch throughput priority | High throughput but longer stop-the-world pauses |
| **ZGC** | Very large heaps (> 32GB), latency-sensitive | Sub-millisecond pauses, higher CPU overhead (Java 15+) |

**Databricks default:** G1GC is the default on Databricks runtimes.

### Key GC Settings

```python
# G1GC configuration (recommended)
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:G1HeapRegionSize=16m "          # Region size (auto-tuned, but 16m is good for large heaps)
    "-XX:InitiatingHeapOccupancyPercent=35 "  # Start concurrent marking earlier
    "-XX:ConcGCThreads=4 "               # Concurrent GC threads
    "-XX:+ParallelRefProcEnabled "        # Parallel reference processing
    "-verbose:gc "                        # Enable GC logging
    "-XX:+PrintGCDetails "               # Detailed GC output
    "-XX:+PrintGCTimeStamps"             # Timestamps in GC log
)

# For Java 11+ (unified logging)
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC "
    "-XX:G1HeapRegionSize=16m "
    "-XX:InitiatingHeapOccupancyPercent=35 "
    "-Xlog:gc*:file=/tmp/gc.log:time,uptime,level,tags"
)
```

### GC Tuning Decision Tree

```
Is GC Time > 10% of task time?
├── No  → GC is fine, look elsewhere for perf issues
└── Yes
    ├── Are you caching a lot of data?
    │   ├── Yes → Reduce cache, use MEMORY_AND_DISK, or increase heap
    │   └── No  → Continue
    ├── Is the heap > 32GB?
    │   ├── Yes → Use compressed oops (-XX:+UseCompressedOops, auto below 32GB)
    │   │         Consider splitting into more executors with smaller heaps
    │   └── No  → Continue
    ├── Are major GC pauses dominating?
    │   ├── Yes → Lower InitiatingHeapOccupancyPercent (e.g., 25)
    │   │         Increase ConcGCThreads
    │   └── No  → Minor GC is the issue
    └── High minor GC?
        └── Increase Young Generation: -XX:NewRatio=2
            Or increase heap if young gen is too small
```

> **Key rule:** When GC time exceeds 10% of task time, you have a problem. When it exceeds 30%, your tasks are spending more time collecting garbage than doing useful work. The solution is almost always: reduce the data processed per task (more partitions), reduce cached data, or increase executor memory.

---

## 5. Common OOM Scenarios and Fixes

| Scenario | Symptoms | Root Cause | Fix |
|----------|----------|-----------|-----|
| **Driver OOM from `collect()`** | `OutOfMemoryError` on driver; job fails at the action stage | `collect()`, `toPandas()`, or `toLocalIterator()` pulling too much data to driver | Use `take(n)`, `limit(n)`, or write results to storage with `df.write` |
| **Driver OOM from broadcast** | `OutOfMemoryError` on driver during broadcast; broadcast variable creation fails | Table exceeds `autoBroadcastJoinThreshold` but driver memory can't hold it; or explicit `broadcast()` of a large table | Reduce `spark.sql.autoBroadcastJoinThreshold`, increase `spark.driver.memory`, or let Spark choose sort-merge join |
| **Executor OOM from data skew** | One or few tasks fail with OOM while most tasks complete quickly; massive variance in task durations | A single partition has disproportionate data (e.g., null keys, hot keys) | Salt join keys, enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled`), repartition on a different key, filter/handle nulls separately |
| **Executor OOM from aggregation** | `OutOfMemoryError` during `groupBy().agg()`; high spill followed by OOM | Too many distinct groups or too many aggregation columns for the hash map to fit in memory | Two-phase aggregation (partial agg then shuffle then final agg), increase `spark.executor.memory`, reduce partition skew |
| **Executor OOM from window functions** | `OutOfMemoryError` during window computation; tasks with specific partition keys fail | Window function must buffer all rows in a partition; one partition is very large | Partition the window by more columns to reduce per-partition size, or pre-filter data |
| **Container killed (YARN/K8s)** | `ExecutorLostFailure`, "Container killed by YARN for exceeding memory limits", exit code 137 | Total process memory (heap + off-heap + overhead) exceeds container allocation | Increase `spark.executor.memoryOverhead`; typically set to 15-25% of executor memory for heavy shuffle/PySpark workloads |
| **Python worker OOM** | "Python worker exited unexpectedly", `MemoryError` in Python stderr | Pandas UDF processing too much data per batch, or Python objects too large | Reduce `spark.sql.execution.arrow.maxRecordsPerBatch` (default 10000), optimize Pandas UDF logic, increase `spark.executor.pyspark.memory` |

### Fix Examples

**Skewed join with salting:**
```python
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

NUM_SALTS = 10

# Salt the skewed (large) table
large_df = large_df.withColumn("salt", (F.rand() * NUM_SALTS).cast(IntegerType()))
large_df = large_df.withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))

# Explode the small table to match all salt values
from pyspark.sql.functions import explode, array, lit
small_df = small_df.withColumn("salt", explode(array([lit(i) for i in range(NUM_SALTS)])))
small_df = small_df.withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))

# Join on salted key
result = large_df.join(small_df, "salted_key", "inner")
result = result.drop("salt", "salted_key")
```

**Two-phase aggregation:**
```python
# Instead of one massive groupBy:
# df.groupBy("high_cardinality_key").agg(F.sum("value"))

# Phase 1: Pre-aggregate within existing partitions (no shuffle)
pre_agg = df.repartition(200, "high_cardinality_key") \
    .groupBy("high_cardinality_key") \
    .agg(F.sum("value").alias("partial_sum"))

# Phase 2: Final aggregation (much smaller data)
result = pre_agg.groupBy("high_cardinality_key") \
    .agg(F.sum("partial_sum").alias("total_sum"))
```

**Safe data collection:**
```python
# BAD: pulls everything to driver
all_data = df.collect()
pdf = df.toPandas()

# GOOD: bounded collection
sample = df.limit(10000).toPandas()
top_records = df.orderBy(F.desc("value")).take(100)

# GOOD: write large results to storage
df.write.mode("overwrite").parquet("/mnt/output/results")

# GOOD: iterate without loading everything at once
for row in df.toLocalIterator():
    process(row)  # one partition at a time (still uses driver memory per partition)
```

---

## 6. Memory Optimization Techniques

### Reducing Memory Footprint

**Column pruning -- select only what you need:**
```python
# BAD: reads all 200 columns, all stay in memory through the plan
df = spark.read.table("huge_table")
result = df.filter(df.status == "active").select("id", "name")

# GOOD: Spark can push column selection down, but being explicit helps
# and makes the plan clearer
df = spark.read.table("huge_table").select("id", "name", "status")
result = df.filter(df.status == "active").select("id", "name")
```

**Filter early -- reduce data volume before expensive operations:**
```python
# BAD: join first, filter later (shuffles unnecessary data)
result = large_df.join(dim_df, "key").filter(F.col("date") > "2025-01-01")

# GOOD: filter before the join (less data to shuffle and join)
filtered = large_df.filter(F.col("date") > "2025-01-01")
result = filtered.join(dim_df, "key")
```

**Use appropriate data types:**
```python
# BAD: strings where integers suffice
df = df.withColumn("category_id", F.col("category_id").cast("string"))

# GOOD: use the smallest sufficient type
from pyspark.sql.types import ShortType, IntegerType
df = df.withColumn("category_id", F.col("category_id").cast(ShortType()))  # 2 bytes vs 40+ bytes for string

# Type memory comparison (approximate per value in JVM):
# boolean   →  1 byte
# byte      →  1 byte
# short     →  2 bytes
# int       →  4 bytes
# long      →  8 bytes
# float     →  4 bytes
# double    →  8 bytes
# string    →  40+ bytes (object header + char array + hash)
# timestamp →  8 bytes
# decimal   →  16 bytes (Decimal128)
```

### Efficient Caching

**When to cache:**
- DataFrame is reused multiple times in the same job
- Recomputation is expensive (complex joins, aggregations)
- Data fits reasonably in memory

**When NOT to cache:**
- DataFrame is used only once
- Data is too large relative to available memory
- The source is already optimized (e.g., Delta with data skipping)

**Storage levels:**

| Storage Level | Heap Used | Disk Used | Serialized | Copies | Use Case |
|--------------|-----------|-----------|------------|--------|----------|
| `MEMORY_ONLY` | Yes | No | No | 1 | Default. Fast access, evicted under pressure. |
| `MEMORY_ONLY_SER` | Yes | No | Yes | 1 | 2-5x more compact, but CPU for ser/deser. Good for large datasets. |
| `MEMORY_AND_DISK` | Yes | Overflow | No | 1 | Recommended for most cases. Falls back to disk gracefully. |
| `MEMORY_AND_DISK_SER` | Yes | Overflow | Yes | 1 | Best for large datasets you want to keep. Compact + disk fallback. |
| `DISK_ONLY` | No | Yes | Yes | 1 | Rare. Only when memory is extremely constrained. |
| `OFF_HEAP` | No | No | Yes | 1 | Uses off-heap memory. Avoids GC. Requires `spark.memory.offHeap.enabled`. |

```python
from pyspark import StorageLevel

# Default cache (MEMORY_AND_DISK in Spark 3.x on Databricks)
df.cache()

# Explicit storage level
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# IMPORTANT: cache is lazy -- the DataFrame is not cached until
# an action triggers computation
df.cache()
df.count()  # THIS triggers the actual caching

# Always unpersist when done
df.unpersist()
```

> **Photon:** Photon uses columnar memory layouts for cached data, which is more memory-efficient than JVM row-based storage. Photon caching is managed outside the JVM heap, so it does not contribute to GC pressure. However, the memory still comes from the off-heap/overhead budget. On Photon clusters, caching is generally less necessary because Photon's native execution is already much faster at recomputation.

### Avoiding Driver Memory Issues

```python
# Pattern: Process results without bringing them to the driver
# -----------------------------------------------------------

# BAD: Collect and write from driver
data = df.collect()
with open("output.csv", "w") as f:
    for row in data:
        f.write(",".join(str(x) for x in row) + "\n")

# GOOD: Write directly from executors
df.write.mode("overwrite").csv("/mnt/output/results")

# BAD: Aggregate in driver
data = df.collect()
total = sum(row["amount"] for row in data)

# GOOD: Aggregate in Spark, collect scalar
total = df.agg(F.sum("amount")).collect()[0][0]

# BAD: Broadcast a huge lookup table
huge_lookup = spark.read.table("huge_reference")  # 5 GB table
result = df.join(F.broadcast(huge_lookup), "key")  # Driver OOM

# GOOD: Let Spark decide join strategy, or increase threshold carefully
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast
result = df.join(huge_lookup, "key")  # Sort-merge join
```

### Off-Heap Tuning for Large Shuffles

When shuffle data is very large, the network layer (Netty) uses direct byte buffers that live outside the JVM heap:

```python
# Increase off-heap for shuffle-heavy workloads
spark.conf.set("spark.executor.memoryOverhead", "4g")

# Enable Tungsten off-heap memory (reduces GC, improves shuffle)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

# Tune Netty's direct memory allocation
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:MaxDirectMemorySize=2g")
```

---

## 7. PySpark Memory Considerations

### PySpark Memory Architecture

PySpark adds additional memory complexity because each executor task spawns a Python worker process that runs outside the JVM:

```
+------------------------------------------------------------------+
|  Container                                                        |
|                                                                   |
|  +---------------------------+  +------------------------------+  |
|  |  JVM Executor             |  |  Python Worker(s)            |  |
|  |                           |  |                              |  |
|  |  - Spark execution        |  |  - Python UDFs               |  |
|  |  - Catalyst optimizer     |  |  - Pandas UDFs               |  |
|  |  - Shuffle management     |  |  - PySpark transformations   |  |
|  |  - Storage management     |  |  - Arrow (de)serialization   |  |
|  |                           |  |                              |  |
|  |  Memory:                  |  |  Memory:                     |  |
|  |  spark.executor.memory    |  |  spark.executor.pyspark.     |  |
|  |                           |  |    memory (no default)       |  |
|  +---------------------------+  +------------------------------+  |
|                                                                   |
|  Off-heap / memoryOverhead (shared by JVM + Python)               |
+------------------------------------------------------------------+
```

### Key PySpark Memory Configs

| Config | Default | Description |
|--------|---------|-------------|
| `spark.executor.pyspark.memory` | Not set | Hard memory limit for Python workers per executor. When set, Python workers are killed if they exceed this. |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10000` | Max rows per batch for Arrow transfers between JVM and Python. Lower = less memory per batch. |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` (DBR) | Use Arrow for `toPandas()` and `createDataFrame()`. Much faster and more memory-efficient than pickle. |
| `spark.python.worker.memory` | `512m` | Soft limit per Python worker. Triggers spill to disk when exceeded. |
| `spark.python.worker.reuse` | `true` | Reuse Python workers across tasks. Saves startup time but may accumulate memory. |

### Pandas UDF Memory Optimization

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# BAD: Pandas UDF that creates large intermediate objects
@pandas_udf("double")
def expensive_udf(series: pd.Series) -> pd.Series:
    # This creates a full copy of the data in Python memory
    df = pd.DataFrame({"value": series})
    df["normalized"] = (df["value"] - df["value"].mean()) / df["value"].std()
    df["squared"] = df["normalized"] ** 2
    df["result"] = df["squared"].cumsum()
    return df["result"]

# GOOD: Minimize intermediate allocations
@pandas_udf("double")
def efficient_udf(series: pd.Series) -> pd.Series:
    normalized = (series - series.mean()) / series.std()
    return (normalized ** 2).cumsum()

# Control batch size to limit per-batch memory
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
```

### PySpark Memory Pitfalls

```python
# PITFALL 1: Closures capturing large objects
large_dict = load_big_dict()  # 500MB dict

@F.udf("string")
def lookup(key):
    return large_dict.get(key)  # large_dict is serialized and sent to every task!

# FIX: Use broadcast variable
large_dict_bc = spark.sparkContext.broadcast(large_dict)

@F.udf("string")
def lookup(key):
    return large_dict_bc.value.get(key)

# PITFALL 2: Collecting into Python objects
pdf = huge_spark_df.toPandas()  # Copies entire DF into Python memory via Arrow

# FIX: Process in chunks
for batch_df in huge_spark_df.toLocalIterator(prefetchBuffers=True):
    process_batch(batch_df)

# PITFALL 3: Using mapInPandas/applyInPandas with unbounded state
def process_group(key, pdf: pd.DataFrame) -> pd.DataFrame:
    # If one group is 10GB, this will OOM the Python worker
    return pdf.assign(rank=pdf["value"].rank())

# FIX: Ensure groups are bounded, or handle large groups differently
# Check group sizes first:
df.groupBy("group_key").count().orderBy(F.desc("count")).show(10)
```

### Arrow Serialization

Arrow is the columnar format used for transferring data between JVM and Python. It is significantly faster and more memory-efficient than pickle serialization:

```python
# Ensure Arrow is enabled (default on Databricks)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Arrow benefits:
# - Zero-copy reads for numeric types
# - Columnar format matches Pandas internal layout
# - 10-100x faster than pickle for toPandas()/createDataFrame()
# - Reduces peak memory during transfer

# Arrow limitation:
# - Complex nested types may fall back to pickle
# - Very wide DataFrames (1000+ columns) may use more memory with Arrow
```

---

## 8. Memory Profiling

### Enabling Process Tree Metrics

Spark can track the actual OS-level memory usage of executor processes (including Python workers):

```python
# Enable process tree metrics (Spark 3.0+)
spark.conf.set("spark.executor.processTreeMetrics.enabled", "true")
```

When enabled, Spark collects:

| Metric | Description |
|--------|-------------|
| **ProcessTreeJVMVMemory** | JVM virtual memory usage |
| **ProcessTreeJVMRSSMemory** | JVM resident set size (actual physical memory) |
| **ProcessTreePythonVMemory** | Python worker virtual memory |
| **ProcessTreePythonRSSMemory** | Python worker resident set size |
| **ProcessTreeOtherVMemory** | Other child process virtual memory |
| **ProcessTreeOtherRSSMemory** | Other child process resident set size |

### Reading RSS/VMS Metrics

```
Spark UI → Executors → Click executor ID → Thread Dump / Metrics

Key metrics to watch:
- JVM RSS > spark.executor.memory: JVM is using more than allocated
  (usually off-heap/direct buffers)
- Python RSS growing over time: Python memory leak
- Total RSS approaching container limit: Risk of OOM kill
```

Calculating actual memory utilization:

```
Container Memory Limit = spark.executor.memory + spark.executor.memoryOverhead

Actual Usage = JVM RSS + Python RSS + Other RSS

Headroom = Container Limit - Actual Usage

If Headroom < 0 → Container will be killed (exit code 137)
If Headroom < 10% → At risk of OOM under load spikes
```

### Identifying Memory Leaks in Long-Running Jobs

Memory leaks typically manifest in streaming jobs or long-running batch pipelines. Signs:

```
Time →
RSS Memory:
  Start:    ████                          2 GB
  1 hour:   ██████                        3 GB
  2 hours:  ████████                      4 GB
  4 hours:  ████████████                  6 GB    <-- linear growth = leak
  8 hours:  ██████████████████████████    13 GB   <-- OOM imminent
```

**Common leak causes and fixes:**

| Cause | Detection | Fix |
|-------|-----------|-----|
| Accumulating broadcast variables | Driver memory grows, `sc.broadcast` calls without `unpersist()` | Call `broadcast_var.unpersist()` after use |
| Caches never unpersisted | Storage tab shows growing cached data | Add `df.unpersist()` after cache is no longer needed |
| Python closures holding references | Python RSS grows; objects not garbage collected | Avoid closures over large objects; use broadcast |
| Thread-local state in UDFs | JVM heap grows per-executor over time | Avoid static mutable state; use accumulators |
| Listener / callback accumulation | Driver heap grows; many SparkListener objects | Deregister listeners; check custom listeners |

**Profiling with heap dumps (advanced):**

```python
# Trigger a heap dump on OOM (add to extraJavaOptions)
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+HeapDumpOnOutOfMemoryError "
    "-XX:HeapDumpPath=/tmp/executor_heap_dump.hprof"
)

# Analyze with tools: Eclipse MAT, VisualVM, jmap
# On Databricks, heap dumps are written to the executor's local storage
# and can be retrieved from the cluster's DBFS or ephemeral storage
```

> **Photon:** Photon-enabled clusters have different memory profiling characteristics. Since Photon uses native C++ memory, JVM heap dumps will not show Photon's memory usage. Instead, monitor the overall container RSS via process tree metrics. Photon provides its own memory usage metrics visible in the Spark UI's SQL tab under the Photon operator nodes. Photon also has built-in memory accounting that prevents runaway allocation, making OOM from Photon operations less common than from JVM operations.

---

## Quick Reference: Memory Tuning Cheat Sheet

```
+------------------------------------------------------------------+
|                  MEMORY ISSUE DECISION TREE                       |
+------------------------------------------------------------------+

Symptom: Executor OOM
├── Exit code 137 / "Container killed"
│   └── Increase spark.executor.memoryOverhead
├── "Java heap space"
│   ├── GC time high? → Tune GC, reduce data per task
│   ├── Skewed tasks? → Salt keys, AQE, repartition
│   └── Large aggregation? → Two-phase agg, increase memory
├── "GC overhead limit exceeded"
│   └── Too many objects in heap → Increase memory or reduce data
└── Python worker crash
    └── Increase spark.executor.pyspark.memory,
        reduce arrow.maxRecordsPerBatch

Symptom: Driver OOM
├── After collect() / toPandas()
│   └── Use take(), limit(), write to storage
├── During broadcast
│   └── Disable auto-broadcast or increase driver memory
└── Growing over time
    └── Unpersist broadcasts, check listener leaks

Symptom: High Spill
├── All tasks spilling equally
│   └── Increase spark.executor.memory or reduce partition size
├── Only some tasks spilling (skew)
│   └── Repartition, salt keys, enable AQE skew handling
└── Spill on shuffle
    └── Increase partitions (spark.sql.shuffle.partitions),
        increase memory, or optimize the query plan

Symptom: High GC
├── > 10% of task time
│   ├── Large heap? → Use G1GC, tune IHOP
│   ├── Many cached DFs? → Reduce caching, use SER storage
│   └── Many small objects? → Use DataFrame API (not RDD),
│       avoid UDFs that create objects
└── > 30% of task time (thrashing)
    └── URGENT: Increase memory or drastically reduce data per task
```

### Essential Memory Configurations

| Config | Default | Recommendation |
|--------|---------|---------------|
| `spark.executor.memory` | `1g` | Start with 4-8g. Avoid >32g (compressed oops boundary). |
| `spark.executor.memoryOverhead` | `10%` of executor memory | Set 15-25% for PySpark or shuffle-heavy jobs. |
| `spark.driver.memory` | `1g` | 2-4g for most jobs. Increase if broadcasting or collecting. |
| `spark.memory.fraction` | `0.6` | Increase to 0.7-0.8 if no/minimal UDF state. Decrease if UDFs need more user memory. |
| `spark.memory.storageFraction` | `0.5` | Lower (0.3) if you don't cache much. Higher (0.7) if caching is critical. |
| `spark.executor.pyspark.memory` | Not set | Set to 1-2g for Pandas UDF workloads. |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10000` | Reduce to 2000-5000 if Python workers OOM during Pandas UDFs. |
| `spark.executor.processTreeMetrics.enabled` | `false` | Set `true` when debugging memory issues. Minor overhead. |

> **Takeaway:** Most memory problems in Spark come down to three root causes: (1) data skew concentrating too much data in one task, (2) insufficient memory for the workload, or (3) pulling too much data to the driver. Start by identifying which one you're dealing with using Spark UI metrics, then apply the targeted fix from this guide.
