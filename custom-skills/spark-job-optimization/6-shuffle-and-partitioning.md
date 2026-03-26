# 6. Shuffle and Partitioning

Shuffles are the most expensive operations in Spark. They involve serializing data, writing it to disk, transferring it across the network, and deserializing it on the receiving side. Understanding when shuffles happen, how to size partitions, and how to mitigate skew is essential for performant Spark jobs.

---

## Table of Contents

1. [What Causes Shuffles](#1-what-causes-shuffles)
2. [Partition Sizing](#2-partition-sizing)
3. [coalesce vs repartition](#3-coalesce-vs-repartition)
4. [Adaptive Query Execution (AQE) for Partitioning](#4-adaptive-query-execution-aqe-for-partitioning)
5. [Data Skew Detection and Mitigation](#5-data-skew-detection-and-mitigation)
6. [Partitioning Strategies for Different Workloads](#6-partitioning-strategies-for-different-workloads)
7. [Shuffle Optimization Configs](#7-shuffle-optimization-configs)
8. [Eliminating Unnecessary Shuffles](#8-eliminating-unnecessary-shuffles)

---

## 1. What Causes Shuffles

A **shuffle** (also called an **exchange**) occurs whenever Spark must redistribute data across partitions. This creates a **stage boundary** — the upstream stage writes shuffle files and the downstream stage reads them.

### Operations That Trigger Shuffles

| Operation | Why It Shuffles | Example |
|-----------|----------------|---------|
| `groupBy` / `groupByKey` | All rows with the same key must land on the same partition | `df.groupBy("dept").agg(sum("salary"))` |
| `join` (sort-merge or shuffle-hash) | Both sides must be co-partitioned on the join key | `df1.join(df2, "id")` |
| `repartition` | Explicitly redistributes data | `df.repartition(200, "country")` |
| `distinct` | Requires grouping identical rows | `df.distinct()` |
| `sort` / `orderBy` (global) | Global ordering requires range partitioning | `df.orderBy("timestamp")` |
| Window functions with `partitionBy` | Rows in the same window partition must be co-located | `F.row_number().over(Window.partitionBy("user"))` |
| `union` + `distinct` | Union itself has no shuffle, but the dedup does | `df1.union(df2).distinct()` |
| `intersect` / `except` | Set operations require comparing rows across partitions | `df1.intersect(df2)` |
| `cube` / `rollup` | Multi-level aggregation | `df.cube("region", "product").agg(...)` |
| `cogroup` (RDD API) | Groups two RDDs by key | `rdd1.cogroup(rdd2)` |

### Operations That Do NOT Trigger Shuffles

| Operation | Why No Shuffle |
|-----------|---------------|
| `filter` / `where` | Row-level, no redistribution needed |
| `select` / `withColumn` | Column projection/transformation |
| `map` / `flatMap` | Per-row transformation |
| `union` (without dedup) | Just concatenates partitions |
| `coalesce` (reducing partitions) | Narrow dependency — merges adjacent partitions |
| `broadcast join` | Small table sent to all executors, no shuffle of the large side |

### Identifying Shuffles in Query Plans

When you run `df.explain("formatted")`, look for **Exchange** nodes:

```
== Physical Plan ==
*(2) HashAggregate(keys=[dept#12], functions=[sum(salary#14)])
+- Exchange hashpartitioning(dept#12, 200), ENSURE_REQUIREMENTS, [plan_id=45]
   +- *(1) HashAggregate(keys=[dept#12], functions=[partial_sum(salary#14)])
      +- *(1) FileScan parquet [dept#12,salary#14]
```

Key exchange types:
- **`hashpartitioning`** — hash-based redistribution (most common: joins, groupBy)
- **`rangepartitioning`** — range-based redistribution (orderBy, range repartition)
- **`RoundRobinPartitioning`** — even distribution (repartition without columns)
- **`SinglePartition`** — all data to one partition (global aggregate)

### Identifying Shuffles in Spark UI

```
Spark UI Clues:
┌──────────────────────────────────────────────────┐
│  Jobs Tab                                        │
│  └─ Each shuffle creates a NEW STAGE             │
│     Stage 0: FileScan ──► partial_agg            │
│     Stage 1: Exchange ──► final_agg ──► output   │
│                                                  │
│  Stages Tab                                      │
│  └─ "Shuffle Read" and "Shuffle Write" columns   │
│     show bytes moved across the network          │
│                                                  │
│  SQL Tab                                         │
│  └─ DAG shows Exchange nodes with metrics:       │
│     - data size                                  │
│     - shuffle records written/read               │
└──────────────────────────────────────────────────┘
```

> **Takeaway:** Every Exchange node in your query plan is a potential performance bottleneck. Count your exchanges — fewer is almost always better.

---

## 2. Partition Sizing

### The 128 MB Rule of Thumb

Spark works best when each partition is approximately **128 MB** of compressed, in-memory data. This balances parallelism against per-task overhead.

```
                    Partition Size Spectrum
    ◄────────────────────────────────────────────────►
    1 MB          32 MB        128 MB       512 MB    2 GB+
    │              │             │            │         │
    Too small      Acceptable    Optimal      Large     Too large
    ▼              ▼             ▼            ▼         ▼
    Scheduling     OK for fast   Best         Memory    OOM risk,
    overhead       transforms    balance      pressure  GC pauses,
    dominates                                           spill to disk
```

### Calculating Optimal Partition Count

**Formula:**

```
optimalPartitionCount = totalDataSize / targetPartitionSize
```

```python
# Method 1: From known data size
data_size_gb = 100  # 100 GB dataset
target_mb = 128
optimal_partitions = int((data_size_gb * 1024) / target_mb)
# => 800 partitions

# Method 2: Measure from a DataFrame (approximate)
from pyspark.sql import functions as F

# Check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Estimate data size using Catalyst stats (after an action)
df.cache().count()  # materialize
plan = df._jdf.queryExecution().optimizedPlan()
size_bytes = plan.stats().sizeInBytes()
optimal = int(size_bytes / (128 * 1024 * 1024))
print(f"Estimated optimal partitions: {max(optimal, 1)}")
df.unpersist()

# Method 3: Quick estimate from row count and avg row size
row_count = df.count()
avg_row_bytes = 200  # estimate or sample
total_bytes = row_count * avg_row_bytes
optimal = int(total_bytes / (128 * 1024 * 1024))
```

### Problems with Wrong Partition Counts

| Symptom | Cause | Effect |
|---------|-------|--------|
| Tasks finish in < 100ms | Too many partitions | Scheduling overhead dominates; driver bottleneck |
| Some tasks take 10x longer | Too few partitions or skew | Stragglers; poor cluster utilization |
| Executor OOM | Too few partitions | Each task processes too much data |
| Excessive shuffle spill | Partitions too large for memory | Disk I/O kills performance |
| Thousands of tiny output files | Too many partitions at write | Downstream read performance degrades |

> **Takeaway:** After every shuffle-heavy operation, verify your partition count. Use `df.rdd.getNumPartitions()` and cross-reference with the data size. Target 128 MB per partition as a starting point, adjusting for memory-intensive operations.

---

## 3. coalesce vs repartition

These are the two primary tools for changing partition count. They have fundamentally different mechanics.

### Comparison Table

| Aspect | `coalesce(n)` | `repartition(n)` / `repartition(n, col)` |
|--------|--------------|------------------------------------------|
| Direction | Only **reduces** partitions | Can **increase or decrease** |
| Shuffle | **No shuffle** (narrow dependency) | **Full shuffle** (wide dependency) |
| Mechanism | Merges adjacent partitions | Hash-distributes all data |
| Data distribution | Uneven (inherits upstream skew) | Even distribution |
| Partition column | Not supported | Supported: `repartition(n, "col")` |
| Query plan node | `Coalesce` | `Exchange` |
| Performance | Faster (no network I/O) | Slower (full data redistribution) |

### Visual: How They Work

```
coalesce(2) — merges adjacent partitions, no shuffle:

  Before (4 partitions):     After (2 partitions):
  ┌────┐ ┌────┐              ┌─────────┐
  │ P0 │ │ P1 │  ──merge──►  │ P0 + P1 │
  └────┘ └────┘              └─────────┘
  ┌────┐ ┌────┐              ┌─────────┐
  │ P2 │ │ P3 │  ──merge──►  │ P2 + P3 │
  └────┘ └────┘              └─────────┘

  Executor 1 keeps P0+P1, Executor 2 keeps P2+P3
  No data crosses the network.


repartition(2) — full shuffle via hash:

  Before (4 partitions):     Shuffle          After (2 partitions):
  ┌────┐                    ┌──────┐          ┌────┐
  │ P0 │──── hash(row) ───►│      │────────► │ P0'│  (rows with hash%2==0)
  └────┘                   │      │          └────┘
  ┌────┐                   │ All  │
  │ P1 │──── hash(row) ───►│ data │
  └────┘                   │  is  │          ┌────┐
  ┌────┐                   │redis-│────────► │ P1'│  (rows with hash%2==1)
  │ P2 │──── hash(row) ───►│tribu-│          └────┘
  └────┘                   │ ted  │
  ┌────┐                   │      │
  │ P3 │──── hash(row) ───►│      │
  └────┘                    └──────┘

  Every row potentially moves across the network.
```

### When to Use Each

```python
# USE coalesce WHEN:
# - Reducing partitions after a filter that shrinks data significantly
df_filtered = df.filter(F.col("status") == "active")  # 10% of data remains
df_filtered.coalesce(20).write.parquet("/output")

# - Reducing output file count for writes
df.coalesce(10).write.mode("overwrite").parquet("/output")

# USE repartition WHEN:
# - Increasing partition count (coalesce cannot do this)
df.repartition(500)

# - You need even data distribution
df.repartition(200, "customer_id")

# - Preparing for a join (co-partitioning)
df1 = df1.repartition(200, "join_key")
df2 = df2.repartition(200, "join_key")
result = df1.join(df2, "join_key")

# - Writing partitioned output with controlled file count per partition
df.repartition("year", "month").write.partitionBy("year", "month").parquet("/out")
```

### Common Mistake: Using repartition When coalesce Would Work

```python
# BAD: Unnecessary shuffle just to reduce file count
df.repartition(10).write.parquet("/output")

# GOOD: No shuffle needed
df.coalesce(10).write.parquet("/output")

# EXCEPTION: If upstream partitions are heavily skewed, coalesce
# preserves that skew. In that case repartition may be justified.
```

> **Takeaway:** Default to `coalesce` when reducing partition counts. Only use `repartition` when you need to increase partitions, need even distribution, or need to partition by specific columns.

> **Photon:** Photon's native shuffle implementation makes `repartition` less expensive than on JVM Spark, but it is still a full shuffle. Prefer `coalesce` for simple partition reduction even on Photon clusters.

---

## 4. Adaptive Query Execution (AQE) for Partitioning

AQE (enabled by default on Databricks) dynamically adjusts query plans at stage boundaries based on runtime statistics. For partitioning, it provides three critical optimizations.

### 4.1 Post-Shuffle Partition Coalescing

After a shuffle, AQE examines actual partition sizes and merges small partitions together.

```
Without AQE (spark.sql.shuffle.partitions = 200):

  200 shuffle partitions, many nearly empty:
  ┌──┐┌──┐┌──┐┌──┐┌──┐┌──┐     ┌──┐┌──┐
  │2M││1M││3M││0 ││0 ││5M│ ... │1M││0 │
  └──┘└──┘└──┘└──┘└──┘└──┘     └──┘└──┘
  ▲ Many tiny or empty partitions = wasted tasks

With AQE coalescing:

  Merged into ~10 right-sized partitions:
  ┌──────────┐┌──────────┐     ┌──────────┐
  │  128 MB  ││  120 MB  │ ... │   95 MB  │
  └──────────┘└──────────┘     └──────────┘
  ▲ Fewer tasks, each with meaningful work
```

**Key config:**

```python
# Target size for coalesced partitions (default: 64MB on Databricks)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# Minimum partition size (prevents over-coalescing)
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
```

### 4.2 Skewed Partition Splitting

AQE detects partitions that are significantly larger than the median and automatically splits them.

```
Before AQE skew handling:
  ┌──┐ ┌──┐ ┌──┐ ┌──────────────────┐ ┌──┐
  │50││50││50││     2000 MB         ││50│   ← skewed partition
  └──┘ └──┘ └──┘ └──────────────────┘ └──┘

After AQE splits the skewed partition:
  ┌──┐ ┌──┐ ┌──┐ ┌───┐┌───┐┌───┐┌───┐ ┌──┐
  │50││50││50││500││500││500││500││50│   ← split into 4 sub-partitions
  └──┘ └──┘ └──┘ └───┘└───┘└───┘└───┘ └──┘
```

AQE identifies skew when a partition is larger than both:
- `skewedPartitionFactor` (default: 5) times the **median** partition size
- `skewedPartitionThresholdInBytes` (default: 256 MB)

```python
# Enable skew join handling (default: true on Databricks)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

### 4.3 The "Set It High and Let AQE Optimize" Pattern

With AQE enabled, you can set `spark.sql.shuffle.partitions` to a **high value** and let AQE coalesce down:

```python
# Classic approach (fragile — must manually tune):
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Too few? Too many?

# AQE approach (robust — auto-adapts):
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # Set high
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
# AQE will coalesce 2000 partitions down to whatever count produces ~128MB each
```

**Why this works:** AQE can *merge* small partitions but cannot *split* non-skewed large partitions. Starting high gives AQE room to consolidate. Starting too low leaves AQE with oversized partitions it cannot fix.

### Identifying AQE in Query Plans

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(2) HashAggregate(keys=[dept#12], functions=[sum(salary#14)])
   +- CustomShuffleReader coalesced                        ← AQE coalesced partitions
      +- ShuffleQueryStage 0
         +- Exchange hashpartitioning(dept#12, 2000)       ← original 2000 partitions
            +- *(1) HashAggregate(keys=[dept#12], ...)
```

Look for:
- `AdaptiveSparkPlan isFinalPlan=true` — AQE is active and has finalized
- `CustomShuffleReader coalesced` — AQE merged partitions
- `CustomShuffleReader skewed` — AQE handled skew

> **Takeaway:** With AQE enabled, set `spark.sql.shuffle.partitions` to a deliberately high value (e.g., 2x your cluster's total cores) and let AQE right-size partitions automatically. Focus on tuning `advisoryPartitionSizeInBytes` rather than manually counting partitions.

---

## 5. Data Skew Detection and Mitigation

Data skew occurs when one or a few partition keys contain disproportionately more data than others. A single skewed partition becomes the bottleneck for the entire stage.

### 5.1 How to Detect Skew

#### Method 1: Spark UI — Task Duration Variance

In the **Stages** tab, click into a stage and examine the **Summary Metrics** table:

```
Task Duration Summary:
┌──────────┬────────┬────────┬────────┬────────┬────────┐
│ Metric   │  Min   │  25th  │ Median │  75th  │  Max   │
├──────────┼────────┼────────┼────────┼────────┼────────┤
│ Duration │  0.5s  │  1.2s  │  1.5s  │  1.8s  │ 45 min │  ← SKEW! Max >> Median
│ Input    │  50MB  │ 120MB  │ 130MB  │ 140MB  │  12GB  │  ← One partition is 100x larger
│ Records  │  50K   │  110K  │  120K  │  130K  │  15M   │
└──────────┘────────┘────────┘────────┘────────┘────────┘
```

**Red flag:** When Max is more than 5-10x the Median for duration, input size, or record count.

#### Method 2: Code — Distribution Analysis

```python
from pyspark.sql import functions as F

# Check key distribution
key_dist = df.groupBy("join_key").count()

# Summary statistics
key_dist.describe("count").show()
# +-------+------------------+
# |summary|             count|
# +-------+------------------+
# |  count|           1000000|  ← 1M distinct keys
# |   mean|             100.0|  ← average 100 rows per key
# | stddev|           15234.5|  ← HIGH stddev = skew
# |    min|                 1|
# |    max|          12000000|  ← one key has 12M rows!
# +-------+------------------+

# Find the hot keys
key_dist.orderBy(F.desc("count")).show(20)

# Quantile analysis
key_dist.approxQuantile("count", [0.5, 0.9, 0.95, 0.99, 1.0], 0.01)
# [100.0, 200.0, 500.0, 5000.0, 12000000.0]
# ▲ The 99th percentile has 5K rows but the max has 12M — extreme skew
```

#### Method 3: Query Plan — AQE Skew Indicators

```
-- In explain output, look for:
CustomShuffleReader skewed
-- This confirms AQE detected and is handling skew.

-- Also check the SQL tab in Spark UI for:
-- "number of skewed partitions" metric on Exchange nodes
```

### 5.2 Skew Mitigation Strategies

#### Strategy 1: AQE Automatic Skew Handling

The simplest approach — let AQE handle it. Ensure these configs are set:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# For heavy skew, lower the threshold:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64m")
```

**Limitation:** AQE skew handling works for **sort-merge joins**. It does not help with skewed aggregations or other non-join shuffles.

#### Strategy 2: Salting (Full Code Pattern)

Salting artificially distributes a hot key across multiple partitions by appending a random suffix.

```python
from pyspark.sql import functions as F
import random

SALT_BUCKETS = 10  # Split hot keys across 10 sub-partitions

# === For Skewed Joins ===

# Step 1: Salt the skewed (large) side
df_large_salted = df_large.withColumn(
    "salt", (F.rand() * SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Step 2: Explode the small side to match all salt values
from pyspark.sql.types import ArrayType, IntegerType

salt_values = list(range(SALT_BUCKETS))
df_small_exploded = df_small.withColumn(
    "salt", F.explode(F.array([F.lit(s) for s in salt_values]))
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Step 3: Join on the salted key
result = df_large_salted.join(
    df_small_exploded,
    on="salted_key",
    how="inner"
).drop("salt", "salted_key")
```

```
Salting Visualization:

Before salting (all "US" rows in one partition):
  Partition 0: [US, US, US, US, US, US, ...US]    ← 12M rows, SKEWED
  Partition 1: [CA, CA, CA]                         ← 300 rows
  Partition 2: [MX, MX]                             ← 200 rows

After salting with SALT_BUCKETS=3:
  Partition 0: [US_0, US_0, US_0, US_0]            ← ~4M rows
  Partition 1: [US_1, US_1, US_1, US_1]            ← ~4M rows
  Partition 2: [US_2, US_2, US_2, US_2]            ← ~4M rows
  Partition 3: [CA_0, CA_1, CA_2, MX_0, MX_1, ...]← small keys unaffected
```

#### Strategy 3: Two-Phase Aggregation

For skewed aggregations (not joins), aggregate in two phases:

```python
# Problem: groupBy("country").agg(sum("revenue"))
# where "US" has 100x more rows than other countries

# Phase 1: Partial aggregation with salted keys
df_partial = (
    df
    .withColumn("salt", (F.rand() * 10).cast("int"))
    .groupBy("country", "salt")
    .agg(
        F.sum("revenue").alias("partial_revenue"),
        F.count("*").alias("partial_count")
    )
)

# Phase 2: Final aggregation (much smaller dataset now)
df_final = (
    df_partial
    .groupBy("country")
    .agg(
        F.sum("partial_revenue").alias("total_revenue"),
        F.sum("partial_count").alias("total_count")
    )
)
```

**Why it works:** Phase 1 distributes the hot key across 10 partitions, performing a partial reduction. Phase 2 aggregates only the pre-reduced results (10 rows per key maximum).

#### Strategy 4: Partial Broadcast for Hot Keys

When only a few keys are skewed, separate them and use broadcast:

```python
# Identify hot keys
hot_keys = (
    df_large.groupBy("join_key")
    .count()
    .filter(F.col("count") > 1000000)
    .select("join_key")
    .collect()
)
hot_key_list = [row["join_key"] for row in hot_keys]

# Split large DataFrame
df_large_hot = df_large.filter(F.col("join_key").isin(hot_key_list))
df_large_normal = df_large.filter(~F.col("join_key").isin(hot_key_list))

# Split small DataFrame
df_small_hot = df_small.filter(F.col("join_key").isin(hot_key_list))
df_small_normal = df_small.filter(~F.col("join_key").isin(hot_key_list))

# Broadcast join for hot keys (no shuffle on the large side)
result_hot = df_large_hot.join(F.broadcast(df_small_hot), "join_key")

# Regular join for normal keys (no skew)
result_normal = df_large_normal.join(df_small_normal, "join_key")

# Combine
result = result_hot.union(result_normal)
```

#### Strategy 5: Repartition with Explicit Expressions

When data is skewed because of the default hash function, use a custom expression:

```python
# Instead of repartition by a skewed column directly...
df.repartition(200, "skewed_column")

# ... add entropy to the partition expression
df.repartition(200, F.concat(F.col("skewed_column"), F.col("secondary_column")))

# Or use a hash with more columns to distribute better
df.repartition(200, F.hash(F.col("skewed_column"), F.col("id")) % 200)
```

### Skew Strategy Decision Tree

```
Is the skew in a JOIN or an AGGREGATION?
│
├── JOIN
│   ├── Is AQE enabled and is it a sort-merge join?
│   │   ├── YES → Let AQE handle it. Check if it's sufficient.
│   │   └── NO  → Enable AQE or use manual strategies below.
│   │
│   ├── Is the small side broadcastable (< 100MB)?
│   │   └── YES → Use broadcast join. Problem solved.
│   │
│   ├── Are only a few keys skewed?
│   │   └── YES → Use partial broadcast (Strategy 4)
│   │
│   └── Many keys skewed → Use salting (Strategy 2)
│
└── AGGREGATION
    ├── Is AQE helping? (Check plan for coalesced partitions)
    │   └── YES → May be sufficient; monitor task durations.
    │
    └── Still skewed → Use two-phase aggregation (Strategy 3)
```

> **Takeaway:** Always check AQE first — it handles many skew scenarios automatically. For extreme skew or aggregation skew, use salting or two-phase aggregation. The right strategy depends on whether the skew appears in a join or an aggregation, and how many keys are affected.

---

## 6. Partitioning Strategies for Different Workloads

### Hash Partitioning

The default strategy for most shuffles. Rows are assigned to partitions via `hash(key) % numPartitions`.

```python
# Explicit hash partitioning
df.repartition(200, "customer_id")

# Spark also uses hash partitioning internally for:
# - groupBy
# - join (sort-merge)
# - distinct
```

**Pros:** Even distribution when keys have reasonable cardinality.
**Cons:** Skew when a few keys dominate; different key types may hash poorly.

### Range Partitioning

Spark samples the data to determine partition boundaries, then assigns rows to ranges.

```python
# Explicit range partitioning
df.repartitionByRange(200, "timestamp")

# Spark uses range partitioning internally for:
# - orderBy / sort (global ordering)
# - range-based window functions
```

**Pros:** Sorted output within partitions; excellent for range queries and time-series data.
**Cons:** Requires a sampling phase; can be skewed if data clusters in certain ranges.

### Round-Robin Partitioning

Distributes rows evenly without regard to content. Used by `repartition(n)` without column arguments.

```python
# Round-robin: guaranteed even distribution
df.repartition(200)  # No column specified → round-robin
```

**Pros:** Perfectly even partition sizes.
**Cons:** No data locality; cannot benefit co-partitioned operations.

### Custom Partitioning (RDD API)

For advanced use cases, the RDD API supports custom partitioners:

```python
from pyspark import Partitioner

class GeoPartitioner(Partitioner):
    def __init__(self, num_partitions):
        self._num_partitions = num_partitions

    def numPartitions(self):
        return self._num_partitions

    def partitionOf(self, key):
        # Custom logic: partition by geographic region
        region_map = {"US": 0, "EU": 1, "APAC": 2}
        return region_map.get(key, self._num_partitions - 1)

# Apply custom partitioner (RDD-level only)
rdd = df.rdd.map(lambda row: (row["region"], row))
partitioned_rdd = rdd.partitionBy(3, GeoPartitioner(3))
```

### Co-Partitioned Joins

When two DataFrames are partitioned on the same key with the same number of partitions, Spark can skip the shuffle for the join:

```python
# Pre-partition both sides on the join key with same partition count
num_parts = 200
df_orders = df_orders.repartition(num_parts, "customer_id")
df_customers = df_customers.repartition(num_parts, "customer_id")

# This join may avoid one or both shuffles
result = df_orders.join(df_customers, "customer_id")

# Verify in the plan — look for absence of Exchange nodes
result.explain()
```

**Important:** Co-partitioning only eliminates shuffles when:
1. Same number of partitions
2. Same partitioning columns
3. Same partitioning scheme (hash)
4. Operations are in the same query stage (no materialization between them)

### Strategy Comparison Table

| Strategy | Distribution | Shuffle | Best For |
|----------|-------------|---------|----------|
| Hash | By key hash | Yes | Joins, groupBy, dedup |
| Range | By value ranges | Yes | Sorting, time-series, range queries |
| Round-robin | Even, random | Yes | Write balancing, no downstream key dependency |
| Coalesce | Uneven (merge) | No | Reducing partition count |
| Custom | User-defined | Yes (RDD) | Domain-specific locality requirements |

> **Takeaway:** Use `repartition("column")` before joins to co-partition your data and potentially eliminate shuffle. Use range partitioning for sorted/time-series workloads. Use round-robin when you just need even distribution without key semantics.

---

## 7. Shuffle Optimization Configs

### Core Shuffle Settings

| Config | Default | Recommended | Purpose |
|--------|---------|-------------|---------|
| `spark.sql.shuffle.partitions` | 200 | `"auto"` or 2x cores | Number of partitions after each shuffle |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64 MB | 128 MB | Target partition size for AQE coalescing |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1 MB | 1-4 MB | Minimum partition size (prevents tiny partitions) |
| `spark.shuffle.compress` | true | true | Compress shuffle output (LZ4 by default) |
| `spark.shuffle.spill.compress` | true | true | Compress data spilled to disk during shuffle |
| `spark.reducer.maxSizeInFlight` | 48 MB | 48-96 MB | Buffer size for shuffle fetch per reduce task |

### Detailed Config Explanations

#### `spark.sql.shuffle.partitions`

Controls the number of output partitions for all shuffle operations (joins, aggregations, etc.).

```python
# Static setting (pre-AQE approach)
spark.conf.set("spark.sql.shuffle.partitions", "400")

# With AQE, set high and let Spark optimize
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# On Databricks, "auto" delegates entirely to AQE
spark.conf.set("spark.sql.shuffle.partitions", "auto")
```

**Pitfall:** This is a *global* setting — it applies to every shuffle in the job. Without AQE, a single value rarely fits all stages.

#### `spark.sql.adaptive.advisoryPartitionSizeInBytes`

Tells AQE the target size for each coalesced partition. This is the single most impactful AQE tuning knob.

```python
# Default on Databricks: 64MB
# For large-scale ETL with beefy executors:
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")

# For memory-constrained or complex transformations:
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
```

#### `spark.sql.adaptive.coalescePartitions.minPartitionSize`

Prevents AQE from creating extremely small partitions. Useful when data is sparse.

```python
# Default: 1MB
# If you see tasks completing in milliseconds, raise this:
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "4m")
```

#### `spark.shuffle.compress`

Compresses shuffle data written to disk. Uses LZ4 by default (fast, moderate compression). Almost always leave enabled.

```python
# Default: true — leave it on
spark.conf.set("spark.shuffle.compress", "true")

# Change compression codec if needed (rare):
spark.conf.set("spark.io.compression.codec", "lz4")  # or "zstd", "snappy"
```

#### `spark.shuffle.spill.compress`

When shuffle data exceeds memory and spills to disk, this controls whether spill files are compressed. Reduces disk I/O at the cost of CPU.

```python
# Default: true — leave it on
spark.conf.set("spark.shuffle.spill.compress", "true")
```

#### `spark.reducer.maxSizeInFlight`

Controls how much shuffle data a reduce task fetches simultaneously from map tasks. Larger values improve throughput at the cost of memory.

```python
# Default: 48MB
# For network-heavy shuffles on fast networks:
spark.conf.set("spark.reducer.maxSizeInFlight", "96m")

# If executors are OOMing during shuffle fetch, reduce:
spark.conf.set("spark.reducer.maxSizeInFlight", "24m")
```

### Recommended Config Template

```python
# === Balanced shuffle configuration for Databricks ===

# Let AQE manage partition counts
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Target 128MB partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "4m")

# Enable skew handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# Shuffle I/O
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")
```

> **Photon:** Photon uses a native C++ shuffle implementation that is significantly faster than the JVM-based shuffle. On Photon-enabled clusters, shuffle overhead may drop by 2-3x, making some manual optimizations (like aggressive coalescing to avoid shuffles) less critical. However, reducing unnecessary shuffles is still beneficial — the fastest shuffle is the one that never happens.

---

## 8. Eliminating Unnecessary Shuffles

The best shuffle optimization is avoiding the shuffle entirely. These patterns help you eliminate or reduce shuffles.

### 8.1 Bucketing

Bucketing pre-partitions data at write time, so reads and joins can skip shuffles entirely.

```python
# Write bucketed table (once)
(
    df
    .write
    .bucketBy(256, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("orders_bucketed")
)

# Now joins on customer_id skip the shuffle!
orders = spark.table("orders_bucketed")
customers = spark.table("customers_bucketed")  # also bucketed by customer_id

# Check the plan — should show NO Exchange nodes for the join
orders.join(customers, "customer_id").explain()
```

**Bucketing requirements for shuffle elimination:**
- Both tables bucketed on the join column
- Same number of buckets (or one is a multiple of the other)
- Both tables sorted by the same column
- Reading via `spark.table()` (not `spark.read.parquet()`)

**Bucketing limitations:**
- Only works with managed tables / `saveAsTable`
- Must be maintained as data changes
- Bucket count is fixed at write time

> **Photon:** Photon does not currently leverage bucketing for shuffle elimination in all cases. Verify with `explain()` that your bucketed join actually eliminates the Exchange node on Photon clusters.

### 8.2 Pre-Partitioning

If you perform multiple operations on the same key, repartition once and reuse:

```python
# BAD: Multiple shuffles on the same key
result1 = df.groupBy("user_id").agg(F.sum("amount"))
result2 = df.groupBy("user_id").agg(F.avg("amount"))
result3 = df.join(other_df, "user_id")
# ▲ Three separate shuffles on user_id

# GOOD: Single repartition, reuse for multiple operations
df_partitioned = df.repartition(200, "user_id").cache()
result1 = df_partitioned.groupBy("user_id").agg(F.sum("amount"))
result2 = df_partitioned.groupBy("user_id").agg(F.avg("amount"))
result3 = df_partitioned.join(other_df.repartition(200, "user_id"), "user_id")
# ▲ df_partitioned is already partitioned — subsequent ops may avoid re-shuffling

# BEST: Combine aggregations into one pass
result = df.groupBy("user_id").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("average")
)
# ▲ Single shuffle for all aggregations
```

### 8.3 Co-Locating Data

When repeatedly joining the same datasets, persist them co-partitioned:

```python
# Persist both DataFrames with matching partitioning
N_PARTS = 200

df_orders = spark.table("orders").repartition(N_PARTS, "customer_id").persist()
df_customers = spark.table("customers").repartition(N_PARTS, "customer_id").persist()

# Materialize
df_orders.count()
df_customers.count()

# Subsequent joins use the cached, co-partitioned data
# Spark recognizes matching partitioning and skips the Exchange
enriched = df_orders.join(df_customers, "customer_id")
```

### 8.4 Broadcast Joins

For small-to-medium tables, broadcast eliminates the shuffle on the large side entirely:

```python
from pyspark.sql import functions as F

# Auto-broadcast (tables under threshold are automatically broadcast)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")  # 100MB threshold

# Explicit broadcast hint
result = df_large.join(
    F.broadcast(df_small),
    "join_key"
)

# SQL hint
result = spark.sql("""
    SELECT /*+ BROADCAST(dim) */ *
    FROM fact_table f
    JOIN dim_table dim ON f.dim_id = dim.id
""")
```

```
Broadcast Join — No Shuffle:

  ┌──────────────┐
  │  Small Table │──── broadcast ──►  Sent to ALL executors
  └──────────────┘

  ┌────┐ ┌────┐ ┌────┐ ┌────┐
  │ P0 │ │ P1 │ │ P2 │ │ P3 │       Large table partitions
  │    │ │    │ │    │ │    │       stay where they are.
  │+dim│ │+dim│ │+dim│ │+dim│       Each joins locally with
  └────┘ └────┘ └────┘ └────┘       the broadcast copy.

  No shuffle of the large table. No Exchange node in plan.
```

### 8.5 Map-Side Aggregation

Spark automatically performs partial (map-side) aggregation before a shuffle. Ensure it is effective:

```python
# Spark automatically does partial aggregation:
#   Stage 1: partial_sum per partition (map-side)
#   Shuffle: send partial results
#   Stage 2: final_sum (reduce-side)

# This works well when:
# - Many duplicate keys per partition → high reduction ratio
# - Aggregation functions are decomposable (sum, count, min, max)

# Verify in the plan — look for TWO HashAggregate nodes:
df.groupBy("country").agg(F.sum("revenue")).explain()
# *(2) HashAggregate(keys=[country], functions=[sum(revenue)])    ← final
# +- Exchange hashpartitioning(country, 200)                      ← shuffle
#    +- *(1) HashAggregate(keys=[country], functions=[partial_sum(revenue)])  ← partial

# If you see only ONE HashAggregate, partial aggregation was skipped
# (happens with some complex aggregations or UDAFs)
```

### 8.6 Filter Before Shuffle

Push filters as early as possible to reduce shuffle data volume:

```python
# BAD: Filter after join (shuffles all data, then discards)
result = df_large.join(df_small, "key").filter(F.col("status") == "active")

# GOOD: Filter before join (less data to shuffle)
df_large_filtered = df_large.filter(F.col("status") == "active")
result = df_large_filtered.join(df_small, "key")
```

> **Note:** The Catalyst optimizer often pushes filters down automatically (predicate pushdown), but it cannot always do so — especially across joins or when filters reference columns from both sides.

### Shuffle Elimination Checklist

```
Before optimizing, ask:
┌──────────────────────────────────────────────────────────────────┐
│ 1. Can I use a broadcast join?           (< 100MB small side)   │
│ 2. Can I bucket the tables?              (repeated join pattern) │
│ 3. Can I combine aggregations?           (same groupBy key)     │
│ 4. Can I filter before the shuffle?      (reduce data volume)   │
│ 5. Can I co-partition for reuse?         (multiple joins on key) │
│ 6. Can I use coalesce instead of         (reducing partitions)  │
│    repartition?                                                  │
│ 7. Can AQE handle it automatically?      (enable and verify)    │
└──────────────────────────────────────────────────────────────────┘
```

> **Takeaway:** Treat every Exchange node in your query plan as a cost to be justified. Eliminate shuffles through broadcast joins, bucketing, and filter pushdown. When shuffles are unavoidable, minimize data volume before the shuffle and let AQE optimize partition sizing afterward.

---

## Summary

```
Shuffle Optimization Priority Stack:

  ┌─────────────────────────────────────────┐
  │ 1. ELIMINATE the shuffle                │  ← Broadcast, bucket, filter early
  ├─────────────────────────────────────────┤
  │ 2. REDUCE data before the shuffle       │  ← Filter, project, partial agg
  ├─────────────────────────────────────────┤
  │ 3. RIGHT-SIZE partitions after shuffle  │  ← AQE, advisoryPartitionSize
  ├─────────────────────────────────────────┤
  │ 4. HANDLE SKEW in the shuffle           │  ← AQE skew, salting, two-phase
  ├─────────────────────────────────────────┤
  │ 5. TUNE shuffle I/O                     │  ← Compression, fetch buffers
  └─────────────────────────────────────────┘

  Work top-down. Each level provides diminishing returns
  compared to the one above it.
```

---

*Next: [7-memory-and-spill.md](7-memory-and-spill.md) — Executor memory tuning, garbage collection, spill detection, and OOM diagnosis.*
*Previous: [5-join-optimization.md](5-join-optimization.md) — Join strategies, broadcast vs sort-merge, and skewed join handling.*
