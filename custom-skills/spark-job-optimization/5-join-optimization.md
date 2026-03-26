# 5. Join Optimization

Joins are the most expensive operation in most Spark jobs. A single poorly chosen join strategy can turn a 5-minute job into a 5-hour one. This file covers every join strategy Spark offers, when each is selected, how to influence the optimizer, and how to handle the most common join performance killers -- especially data skew.

---

## Table of Contents

1. [Join Strategies Overview](#1-join-strategies-overview)
2. [Broadcast Hash Join](#2-broadcast-hash-join)
3. [Sort Merge Join](#3-sort-merge-join)
4. [Shuffle Hash Join](#4-shuffle-hash-join)
5. [Handling Data Skew in Joins](#5-handling-data-skew-in-joins)
6. [Join Optimization Patterns](#6-join-optimization-patterns)
7. [Join Type Selection Guide](#7-join-type-selection-guide)
8. [Common Join Anti-Patterns](#8-common-join-anti-patterns)

---

## 1. Join Strategies Overview

Spark has five physical join strategies. The Catalyst optimizer selects one based on table sizes, join type, join condition, and configuration.

### The Five Strategies at a Glance

| Strategy | When Used | Shuffle Required | Best For |
|---|---|---|---|
| **BroadcastHashJoin** | One side fits in memory (< threshold) | No | Small-to-large joins |
| **SortMergeJoin** | Both sides large, equi-join | Yes (both sides) | Large-to-large equi-joins |
| **ShuffledHashJoin** | Both sides large, one smaller, hash preferred | Yes (both sides) | Medium-to-large equi-joins |
| **CartesianProduct** | No join condition (cross join) | Yes | Rarely desired |
| **BroadcastNestedLoopJoin** | Non-equi join + one side broadcastable | Sometimes | Non-equi joins with small table |

### Decision Tree for Join Strategy Selection

```
                        Is there an equi-join condition?
                        /                              \
                      YES                               NO
                      /                                   \
           Is one side small                    Is one side small
           enough to broadcast?                 enough to broadcast?
              /         \                          /          \
            YES          NO                      YES           NO
             |            |                       |             |
    BroadcastHashJoin     |              BroadcastNestedLoop    |
                          |              Join                   |
                   preferSortMergeJoin?                  CartesianProduct
                      /         \                    (AVOID if possible)
                    YES          NO
                     |            |
              SortMergeJoin   ShuffledHashJoin
              (default)       (if buildable)
```

### How to See Which Strategy Was Chosen

```python
# Check the physical plan
df_joined = df_large.join(df_small, "key")
df_joined.explain()

# More detailed output
df_joined.explain("formatted")

# Look for these node names in the output:
# - BroadcastHashJoin
# - SortMergeJoin
# - ShuffledHashJoin
# - CartesianProduct
# - BroadcastNestedLoopJoin
```

```
# Example physical plan showing BroadcastHashJoin
== Physical Plan ==
*(2) Project [key#10, value_large#11, value_small#20]
+- *(2) BroadcastHashJoin [key#10], [key#19], Inner, BuildRight, false
   :- *(2) Filter isnotnull(key#10)
   :  +- *(2) ColumnarToRow
   :     +- FileScan parquet [key#10,value_large#11]
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false]),false), [plan_id=45]
      +- *(1) Filter isnotnull(key#19)
         +- *(1) ColumnarToRow
            +- FileScan parquet [key#19,value_small#20]
```

> **Key takeaway:** Always check `explain()` after writing a join. The strategy Spark picks can surprise you, especially when statistics are stale or unavailable.

---

## 2. Broadcast Hash Join

The fastest join strategy in Spark. One (smaller) side of the join is serialized, sent to the driver, and then broadcast to every executor. Each executor builds an in-memory hash table from the broadcast side and probes it with its local partition of the larger side. No shuffle of the large side is required.

### How It Works

```
Driver collects small table
         |
         v
  Broadcasts to all executors
         |
    +---------+---------+---------+
    | Exec 1  | Exec 2  | Exec 3  |    (each executor has full copy)
    +---------+---------+---------+
    | Build   | Build   | Build   |
    | hash    | hash    | hash    |
    | table   | table   | table   |
    +---------+---------+---------+
    | Probe   | Probe   | Probe   |    (probe with local large-table partitions)
    | with    | with    | with    |
    | large   | large   | large   |
    | partition| partition| partition|
    +---------+---------+---------+
         |         |         |
         v         v         v
       Results   Results   Results
```

### Configuration

```python
# Default threshold: 10 MB (auto-broadcast if estimated size <= this)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

# Increase for larger dimension tables (e.g., 100 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")

# Disable auto-broadcast entirely (force sort-merge for testing/debugging)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Using Broadcast Hints

When Spark's size estimates are wrong (common with complex subqueries, UDFs, or missing statistics), you can force a broadcast with hints.

```python
from pyspark.sql.functions import broadcast

# Python API — function wrapper
df_result = df_large.join(broadcast(df_small), "key")

# Python API — hint method
df_result = df_large.join(df_small.hint("broadcast"), "key")

# These are equivalent; use whichever reads better in your code
```

```sql
-- SQL hint syntax
SELECT /*+ BROADCAST(small) */ *
FROM large
JOIN small ON large.key = small.key;

-- Alternative hint names (all equivalent)
SELECT /*+ BROADCASTJOIN(small) */ * FROM ...
SELECT /*+ MAPJOIN(small) */ * FROM ...
```

### Performance Characteristics

| Aspect | Detail |
|---|---|
| Time complexity | O(n) for the large side (single hash lookup per row) |
| Shuffle | None for the large side; small side collected to driver then broadcast |
| Memory | Each executor must hold the full broadcast table in memory |
| Network | Broadcast bytes = (small table size) x (num executors) |
| Parallelism | Fully parallel across all partitions of the large table |

### When to Use

- Dimension table joins (e.g., joining a 50 MB lookup table to a 500 GB fact table)
- The small side is reliably under ~200 MB after filtering and projection
- You need to eliminate a shuffle on the large side

### Limitations and Pitfalls

```python
# DANGER: Broadcasting a table that's too large
# This will cause OOM on the driver or executors

# Check actual size before broadcasting
df_small.cache()
df_small.count()
size_bytes = spark.sparkContext._jvm.org.apache.spark.util.SizeEstimator.estimate(
    df_small._jdf
)
print(f"Estimated size: {size_bytes / 1024 / 1024:.1f} MB")

# Better approach: let AQE handle it dynamically
# AQE can convert a SortMergeJoin to BroadcastHashJoin at runtime
# if it discovers one side is actually small after shuffle
spark.conf.set("spark.sql.adaptive.enabled", "true")  # default in Databricks
```

**Common mistakes:**
- Broadcasting a table that's small in row count but wide (many columns, large strings) -- size in memory can be 5-10x larger than on disk
- Broadcasting inside a loop (the broadcast happens every iteration)
- Broadcasting when the "small" table is actually a complex subquery whose size Spark can't estimate

### How to Verify in the Query Plan

```
# What to look for:
# 1. "BroadcastHashJoin" node
# 2. "BroadcastExchange" below the small side
# 3. No "Exchange" (shuffle) on the large side

== Physical Plan ==
*(2) BroadcastHashJoin [key#10], [key#19], Inner, BuildRight, false
:- *(2) FileScan parquet [...]          <-- Large side: NO shuffle
+- BroadcastExchange [...]              <-- Small side: broadcast
   +- *(1) FileScan parquet [...]
```

> **Photon note:** On Databricks with Photon enabled, broadcast joins are executed natively in C++ and are significantly faster than JVM-based broadcast joins. Photon also raises the practical size limit for broadcast tables because its memory management is more efficient. You may see `PhotonBroadcastHashJoin` in explain output.

---

## 3. Sort Merge Join

The default strategy for joining two large datasets on an equi-join condition. Both sides are shuffled by the join key, sorted within each partition, and then merged using a two-pointer scan.

### How It Works

```
Left DataFrame                      Right DataFrame
     |                                    |
     v                                    v
Exchange (shuffle by key)         Exchange (shuffle by key)
     |                                    |
     v                                    v
Sort (by key within partition)    Sort (by key within partition)
     |                                    |
     +----------------+------------------+
                       |
                       v
              SortMergeJoin
       (two-pointer merge of sorted streams)
                       |
                       v
                    Result
```

### Reading It in the Query Plan

```
== Physical Plan ==
*(5) SortMergeJoin [key#10], [key#50], Inner
:- *(2) Sort [key#10 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(key#10, 200), ENSURE_REQUIREMENTS, [plan_id=80]
:     +- *(1) Filter isnotnull(key#10)
:        +- *(1) FileScan parquet [key#10, col_a#11]
+- *(4) Sort [key#50 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(key#50, 200), ENSURE_REQUIREMENTS, [plan_id=81]
      +- *(3) Filter isnotnull(key#50)
         +- *(3) FileScan parquet [key#50, col_b#51]
```

**What to look for:** `Exchange` + `Sort` + `SortMergeJoin`. Two `Exchange` nodes mean both sides are being shuffled.

### Performance Characteristics

| Aspect | Detail |
|---|---|
| Time complexity | O(n log n) per side for sort, O(n) for merge |
| Shuffle | Both sides shuffled by join key |
| Memory | Moderate -- sorted streams can spill to disk |
| Disk I/O | Sort may spill; check "Spill (Memory)" and "Spill (Disk)" in Spark UI |
| Scalability | Best strategy for two genuinely large datasets |

### When It's Chosen

- Both sides are too large to broadcast
- There's an equi-join condition (equality on join keys)
- `spark.sql.join.preferSortMergeJoin` is `true` (default)

### Optimizing Sort Merge Joins

```python
# 1. Pre-partition (bucket) tables by join key to skip shuffle
# If both tables are bucketed on the same key with the same number of buckets,
# Spark can skip the Exchange (shuffle) entirely

# Write bucketed table
(df_orders
 .write
 .bucketBy(256, "customer_id")
 .sortBy("customer_id")
 .saveAsTable("orders_bucketed"))

(df_customers
 .write
 .bucketBy(256, "customer_id")
 .sortBy("customer_id")
 .saveAsTable("customers_bucketed"))

# Join — no shuffle needed!
df_orders_b = spark.table("orders_bucketed")
df_customers_b = spark.table("customers_bucketed")
df_joined = df_orders_b.join(df_customers_b, "customer_id")
df_joined.explain()
# Look for: NO Exchange nodes, just Sort + SortMergeJoin

# 2. Increase shuffle partitions for very large joins
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# 3. Or let AQE handle it automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

> **Key takeaway:** Sort merge join is reliable and scales well, but the shuffle + sort overhead is real. If you're joining the same tables repeatedly, bucketing eliminates the shuffle cost entirely.

---

## 4. Shuffle Hash Join

A lesser-known strategy where both sides are shuffled by the join key, then the smaller side is built into an in-memory hash table within each partition (no global sort required).

### When It's Used

By default, Spark prefers sort-merge join over shuffle hash join. You must explicitly opt in:

```python
# Enable shuffle hash join preference
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

# Spark will choose ShuffledHashJoin when:
# 1. One side is at least 3x smaller than the other (per partition)
# 2. The smaller side fits in memory for hash table construction
# 3. preferSortMergeJoin is false
```

```sql
-- You can also hint it
SELECT /*+ SHUFFLE_HASH(small_table) */ *
FROM large_table
JOIN small_table ON large_table.key = small_table.key;
```

### How It Differs from Sort Merge Join

```
Sort Merge Join:                     Shuffle Hash Join:
  Shuffle both sides                   Shuffle both sides
  Sort both sides         vs.          Build hash table on smaller side
  Merge sorted streams                 Probe with larger side
```

### Trade-offs: Shuffle Hash vs. Sort Merge

| Dimension | ShuffledHashJoin | SortMergeJoin |
|---|---|---|
| CPU cost | Lower (no sort) | Higher (sort both sides) |
| Memory cost | Higher (hash table in memory per partition) | Lower (sorted streams can spill) |
| Spill tolerance | Poor -- hash table must fit in memory | Good -- sort can spill to disk |
| Skew sensitivity | High -- skewed partition = large hash table | Moderate -- sort spills gracefully |
| Best when | One side is 3-5x smaller, fits in partition memory | Both sides are very large |

### Reading It in the Query Plan

```
== Physical Plan ==
*(3) ShuffledHashJoin [key#10], [key#50], Inner, BuildLeft
:- Exchange hashpartitioning(key#10, 200), ENSURE_REQUIREMENTS
:  +- *(1) FileScan parquet [key#10, col_a#11]
+- Exchange hashpartitioning(key#50, 200), ENSURE_REQUIREMENTS
   +- *(2) FileScan parquet [key#50, col_b#51]
```

**What to look for:** Two `Exchange` nodes (like sort-merge) but `ShuffledHashJoin` instead of `Sort` + `SortMergeJoin`.

> **Photon note:** Photon's native hash join implementation is highly optimized. On Photon-enabled clusters, shuffle hash join can outperform sort merge join for a wider range of size ratios. Photon handles hash table memory more efficiently with off-heap allocation, making `ShuffledHashJoin` a more attractive option than on pure Spark.

---

## 5. Handling Data Skew in Joins

Data skew is the single most common cause of slow joins. It occurs when a small number of join keys have disproportionately many rows, causing one or a few tasks to process far more data than others.

### Diagnosing Skew

**In the Spark UI (Stages tab):**
```
Task Metrics:
  Min duration:    2 seconds
  25th percentile: 3 seconds
  Median:          4 seconds
  75th percentile: 5 seconds
  Max duration:    45 minutes    <-- THIS IS SKEW

  Min shuffle read:    10 MB
  Max shuffle read:    50 GB     <-- THIS IS SKEW
```

**Rule of thumb:** If the max task time is 10x or more the median, you have skew.

**Programmatic detection:**

```python
# Find skewed keys
from pyspark.sql import functions as F

key_counts = (
    df.groupBy("join_key")
    .count()
    .orderBy(F.desc("count"))
)

# Show top skewed keys
key_counts.show(20)

# Quantify the skew
stats = key_counts.select(
    F.mean("count").alias("mean"),
    F.stddev("count").alias("stddev"),
    F.max("count").alias("max"),
    F.min("count").alias("min"),
    F.expr("percentile_approx(count, 0.99)").alias("p99")
).collect()[0]

print(f"Mean: {stats['mean']:.0f}, Max: {stats['max']}, "
      f"P99: {stats['p99']}, Stddev: {stats['stddev']:.0f}")
# If max >> p99, you have skew on specific keys
```

### Strategy 1: AQE Skew Join Optimization (Recommended First)

Adaptive Query Execution can automatically detect and handle skew at runtime. This is the lowest-effort solution.

```python
# Enable AQE skew join handling (on by default in Databricks Runtime 7.3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Tuning parameters
# A partition is considered skewed if:
#   size > skewedPartitionFactor * median_size  AND
#   size > skewedPartitionThresholdInBytes
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")       # default: 5
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")  # default: 256MB
```

**How AQE handles skew:**

```
Before AQE (skewed):
  Partition 0: 10 MB    -> Task 0: 2 sec
  Partition 1: 10 MB    -> Task 1: 2 sec
  Partition 2: 50 GB    -> Task 2: 45 min  <-- SKEW
  Partition 3: 10 MB    -> Task 3: 2 sec

After AQE (split):
  Partition 0:   10 MB  -> Task 0: 2 sec
  Partition 1:   10 MB  -> Task 1: 2 sec
  Partition 2a:  10 GB  -> Task 2a: 9 min  (split!)
  Partition 2b:  10 GB  -> Task 2b: 9 min  (split!)
  Partition 2c:  10 GB  -> Task 2c: 9 min  (split!)
  Partition 2d:  10 GB  -> Task 2d: 9 min  (split!)
  Partition 2e:  10 GB  -> Task 2e: 9 min  (split!)
  Partition 3:   10 MB  -> Task 3: 2 sec
```

AQE splits the skewed partition and replicates the corresponding partition from the other side of the join. Look for `CustomShuffleReader` or `AQEShuffleRead` in the plan with `coalesced and skewed` annotations.

### Strategy 2: Key Salting

When AQE isn't enough or you need more control, salting distributes skewed keys across multiple partitions manually.

```python
from pyspark.sql import functions as F
import random

NUM_SALT_BUCKETS = 10  # Spread skewed keys across 10 partitions

# Step 1: Add random salt to the large (skewed) side
df_large_salted = df_large.withColumn(
    "salt", (F.rand() * NUM_SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Step 2: Explode the small side to match all salt values
from pyspark.sql.types import ArrayType, IntegerType

df_small_exploded = (
    df_small
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(NUM_SALT_BUCKETS)])))
    .withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))
)

# Step 3: Join on the salted key
df_result = (
    df_large_salted
    .join(df_small_exploded, "salted_key")
    .drop("salt", "salted_key")
)
```

**Why this works:**
- A key with 10 million rows is now split across 10 partitions (1 million each)
- The small side is replicated 10x, but since it's small, this is affordable
- The join is now balanced across partitions

**When to use salting:**
- AQE skew handling is insufficient for extreme skew (e.g., a single key with billions of rows)
- You need fine-grained control over how skew is distributed
- You're on an older Spark version without AQE

### Strategy 3: Pre-Aggregation Before Join

If you're going to aggregate after the join anyway, aggregate first to reduce skewed rows before the join.

```python
# BEFORE: Join then aggregate (processes all skewed rows through join)
df_result = (
    df_orders
    .join(df_products, "product_id")
    .groupBy("category")
    .agg(F.sum("amount").alias("total_amount"))
)

# AFTER: Pre-aggregate, then join (reduces skew before join)
df_order_totals = (
    df_orders
    .groupBy("product_id")
    .agg(F.sum("amount").alias("total_amount"))
)

df_result = (
    df_order_totals
    .join(df_products, "product_id")
    .groupBy("category")
    .agg(F.sum("total_amount").alias("total_amount"))
)
```

### Strategy 4: Isolate-and-Broadcast Pattern

Separate skewed keys from non-skewed keys. Broadcast-join the skewed keys; sort-merge-join the rest. Union the results.

```python
# Identify the skewed keys
SKEWED_KEYS = ["null_key", "unknown", "N/A"]  # or compute dynamically

# Split the large side
df_large_skewed = df_large.filter(F.col("join_key").isin(SKEWED_KEYS))
df_large_normal = df_large.filter(~F.col("join_key").isin(SKEWED_KEYS))

# Split the small side to match
df_small_skewed = df_small.filter(F.col("join_key").isin(SKEWED_KEYS))
df_small_normal = df_small.filter(~F.col("join_key").isin(SKEWED_KEYS))

# Broadcast join for skewed keys (small_skewed is tiny)
df_joined_skewed = df_large_skewed.join(
    broadcast(df_small_skewed), "join_key"
)

# Sort-merge join for everything else (balanced)
df_joined_normal = df_large_normal.join(df_small_normal, "join_key")

# Combine
df_result = df_joined_skewed.unionByName(df_joined_normal)
```

**Dynamic skewed key detection:**

```python
# Find keys with more than 1M rows
skewed_keys_df = (
    df_large
    .groupBy("join_key")
    .count()
    .filter(F.col("count") > 1_000_000)
    .select("join_key")
)

# Collect to driver (should be a small list)
skewed_keys = [row["join_key"] for row in skewed_keys_df.collect()]
```

### Strategy 5: Skew Hints (Databricks)

Databricks provides dedicated skew hints that internally handle the salting/splitting logic.

```sql
-- Databricks SQL skew hint
SELECT /*+ SKEW('orders', 'customer_id') */
    o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- With specific skewed values
SELECT /*+ SKEW('orders', 'customer_id', ('key1', 'key2', 'key3')) */
    o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

```python
# Python API skew hint
df_result = (
    df_orders.hint("skew", "customer_id")
    .join(df_customers, "customer_id")
)

# With specific values
df_result = (
    df_orders.hint("skew", "customer_id", ["key1", "key2"])
    .join(df_customers, "customer_id")
)
```

> **Key takeaway:** Start with AQE (zero code changes). If that's not enough, try the Databricks skew hint. For extreme skew or maximum control, use salting or isolate-and-broadcast.

---

## 6. Join Optimization Patterns

### Pattern 1: Filter Before Join

Push filters down before the join to reduce the amount of data shuffled.

```python
# BEFORE: Filter after join — all data shuffled
df_result = (
    df_orders.join(df_customers, "customer_id")
    .filter(F.col("order_date") >= "2025-01-01")
    .filter(F.col("country") == "US")
)

# AFTER: Filter before join — less data shuffled
df_result = (
    df_orders.filter(F.col("order_date") >= "2025-01-01")
    .join(
        df_customers.filter(F.col("country") == "US"),
        "customer_id"
    )
)
```

```
# Plan comparison:
#
# BEFORE:                          AFTER:
# Filter (order_date, country)     SortMergeJoin
# └── SortMergeJoin                ├── Exchange (shuffles LESS data)
#     ├── Exchange (ALL orders)    │   └── Filter (order_date >= 2025)
#     │   └── Scan orders          │       └── Scan orders
#     └── Exchange (ALL customers) └── Exchange (shuffles LESS data)
#         └── Scan customers           └── Filter (country = US)
#                                          └── Scan customers
```

> **Note:** Catalyst often pushes filters down automatically for simple predicates. But it cannot push down filters that reference columns from both sides of the join, filters involving UDFs, or filters after complex transformations. Always verify with `explain()`.

### Pattern 2: Project Before Join (Column Pruning)

Select only the columns you need before the join to reduce shuffle data size.

```python
# BEFORE: Shuffling all 50 columns from both tables
df_result = df_orders.join(df_products, "product_id")

# AFTER: Only shuffle the columns you need
df_result = (
    df_orders.select("product_id", "quantity", "amount", "order_date")
    .join(
        df_products.select("product_id", "product_name", "category"),
        "product_id"
    )
)
```

**Impact:** If each table has 50 columns but you only need 4 from each, you reduce shuffle data by ~90%.

> **Note:** Spark's Catalyst optimizer performs automatic column pruning in many cases, but explicit projection ensures it happens and makes the intent clear. This is especially important when column pruning may not propagate through caching, views, or complex subqueries.

### Pattern 3: Replace Cross Join with Proper Conditions

```python
# BEFORE: Accidental cross join (missing join condition!)
# This produces N * M rows and is almost always a bug
df_result = df_a.join(df_b)  # No condition = Cartesian product

# Spark will refuse this by default unless you explicitly enable it:
# spark.conf.set("spark.sql.crossJoin.enabled", "true")

# AFTER: Add the proper join condition
df_result = df_a.join(df_b, df_a.key == df_b.key)

# If you truly need a cross join (rare), be explicit:
df_result = df_a.crossJoin(df_b)
```

```sql
-- SQL: watch for implicit cross joins
-- BEFORE (implicit cross join — dangerous!)
SELECT * FROM orders, customers
WHERE orders.amount > 100;

-- AFTER (proper join)
SELECT * FROM orders
JOIN customers ON orders.customer_id = customers.customer_id
WHERE orders.amount > 100;
```

### Pattern 4: Multi-Way Join Ordering

When joining multiple tables, order matters. Join the smaller/more-filtered tables first to reduce intermediate result sizes.

```python
# BEFORE: Arbitrary join order
df_result = (
    df_fact_table           # 1 billion rows
    .join(df_dim_large, "dim_key_1")   # 10 million rows
    .join(df_dim_small, "dim_key_2")   # 1,000 rows
    .join(df_dim_tiny, "dim_key_3")    # 50 rows
)

# AFTER: Start with the broadcast-eligible small tables
df_result = (
    df_fact_table
    .join(broadcast(df_dim_tiny), "dim_key_3")    # Broadcast: no shuffle
    .join(broadcast(df_dim_small), "dim_key_2")   # Broadcast: no shuffle
    .join(df_dim_large, "dim_key_1")              # Only this one shuffles
)
```

> **Note:** Catalyst's join reordering (`spark.sql.cbo.joinReorder.enabled`) can do this automatically when table statistics are available, but it requires `ANALYZE TABLE ... COMPUTE STATISTICS` to have been run. Explicit ordering is more reliable.

```python
# Ensure stats are available for CBO
spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE customers COMPUTE STATISTICS FOR ALL COLUMNS")

# Enable CBO join reordering
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.dp.star.filter", "false")
```

### Pattern 5: Map-Side Combine Before Join

When you know many rows per key exist on the same partition, pre-aggregate within each partition before shuffling.

```python
# BEFORE: Every row shuffled
df_result = (
    df_events
    .join(df_users, "user_id")
    .groupBy("user_id", "user_name")
    .agg(F.count("*").alias("event_count"))
)

# AFTER: Aggregate first, then join (far fewer rows shuffled)
df_event_counts = (
    df_events
    .groupBy("user_id")
    .agg(F.count("*").alias("event_count"))
)

df_result = df_event_counts.join(df_users, "user_id")
```

### Pattern 6: Semi/Anti Join Instead of Full Join

When you only need to filter one table based on the existence (or absence) of matching keys in another, use semi or anti joins instead of a full inner/left join.

```python
# BEFORE: Full join then discard the right-side columns
df_active_customers = (
    df_customers
    .join(df_orders, "customer_id")
    .select(df_customers.columns)  # Only keep customer columns
    .distinct()
)

# AFTER: Left semi join — much more efficient
df_active_customers = df_customers.join(
    df_orders, "customer_id", "left_semi"
)

# Anti join: find customers with NO orders
df_inactive_customers = df_customers.join(
    df_orders, "customer_id", "left_anti"
)
```

```sql
-- SQL equivalents
-- Semi join
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders);

-- Or using EXISTS (same plan)
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- Anti join
SELECT * FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL);

-- Or using NOT EXISTS (safer with NULLs)
SELECT * FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

**Why semi/anti joins are faster:**
- Spark only needs to check for key existence, not match all rows
- No duplicate rows from the right side
- Less data shuffled (right side can be deduplicated early)
- Query plan shows `LeftSemi` or `LeftAnti` instead of `Inner`

---

## 7. Join Type Selection Guide

| Join Type | SQL Syntax | Result | When to Use | Performance Notes |
|---|---|---|---|---|
| **INNER** | `JOIN` | Only matching rows from both sides | Default when you need matched records | Most optimizable; supports all strategies |
| **LEFT (OUTER)** | `LEFT JOIN` | All left rows + matching right rows (NULL if no match) | Keep all left records, enrich with right | Similar to inner but right side can't be used for semi optimizations |
| **RIGHT (OUTER)** | `RIGHT JOIN` | All right rows + matching left rows (NULL if no match) | Same as LEFT but reversed; prefer LEFT for readability | Internally rewritten to LEFT join by Catalyst |
| **FULL OUTER** | `FULL OUTER JOIN` | All rows from both sides (NULLs where no match) | Need complete union of both datasets | Cannot use BroadcastHashJoin; always SortMergeJoin or ShuffledHashJoin |
| **CROSS** | `CROSS JOIN` | Cartesian product (N x M rows) | Truly need all combinations (rare) | Extremely expensive; verify you actually need this |
| **LEFT SEMI** | `LEFT SEMI JOIN` | Left rows that have a match on the right | Filter left table by existence in right | Very efficient; right side can short-circuit on first match |
| **LEFT ANTI** | `LEFT ANTI JOIN` | Left rows that have NO match on the right | Find missing/unmatched records | Very efficient; right side can short-circuit on first match |

### Decision Framework

```
Do you need columns from the right table?
├── NO
│   ├── Want rows that HAVE a match? → LEFT SEMI JOIN
│   └── Want rows that DON'T have a match? → LEFT ANTI JOIN
└── YES
    ├── Only care about matched rows? → INNER JOIN
    ├── Keep all left rows? → LEFT JOIN
    ├── Keep all right rows? → RIGHT JOIN (or rewrite as LEFT)
    └── Keep everything? → FULL OUTER JOIN
```

> **Key takeaway:** Always use the most restrictive join type that satisfies your requirements. `LEFT SEMI` and `LEFT ANTI` are often 2-5x faster than `INNER`/`LEFT` joins for existence checks because they skip unnecessary column transfer and deduplication.

---

## 8. Common Join Anti-Patterns

### Anti-Pattern 1: Joining on Non-Deterministic Expressions

```python
# BAD: Non-deterministic join condition
# rand() produces different values on each evaluation,
# causing incorrect or unpredictable results
df_result = df_a.join(
    df_b,
    (F.rand() * 10).cast("int") == (F.rand() * 10).cast("int")
)

# BAD: Using current_timestamp in join condition
df_result = df_a.join(
    df_b,
    F.abs(df_a.timestamp - F.current_timestamp()) < F.expr("INTERVAL 1 HOUR")
)

# GOOD: Materialize non-deterministic values first
df_a_with_bucket = df_a.withColumn("bucket", (F.rand() * 10).cast("int"))
df_a_with_bucket.cache()  # Materialize!
df_result = df_a_with_bucket.join(df_b, "bucket")
```

**Why this is bad:** Non-deterministic expressions can be evaluated multiple times during query execution. The value used during shuffle partitioning may differ from the value used during the actual join comparison, producing wrong results or missed matches.

### Anti-Pattern 2: Joining with Implicit Type Coercion

```python
# BAD: Join key types don't match — causes implicit cast
# df_a.key is IntegerType, df_b.key is StringType
df_result = df_a.join(df_b, "key")

# This forces Spark to cast one side, which:
# 1. Prevents predicate pushdown to the data source
# 2. May invalidate partition pruning
# 3. Can produce unexpected NULL matches

# GOOD: Explicit type alignment
df_result = df_a.join(
    df_b.withColumn("key", F.col("key").cast("int")),
    "key"
)

# BEST: Fix the schema at the source
```

**How to detect:** Look for `Cast` nodes in the query plan immediately below a join. Run this check:

```python
# Check join key types
print(f"Left key type: {df_a.schema['key'].dataType}")
print(f"Right key type: {df_b.schema['key'].dataType}")
```

### Anti-Pattern 3: Unnecessary Cartesian Products

```python
# BAD: Missing join condition creates Cartesian product
# 100K rows x 100K rows = 10 billion rows!
df_result = df_a.crossJoin(df_b).filter(df_a.key == df_b.key)

# GOOD: Use a proper equi-join
df_result = df_a.join(df_b, df_a.key == df_b.key)
```

```sql
-- BAD: Comma-separated FROM with join condition in WHERE
-- (may or may not be optimized to a join depending on version)
SELECT * FROM orders o, products p
WHERE o.product_id = p.product_id AND o.amount > 100;

-- GOOD: Explicit JOIN syntax
SELECT * FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.amount > 100;
```

**Detecting accidental Cartesian products:**
```python
# Check your plan for CartesianProduct or BroadcastNestedLoopJoin
plan = df_result._jdf.queryExecution().executedPlan().toString()
if "CartesianProduct" in plan or "BroadcastNestedLoopJoin" in plan:
    print("WARNING: Potential Cartesian product detected!")
```

### Anti-Pattern 4: Joining Large-to-Large When One Side Could Be Pre-Filtered

```python
# BAD: Join two 100GB tables, then filter to 1% of results
df_result = (
    df_transactions    # 100 GB, 1 billion rows
    .join(
        df_accounts,   # 100 GB, 500 million rows
        "account_id"
    )
    .filter(F.col("account_type") == "premium")  # Only 5M accounts
    .filter(F.col("txn_date") >= "2025-01-01")   # Only 3 months of data
)

# GOOD: Filter both sides aggressively before join
df_recent_txns = df_transactions.filter(
    F.col("txn_date") >= "2025-01-01"
)  # ~25 GB instead of 100 GB

df_premium_accounts = df_accounts.filter(
    F.col("account_type") == "premium"
)  # ~1 GB instead of 100 GB — might even be broadcastable!

df_result = df_recent_txns.join(
    broadcast(df_premium_accounts),  # Now small enough to broadcast!
    "account_id"
)
```

**Impact:** Shuffling 100 GB + 100 GB vs. shuffling 25 GB + broadcasting 1 GB. The second version is 10-50x faster.

### Anti-Pattern 5: Repeated Joins to the Same Table

```python
# BAD: Joining to the same lookup table multiple times
df_result = (
    df_orders
    .join(df_products, df_orders.src_product_id == df_products.product_id)
    .withColumnRenamed("product_name", "src_product_name")
    .join(df_products, df_orders.dst_product_id == df_products.product_id)
    .withColumnRenamed("product_name", "dst_product_name")
)
# This broadcasts/shuffles df_products twice!

# GOOD: Alias and join once per logical use, but cache the lookup
df_products.cache()

df_src = df_products.alias("src")
df_dst = df_products.alias("dst")

df_result = (
    df_orders
    .join(broadcast(df_src),
          df_orders.src_product_id == F.col("src.product_id"))
    .join(broadcast(df_dst),
          df_orders.dst_product_id == F.col("dst.product_id"))
    .select(
        df_orders["*"],
        F.col("src.product_name").alias("src_product_name"),
        F.col("dst.product_name").alias("dst_product_name"),
    )
)
```

### Summary: Join Anti-Pattern Checklist

```
Before submitting a join-heavy job, verify:

[ ] All join keys have matching types (no implicit casts)
[ ] No non-deterministic expressions in join conditions
[ ] No accidental Cartesian products (every join has a condition)
[ ] Filters are applied before joins, not after
[ ] Only needed columns are selected before joins
[ ] Using the most restrictive join type (semi/anti where possible)
[ ] Checked explain() for the expected join strategy
[ ] Considered broadcast for any table < 100 MB
[ ] Verified no extreme data skew on join keys
```

> **Photon note:** Photon accelerates all join types but especially benefits BroadcastHashJoin and ShuffledHashJoin due to native vectorized hash table operations. When running on Photon, you may see `PhotonBroadcastHashJoin`, `PhotonSortMergeJoin`, or `PhotonShuffledHashJoin` in explain output. The optimization patterns in this file apply equally -- Photon makes them faster but doesn't eliminate the need for them.

---

**Next:** [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) — Deep dive into shuffle mechanics, partition sizing, and AQE.

**Previous:** [4-databricks-diagnostics.md](4-databricks-diagnostics.md) — Databricks-specific monitoring and diagnostics tools.
