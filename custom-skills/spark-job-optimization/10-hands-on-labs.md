# Hands-On Labs: Spark Job Optimization

These labs deliberately create performance problems and walk you through diagnosing and fixing them. All code is PySpark, runnable in Databricks notebooks, and uses only synthetic data generated with `spark.range()`.

---

## Lab 1: Diagnosing Data Skew

### Objective

Learn to identify data skew in a join operation by reading Spark UI stage details, find the skewed partition, and fix it using salting and AQE skew join handling.

### Setup

Create a skewed DataFrame where key `0` has 10 million rows and all other keys have 100 rows each.

```python
from pyspark.sql import functions as F

# Large table with heavy skew on key=0
skewed_df = (
    spark.range(0, 10_000_000)
    .withColumn("skew_key", F.lit(0))
    .withColumn("value_a", F.rand())
)

other_keys = (
    spark.range(0, 100)
    .crossJoin(spark.range(1, 101).withColumnRenamed("id", "key_id"))
    .withColumn("skew_key", F.col("key_id"))
    .withColumn("value_a", F.rand())
    .drop("key_id")
)

left_table = skewed_df.unionByName(other_keys)
left_table.cache()
left_table.count()  # Materialize

# Right table: uniform distribution, moderate size
right_table = (
    spark.range(0, 101)
    .withColumnRenamed("id", "skew_key")
    .withColumn("value_b", F.rand())
    .withColumn("category", F.concat(F.lit("cat_"), F.col("skew_key").cast("string")))
)
right_table.cache()
right_table.count()

print(f"Left table count: {left_table.count()}")
print(f"Right table count: {right_table.count()}")
```

Run the bad join:

```python
# Disable AQE to see the raw skew problem
spark.conf.set("spark.sql.adaptive.enabled", "false")

result = left_table.join(right_table, on="skew_key", how="inner")
result.write.format("noop").mode("overwrite").save()
```

### Observe

Open the Spark UI for this job and navigate to the stage that performs the join.

1. **Stages tab**: Find the stage with a shuffle. Note the total stage duration.
2. **Stage detail page**: Click into the join stage and look at the **Task** table.
3. **Task duration distribution**: Look at the Summary Metrics. Compare the **Max** task duration against the **Median** and **75th percentile**. A skewed job typically shows:
   - Median: ~1-2 seconds
   - Max: 30+ seconds (the task handling key `0`)
4. **Shuffle Read Size**: Sort tasks by shuffle read size descending. The top task will have read dramatically more data than the others.
5. **Event Timeline**: Look for one long bar stretching far beyond the others -- that is the skewed task holding up the entire stage.

### Diagnose

Answer these questions by examining the Spark UI:

1. How many tasks are in the join stage? What is the ratio of the longest task time to the median task time?
2. How much data (in MB) did the largest task shuffle-read compared to the median task?
3. Which partition number contains the skewed key? (Check the task index.)
4. What is the total stage wall-clock time? How much of that is spent waiting for the single slow task?

### Fix

#### Fix A: Salting the join key

```python
import random

SALT_BUCKETS = 20

# Salt the left (skewed) table: append a random salt to the key
left_salted = left_table.withColumn(
    "salt", (F.rand() * SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", F.concat(F.col("skew_key").cast("string"), F.lit("_"), F.col("salt").cast("string"))
)

# Explode the right table across all salt buckets
right_exploded = right_table.crossJoin(
    spark.range(0, SALT_BUCKETS).withColumnRenamed("id", "salt")
).withColumn(
    "salted_key", F.concat(F.col("skew_key").cast("string"), F.lit("_"), F.col("salt").cast("string"))
)

# Join on the salted key
result_salted = left_salted.join(right_exploded, on="salted_key", how="inner")
result_salted = result_salted.drop("salt", "salted_key")

result_salted.write.format("noop").mode("overwrite").save()
```

The salt distributes key `0`'s 10M rows across 20 partitions instead of 1, so no single task handles more than ~500K rows.

#### Fix B: AQE skew join optimization

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64m")

result_aqe = left_table.join(right_table, on="skew_key", how="inner")
result_aqe.write.format("noop").mode("overwrite").save()
```

AQE detects the skewed partition at runtime and automatically splits it into smaller sub-partitions.

### Verify

```python
# Check the AQE plan for skew join indicators
result_aqe = left_table.join(right_table, on="skew_key", how="inner")
result_aqe.explain("formatted")
```

Look for `SkewJoin` annotations in the physical plan. In the Spark UI:

- The join stage should now show many more tasks (the skewed partition was split).
- The max task duration should be much closer to the median.
- Total stage time should drop significantly.

Compare the three approaches:

| Approach | Max Task Time | Stage Duration | Notes |
|---|---|---|---|
| No fix | ~30s+ | ~35s | One task dominates |
| Salting | ~2-3s | ~5s | Manual, requires code changes on both sides |
| AQE skew join | ~2-4s | ~5s | Automatic, no code changes needed |

---

## Lab 2: Broadcast Join vs Sort-Merge Join

### Objective

Understand the physical difference between a SortMergeJoin and a BroadcastHashJoin by reading `explain()` output, and learn when and how to use broadcast hints to eliminate shuffles.

### Setup

```python
from pyspark.sql import functions as F

# Disable AQE and auto-broadcast to see raw plans
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Large table: 50 million rows
large_table = (
    spark.range(0, 50_000_000)
    .withColumn("category_id", (F.col("id") % 500).cast("int"))
    .withColumn("amount", F.rand() * 1000)
    .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.col("id") % 365).cast("int")))
)

# Small dimension table: 500 rows
dim_table = (
    spark.range(0, 500)
    .withColumnRenamed("id", "category_id")
    .withColumn("category_name", F.concat(F.lit("Category_"), F.col("category_id").cast("string")))
    .withColumn("department", F.concat(F.lit("Dept_"), (F.col("category_id") % 10).cast("string")))
)

large_table.cache().count()
dim_table.cache().count()

print(f"Large table: {large_table.count()} rows")
print(f"Dim table: {dim_table.count()} rows")
```

Run the join without any hints (will default to SortMergeJoin):

```python
import time

# Bad: SortMergeJoin with full shuffle
start = time.time()
smj_result = large_table.join(dim_table, on="category_id", how="inner")
smj_result.write.format("noop").mode("overwrite").save()
smj_time = time.time() - start

print(f"SortMergeJoin time: {smj_time:.1f}s")
smj_result.explain("formatted")
```

### Observe

In the `explain()` output for the SortMergeJoin, identify these nodes:

```
Expected plan structure (simplified):
== Physical Plan ==
*(5) SortMergeJoin [category_id], [category_id], Inner
  :- *(2) Sort [category_id ASC], false, 0
  :  +- Exchange hashpartitioning(category_id, 200)    <-- SHUFFLE of large table
  :     +- *(1) Filter isnotnull(category_id)
  :        +- *(1) ...
  +- *(4) Sort [category_id ASC], false, 0
     +- Exchange hashpartitioning(category_id, 200)    <-- SHUFFLE of dim table
        +- *(3) Filter isnotnull(category_id)
           +- *(3) ...
```

Key observations:
1. **Two Exchange nodes**: Both tables are shuffled across the cluster. The large table shuffle moves tens of millions of rows.
2. **Two Sort nodes**: Both sides must be sorted on the join key before merging.
3. In Spark UI, the **Shuffle Write** and **Shuffle Read** metrics will show substantial data movement.

### Diagnose

1. How many Exchange (shuffle) nodes are in the SortMergeJoin plan?
2. How much data is shuffled in total across both exchanges? (Check Spark UI SQL tab, click the query, look at shuffle bytes.)
3. What is the total number of stages? (SortMergeJoin typically needs at least 3 stages: 2 for shuffle + 1 for the merge.)

### Fix

```python
from pyspark.sql.functions import broadcast

# Good: BroadcastHashJoin -- small table is broadcast to all executors
start = time.time()
bhj_result = large_table.join(broadcast(dim_table), on="category_id", how="inner")
bhj_result.write.format("noop").mode("overwrite").save()
bhj_time = time.time() - start

print(f"BroadcastHashJoin time: {bhj_time:.1f}s")
bhj_result.explain("formatted")
```

Expected plan with broadcast:

```
== Physical Plan ==
*(2) BroadcastHashJoin [category_id], [category_id], Inner, BuildRight
  :- *(2) Filter isnotnull(category_id)
  :  +- *(2) ...
  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, int, false]))
     +- *(1) ...
```

Key differences:
- **Zero Exchange on the large table**: No shuffle of the 50M rows.
- **BroadcastExchange** replaces the shuffle of the small table. This copies the 500-row table to every executor (negligible cost).
- **No Sort nodes**: BroadcastHashJoin uses a hash table, not sorting.
- Stages reduced from 3+ to 1-2.

### Verify

```python
print(f"SortMergeJoin: {smj_time:.1f}s")
print(f"BroadcastHashJoin: {bhj_time:.1f}s")
print(f"Speedup: {smj_time / bhj_time:.1f}x")
```

Also compare in the Spark UI SQL tab:

| Metric | SortMergeJoin | BroadcastHashJoin |
|---|---|---|
| Exchange (shuffle) nodes | 2 | 0 (only BroadcastExchange) |
| Shuffle bytes written | Hundreds of MB | ~0 |
| Sort nodes | 2 | 0 |
| Stages | 3+ | 1-2 |
| Wall time | Slower | Faster |

Finally, restore defaults:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB default
```

---

## Lab 3: Shuffle Spill Investigation

### Objective

Learn to detect and fix shuffle spill (when shuffle data exceeds available memory and must be written to disk), which severely degrades performance.

### Setup

```python
from pyspark.sql import functions as F

# Force small memory per task to trigger spill
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "10")  # Too few partitions for the data

# Generate a large dataset for aggregation
large_data = (
    spark.range(0, 100_000_000)
    .withColumn("group_key", (F.col("id") % 50_000).cast("int"))
    .withColumn("metric_a", F.rand() * 1000)
    .withColumn("metric_b", F.rand() * 500)
    .withColumn("payload", F.repeat(F.lit("X"), 200))  # Inflate row size
)

large_data.cache().count()
print(f"Row count: {large_data.count()}")
```

Run the aggregation that causes spill:

```python
import time

start = time.time()
agg_result = (
    large_data
    .groupBy("group_key")
    .agg(
        F.sum("metric_a").alias("total_a"),
        F.avg("metric_b").alias("avg_b"),
        F.count("*").alias("row_count"),
        F.collect_list("payload").alias("payloads"),  # Expensive: collects all strings per group
    )
)
agg_result.write.format("noop").mode("overwrite").save()
spill_time = time.time() - start
print(f"Aggregation with spill: {spill_time:.1f}s")
```

### Observe

Navigate through the Spark UI to find the spill:

1. **Jobs tab**: Find the job for this aggregation. Click into it.
2. **Stages tab**: Find the stage that performs the shuffle (the aggregation stage). Note the stage ID.
3. **Stage detail page**: Click into the shuffle stage. Look at the **Stage Summary** section at the top and find:
   - **Shuffle Spill (Memory)**: The amount of data that had to be spilled. This is the deserialized in-memory size.
   - **Shuffle Spill (Disk)**: The amount of data written to disk. This is the serialized on-disk size.
   - If Shuffle Spill (Disk) > 0, spilling occurred.
4. **Task table**: Sort by "Spill (Disk)" descending. Observe which tasks spilled the most.
5. **Executor tab**: Check if any executors show high GC time (a side effect of memory pressure from spill).

You should see something like:

```
Shuffle Spill (Memory): ~5-20 GB
Shuffle Spill (Disk):   ~1-5 GB
```

### Diagnose

1. How much data was spilled to disk in total across all tasks?
2. What is the ratio of Shuffle Spill (Memory) to Shuffle Spill (Disk)? (This ratio indicates the compression benefit of serialization.)
3. With only 10 shuffle partitions and 100M rows, how many rows does each partition handle on average? Is this reasonable?
4. Which operation is the main spill culprit: the sum/avg/count, or the `collect_list`?

### Fix

Three-part fix: increase partitions, remove collect_list, and re-enable AQE.

```python
# Fix 1: Increase shuffle partitions to spread the data
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Fix 2: Remove collect_list (the worst offender) -- aggregate differently
start = time.time()
agg_fixed = (
    large_data
    .groupBy("group_key")
    .agg(
        F.sum("metric_a").alias("total_a"),
        F.avg("metric_b").alias("avg_b"),
        F.count("*").alias("row_count"),
        F.max("payload").alias("sample_payload"),  # Take one sample instead of collecting all
    )
)
agg_fixed.write.format("noop").mode("overwrite").save()
fixed_time = time.time() - start
print(f"Aggregation after fix: {fixed_time:.1f}s")
```

```python
# Fix 3: Enable AQE for automatic partition coalescing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

start = time.time()
agg_aqe = (
    large_data
    .groupBy("group_key")
    .agg(
        F.sum("metric_a").alias("total_a"),
        F.avg("metric_b").alias("avg_b"),
        F.count("*").alias("row_count"),
        F.max("payload").alias("sample_payload"),
    )
)
agg_aqe.write.format("noop").mode("overwrite").save()
aqe_time = time.time() - start
print(f"Aggregation with AQE: {aqe_time:.1f}s")
```

### Verify

```python
print(f"Original (10 partitions + collect_list): {spill_time:.1f}s")
print(f"Fixed (200 partitions + no collect_list): {fixed_time:.1f}s")
print(f"AQE (200 partitions + auto-coalesce):    {aqe_time:.1f}s")
```

In the Spark UI, compare the shuffle stages:

| Metric | Before | After |
|---|---|---|
| Shuffle partitions | 10 | 200 |
| Shuffle Spill (Disk) | Multiple GB | 0 or near-zero |
| GC time per task | High | Low |
| Task duration (max) | Very high | Uniform |

Restore defaults:

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## Lab 4: Small File Problem

### Objective

Understand how writing too many small files degrades both write and subsequent read performance, and learn multiple strategies to fix it: repartitioning, `optimizeWrite`, and Delta `OPTIMIZE`.

### Setup

```python
from pyspark.sql import functions as F

output_path_bad = "/tmp/lab4_small_files/bad_output"
output_path_fixed = "/tmp/lab4_small_files/fixed_output"
output_path_optimized = "/tmp/lab4_small_files/optimized_output"

# Clean up any previous runs
dbutils.fs.rm("/tmp/lab4_small_files", recurse=True)

# Generate data and force it into many partitions
data = (
    spark.range(0, 1_000_000)
    .withColumn("category", (F.col("id") % 100).cast("int"))
    .withColumn("value", F.rand() * 1000)
    .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.col("id") % 30).cast("int")))
    .repartition(2000)  # Way too many partitions for this data volume
)

data.cache().count()
```

Write with too many partitions:

```python
import time

start = time.time()
(
    data
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path_bad)
)
write_time_bad = time.time() - start
print(f"Write time (2000 partitions): {write_time_bad:.1f}s")
```

### Observe

Examine the file listing:

```python
files = dbutils.fs.ls(output_path_bad)
parquet_files = [f for f in files if f.name.endswith(".parquet")]
print(f"Number of parquet files: {len(parquet_files)}")

total_size = sum(f.size for f in parquet_files)
avg_size = total_size / len(parquet_files) if parquet_files else 0
print(f"Total size: {total_size / (1024*1024):.1f} MB")
print(f"Average file size: {avg_size / 1024:.1f} KB")
print(f"Min file size: {min(f.size for f in parquet_files) / 1024:.1f} KB")
print(f"Max file size: {max(f.size for f in parquet_files) / 1024:.1f} KB")
```

You should see something like:

```
Number of parquet files: 2000
Average file size: ~5-15 KB
```

This is terrible. Optimal Parquet file size is 64-256 MB. Files under 1 MB are considered problematic.

Now measure read performance:

```python
# Read performance with 2000 tiny files
spark.catalog.clearCache()
start = time.time()
df_bad = spark.read.format("delta").load(output_path_bad)
count_bad = df_bad.groupBy("category").agg(F.sum("value")).count()
read_time_bad = time.time() - start
print(f"Read time (2000 files): {read_time_bad:.1f}s")
```

In the Spark UI, the read will show 2000 tasks in the scan stage, each processing a tiny amount of data. The overhead of scheduling and running 2000 tasks far exceeds the actual compute.

### Diagnose

1. How many files were created? What is the average file size?
2. What is the ratio of file metadata overhead to actual data? (For files under 10KB, metadata can be a significant fraction.)
3. In the Spark UI for the read job, how many scan tasks were created? What is the average task duration?
4. How does the driver's scheduling overhead compare to actual computation time?

### Fix

#### Fix 1: Repartition before write

```python
start = time.time()
(
    data
    .repartition(8)  # Reasonable number for ~15 MB of data
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path_fixed)
)
write_time_fixed = time.time() - start
print(f"Write time (8 partitions): {write_time_fixed:.1f}s")

files_fixed = dbutils.fs.ls(output_path_fixed)
parquet_fixed = [f for f in files_fixed if f.name.endswith(".parquet")]
print(f"Number of files: {len(parquet_fixed)}")
avg_fixed = sum(f.size for f in parquet_fixed) / len(parquet_fixed)
print(f"Average file size: {avg_fixed / (1024*1024):.2f} MB")
```

#### Fix 2: Use optimizeWrite (Databricks Delta feature)

```python
start = time.time()
(
    data
    .write
    .format("delta")
    .option("optimizeWrite", "true")  # Databricks auto-coalesces small files
    .mode("overwrite")
    .save(output_path_optimized)
)
write_time_opt = time.time() - start
print(f"Write time (optimizeWrite): {write_time_opt:.1f}s")

files_opt = dbutils.fs.ls(output_path_optimized)
parquet_opt = [f for f in files_opt if f.name.endswith(".parquet")]
print(f"Number of files: {len(parquet_opt)}")
```

#### Fix 3: Run OPTIMIZE after the fact

```python
spark.sql(f"OPTIMIZE delta.`{output_path_bad}`")

files_after_optimize = dbutils.fs.ls(output_path_bad)
parquet_after = [f for f in files_after_optimize if f.name.endswith(".parquet")]
print(f"Files after OPTIMIZE: {len(parquet_after)}")
# Note: old files still exist until VACUUM runs, but only compacted files are active
```

### Verify

Compare read performance:

```python
spark.catalog.clearCache()

start = time.time()
df_fixed = spark.read.format("delta").load(output_path_fixed)
df_fixed.groupBy("category").agg(F.sum("value")).count()
read_time_fixed = time.time() - start

print(f"Read time (2000 small files): {read_time_bad:.1f}s")
print(f"Read time (8 right-sized files): {read_time_fixed:.1f}s")
print(f"Speedup: {read_time_bad / read_time_fixed:.1f}x")
```

| Metric | 2000 Small Files | 8 Right-Sized Files |
|---|---|---|
| File count | 2000 | 8 |
| Avg file size | ~5-15 KB | ~2 MB |
| Scan tasks | 2000 | 8 |
| Read time | Slow | Fast |

Clean up:

```python
dbutils.fs.rm("/tmp/lab4_small_files", recurse=True)
```

---

## Lab 5: Query Plan Red Flags

### Objective

Learn to read `explain("formatted")` output and spot three common plan red flags: missing predicate pushdown (caused by a UDF in WHERE), Cartesian product joins, and wasteful `SELECT *` projections.

### Setup

```python
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

# Disable AQE so plans are not altered at runtime
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Create tables
orders = (
    spark.range(0, 5_000_000)
    .withColumn("customer_id", (F.col("id") % 10_000).cast("int"))
    .withColumn("product_id", (F.col("id") % 500).cast("int"))
    .withColumn("order_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 365).cast("int")))
    .withColumn("amount", F.rand() * 500)
    .withColumn("status", F.when(F.col("id") % 10 == 0, "cancelled").otherwise("completed"))
    .withColumn("notes", F.repeat(F.lit("note "), 20))
)

products = (
    spark.range(0, 500)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id").cast("string")))
    .withColumn("price", F.rand() * 100)
)

regions = (
    spark.range(0, 50)
    .withColumnRenamed("id", "region_id")
    .withColumn("region_name", F.concat(F.lit("Region_"), F.col("region_id").cast("string")))
)

orders.cache().count()
products.cache().count()
regions.cache().count()
```

Write the deliberately bad query:

```python
# --- BAD QUERY: Three issues hidden inside ---

# Issue 1: UDF in WHERE clause prevents predicate pushdown
@F.udf(BooleanType())
def is_recent(date_val):
    from datetime import date
    if date_val is None:
        return False
    return date_val >= date(2024, 6, 1)

# Issue 2: Cartesian product -- regions has no join key relationship to orders
# Issue 3: SELECT * pulls all columns including the large "notes" field

bad_result = (
    orders
    .select("*")                                      # Issue 3: SELECT * (pulls "notes" and all columns)
    .filter(is_recent(F.col("order_date")))            # Issue 1: UDF blocks predicate pushdown
    .join(products, on="product_id", how="inner")
    .crossJoin(regions)                                # Issue 2: Cartesian product
)

bad_result.explain("formatted")
```

### Observe

Read the physical plan output carefully. Look for these red flags:

**Red Flag 1 -- Missing Predicate Pushdown (UDF in filter)**

```
+- *(1) Filter UDF(order_date)      <-- Filter happens AFTER full scan
   +- *(1) Scan [...] [all columns]  <-- No PushedFilters on date
```

When a native filter like `col("order_date") >= "2024-06-01"` is used, you would see:
```
+- *(1) Scan [...] PushedFilters: [IsNotNull(order_date), GreaterThanOrEqual(order_date, 2024-06-01)]
```

The UDF is a black box to the optimizer; it cannot push the filter down into the scan or use partition pruning.

**Red Flag 2 -- Cartesian Product (BroadcastNestedLoopJoin or CartesianProduct)**

```
+- CartesianProduct                  <-- Every row of left joined with every row of right
   :- ...                           <-- 5M orders x products
   +- ...                           <-- 50 regions
```

This multiplies the result set by 50x (250M rows from 5M). If both sides are large, this can OOM.

**Red Flag 3 -- Unnecessary Columns in Scan**

```
+- Scan [...] ReadSchema: struct<id, customer_id, product_id, order_date, amount, status, notes>
```

The `notes` column (large string) is included even if downstream operations do not need it.

### Diagnose

1. Does the Scan node show any `PushedFilters` for `order_date`? Why not?
2. What type of join is used for the `regions` table? What is the output row count multiplier?
3. How many columns appear in the `ReadSchema` of the Scan node? Which ones are actually needed for the final result?

### Fix

Fix each issue one at a time and compare plans.

#### Fix Issue 1: Replace UDF with native Spark expression

```python
# Replace UDF filter with native expression
fix1 = (
    orders
    .select("*")
    .filter(F.col("order_date") >= "2024-06-01")  # Native: enables predicate pushdown
    .join(products, on="product_id", how="inner")
    .crossJoin(regions)
)

print("=== After Fix 1: Native filter ===")
fix1.explain("formatted")
# Now the Scan node should show:
# PushedFilters: [IsNotNull(order_date), GreaterThanOrEqual(order_date, 2024-06-01)]
```

#### Fix Issue 2: Replace Cartesian product with proper join

```python
# Add a region_id to orders so we can do a proper equi-join
orders_with_region = orders.withColumn("region_id", (F.col("customer_id") % 50).cast("int"))

fix2 = (
    orders_with_region
    .filter(F.col("order_date") >= "2024-06-01")
    .join(products, on="product_id", how="inner")
    .join(F.broadcast(regions), on="region_id", how="inner")  # Equi-join instead of crossJoin
)

print("=== After Fix 2: Equi-join replaces Cartesian ===")
fix2.explain("formatted")
# CartesianProduct is gone, replaced by BroadcastHashJoin on region_id
```

#### Fix Issue 3: Select only needed columns

```python
fix3 = (
    orders_with_region
    .select("product_id", "customer_id", "order_date", "amount", "status", "region_id")  # No "notes"
    .filter(F.col("order_date") >= "2024-06-01")
    .join(products, on="product_id", how="inner")
    .join(F.broadcast(regions), on="region_id", how="inner")
)

print("=== After Fix 3: Column pruning ===")
fix3.explain("formatted")
# ReadSchema no longer includes "notes"
# Reduced data shuffled in joins
```

### Verify

```python
import time

# Time the bad query (limit output to avoid Cartesian explosion)
start = time.time()
bad_count = (
    orders
    .filter(is_recent(F.col("order_date")))
    .join(products, on="product_id", how="inner")
    .crossJoin(regions)
    .count()
)
bad_time = time.time() - start

# Time the fixed query
start = time.time()
good_count = fix3.count()
good_time = time.time() - start

print(f"Bad query:  {bad_count:,} rows in {bad_time:.1f}s")
print(f"Good query: {good_count:,} rows in {good_time:.1f}s")
print(f"Row reduction: {bad_count / good_count:.0f}x fewer rows")
print(f"Speedup: {bad_time / good_time:.1f}x")
```

Restore settings:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## Lab 6: Memory Pressure and Caching

### Objective

Observe how different cache storage levels behave under memory pressure, understand cache eviction in the Spark UI Storage tab, and choose the right storage level for your workload.

### Setup

```python
from pyspark.sql import functions as F
from pyspark import StorageLevel

# Clear all cached data
spark.catalog.clearCache()

# Create a moderately large DataFrame (~500 MB in memory)
large_df = (
    spark.range(0, 20_000_000)
    .withColumn("group", (F.col("id") % 1000).cast("int"))
    .withColumn("value", F.rand() * 1000)
    .withColumn("payload_a", F.repeat(F.lit("A"), 50))
    .withColumn("payload_b", F.repeat(F.lit("B"), 50))
)

# Create a second large DataFrame to compete for memory
competing_df = (
    spark.range(0, 20_000_000)
    .withColumn("key", (F.col("id") % 500).cast("int"))
    .withColumn("metric", F.rand() * 500)
    .withColumn("filler", F.repeat(F.lit("Z"), 50))
)
```

#### Experiment A: MEMORY_ONLY under pressure

```python
# Cache with MEMORY_ONLY
large_df.persist(StorageLevel.MEMORY_ONLY)
count1 = large_df.count()  # Materialize the cache
print(f"Cached large_df: {count1} rows")
```

Check the Storage tab in Spark UI:
- Note the "Size in Memory" and "Fraction Cached" -- should be 100%.

```python
# Now cache the competing DataFrame, creating memory pressure
competing_df.persist(StorageLevel.MEMORY_ONLY)
count2 = competing_df.count()
print(f"Cached competing_df: {count2} rows")
```

### Observe

Go to the **Storage** tab in the Spark UI:

1. **Before memory pressure**: `large_df` shows "Fraction Cached: 100%"
2. **After caching `competing_df`**: Check both entries:
   - If memory was insufficient, one or both DataFrames may show "Fraction Cached: < 100%"
   - Partitions may have been evicted from `large_df` to make room for `competing_df`
3. Click on each cached RDD/DataFrame to see the per-executor breakdown:
   - Which executors evicted partitions?
   - How much memory is used vs. available for caching?

Now trigger a recomputation:

```python
import time

# This will need to recompute evicted partitions (slow)
start = time.time()
result1 = large_df.groupBy("group").agg(F.sum("value")).count()
recompute_time = time.time() - start
print(f"Query on partially-evicted cache: {recompute_time:.1f}s")
```

In the Spark UI job details, look for a mix of:
- `InMemoryTableScan` stages (fast, reading from cache)
- Recomputation stages (slow, re-executing the original plan for evicted partitions)

### Diagnose

1. What fraction of `large_df` was evicted when `competing_df` was cached?
2. In the job that queries `large_df` after eviction, how many tasks read from cache vs. recomputed?
3. What happened to `competing_df`'s cache? Was it fully materialized?

### Fix

Compare different storage levels:

```python
spark.catalog.clearCache()

# --- Storage Level Comparison ---

# MEMORY_ONLY: Fast, but evicts under pressure (partitions lost entirely)
large_df.persist(StorageLevel.MEMORY_ONLY)
large_df.count()

# Check storage
print("=== MEMORY_ONLY ===")
for rdd in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  {rdd.name()}: {rdd.memSize() / (1024*1024):.0f} MB memory, {rdd.diskSize() / (1024*1024):.0f} MB disk")

spark.catalog.clearCache()

# MEMORY_AND_DISK: Evicted partitions spill to disk instead of being lost
large_df.persist(StorageLevel.MEMORY_AND_DISK)
large_df.count()

print("=== MEMORY_AND_DISK ===")
for rdd in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  {rdd.name()}: {rdd.memSize() / (1024*1024):.0f} MB memory, {rdd.diskSize() / (1024*1024):.0f} MB disk")

spark.catalog.clearCache()

# MEMORY_ONLY_SER: Serialized in memory -- uses less memory but needs deserialization on read
large_df.persist(StorageLevel.MEMORY_ONLY_SER)
large_df.count()

print("=== MEMORY_ONLY_SER ===")
for rdd in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  {rdd.name()}: {rdd.memSize() / (1024*1024):.0f} MB memory, {rdd.diskSize() / (1024*1024):.0f} MB disk")

spark.catalog.clearCache()
```

### Verify

Run a benchmark comparing query time on each storage level under memory pressure:

```python
import time

results = {}

for level_name, level in [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
]:
    spark.catalog.clearCache()

    # Cache both DataFrames to create pressure
    large_df.persist(level)
    large_df.count()
    competing_df.persist(level)
    competing_df.count()

    # Query the first DataFrame (may have been partially evicted)
    start = time.time()
    large_df.groupBy("group").agg(F.sum("value")).collect()
    query_time = time.time() - start

    results[level_name] = query_time
    print(f"{level_name}: query time = {query_time:.1f}s")

spark.catalog.clearCache()

print("\n=== Summary ===")
for level_name, t in results.items():
    print(f"  {level_name}: {t:.1f}s")
```

Expected behavior:

| Storage Level | Memory Use | Under Pressure | Read Speed |
|---|---|---|---|
| MEMORY_ONLY | Highest (deserialized objects) | Evicted partitions are lost, must recompute | Fastest (when cached) |
| MEMORY_AND_DISK | Same in-memory, spills to disk | Evicted partitions read from disk (no recompute) | Fast from memory, slower from disk |
| MEMORY_ONLY_SER | Lower (serialized bytes) | Still evicts, but fits more partitions | Moderate (deserialization cost) |

**Guideline**: Use `MEMORY_AND_DISK` when you cannot afford recomputation. Use `MEMORY_ONLY_SER` when memory is tight and you need to cache more data. Avoid `MEMORY_ONLY` unless you are confident the data fits.

---

## Lab 7: AQE in Action

### Objective

See Adaptive Query Execution (AQE) work in real time by running the same query with AQE off and on, comparing partition coalescing, join strategy changes, and skew handling.

### Setup

```python
from pyspark.sql import functions as F

# Create test data
fact_table = (
    spark.range(0, 30_000_000)
    .withColumn("dim_key", (F.col("id") % 200).cast("int"))
    .withColumn("amount", F.rand() * 1000)
    .withColumn("event_type", F.when(F.col("id") % 3 == 0, "click")
                               .when(F.col("id") % 3 == 1, "view")
                               .otherwise("purchase"))
)

# Small dimension table (under 10MB after filtering)
dim_table = (
    spark.range(0, 10_000)
    .withColumnRenamed("id", "dim_key")
    .withColumn("label", F.concat(F.lit("Label_"), F.col("dim_key").cast("string")))
    .withColumn("active", F.when(F.col("dim_key") < 200, True).otherwise(False))
)

# Add skew to fact table: key 0 gets extra rows
skew_extra = (
    spark.range(0, 5_000_000)
    .withColumn("dim_key", F.lit(0).cast("int"))
    .withColumn("amount", F.rand() * 1000)
    .withColumn("event_type", F.lit("click"))
)

fact_with_skew = fact_table.unionByName(skew_extra)
fact_with_skew.cache().count()
dim_table.cache().count()
```

### Experiment A: AQE Disabled

```python
import time

# Disable AQE entirely
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Prevent auto-broadcast
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Run the query
start = time.time()
result_no_aqe = (
    fact_with_skew
    .join(dim_table.filter(F.col("active") == True), on="dim_key", how="inner")
    .groupBy("label", "event_type")
    .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 100)
)
result_no_aqe.write.format("noop").mode("overwrite").save()
no_aqe_time = time.time() - start

print(f"AQE disabled: {no_aqe_time:.1f}s")
print("\n=== Physical Plan (AQE OFF) ===")
result_no_aqe.explain("formatted")
```

Record from the explain output:
- Join type (should be SortMergeJoin)
- Number of Exchange nodes
- Shuffle partition count (200 fixed)

In the Spark UI SQL tab, click the query and note:
- Total shuffle bytes
- Number of output partitions from each shuffle
- Whether any tasks are skewed (check the stage with the join)

### Experiment B: AQE Enabled

```python
# Enable AQE with all features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
spark.conf.set("spark.sql.shuffle.partitions", "200")

start = time.time()
result_aqe = (
    fact_with_skew
    .join(dim_table.filter(F.col("active") == True), on="dim_key", how="inner")
    .groupBy("label", "event_type")
    .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 100)
)
result_aqe.write.format("noop").mode("overwrite").save()
aqe_time = time.time() - start

print(f"AQE enabled: {aqe_time:.1f}s")
print("\n=== Physical Plan (AQE ON) ===")
result_aqe.explain("formatted")
```

### Observe

Compare the two plans side by side. Look for these AQE optimizations:

**1. Partition Coalescing**

Without AQE:
```
Exchange hashpartitioning(dim_key, 200)   <-- Always 200 partitions
```

With AQE:
```
AQEShuffleRead coalesced                  <-- 200 reduced to e.g. 50
```

AQE measured the actual shuffle data size at runtime and reduced the partition count to avoid many near-empty partitions.

**2. Join Strategy Change**

Without AQE (auto-broadcast disabled by threshold setting):
```
SortMergeJoin [dim_key], [dim_key], Inner
```

With AQE (runtime stats reveal dim_table after filter is small):
```
BroadcastHashJoin [dim_key], [dim_key], Inner, BuildRight
```

AQE saw that `dim_table.filter(active == True)` produced only 200 rows at runtime and switched the join strategy.

**3. Skew Handling**

Without AQE, the join stage will have one task handling key `0` (5M+ rows) while others handle ~150K rows. With AQE and skew join enabled, the plan will show the skewed partition being split.

### Diagnose

1. How many shuffle partitions did AQE coalesce from 200 down to? (Check the SQL plan metrics in Spark UI.)
2. Did AQE change the join from SortMergeJoin to BroadcastHashJoin? Why?
3. In the AQE-disabled run, what was the max task duration in the join stage vs. the median? Did AQE fix this?

### Verify

```python
print(f"AQE disabled: {no_aqe_time:.1f}s")
print(f"AQE enabled:  {aqe_time:.1f}s")
print(f"Speedup:      {no_aqe_time / aqe_time:.1f}x")
```

| Feature | AQE Off | AQE On |
|---|---|---|
| Join strategy | SortMergeJoin | BroadcastHashJoin |
| Shuffle partitions | 200 (fixed) | Auto-coalesced (fewer) |
| Skew handling | None (1 slow task) | Auto-split skewed partition |
| Shuffle data | Higher | Lower (broadcast eliminates one shuffle) |
| Wall time | Slower | Faster |

---

## Lab 8: End-to-End Optimization Challenge

### Objective

Apply everything from the previous labs to optimize a deliberately poorly-written ETL pipeline. This lab contains 7 performance issues. Your goal is to find and fix all of them, measuring improvement at each step.

### Setup

Create the source data:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import time

# Disable AQE to make problems visible (we will re-enable as a fix)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", "200")

output_base = "/tmp/lab8_etl"
dbutils.fs.rm(output_base, recurse=True)

# --- Source data ---

# Transactions: 20M rows, skewed on customer_id
transactions = (
    spark.range(0, 20_000_000)
    .withColumn("txn_id", F.col("id"))
    .withColumn("customer_id",
        F.when(F.col("id") < 5_000_000, F.lit(1))  # Skew: 5M rows for customer 1
        .otherwise((F.col("id") % 50_000).cast("int") + 2)
    )
    .withColumn("product_id", (F.col("id") % 1_000).cast("int"))
    .withColumn("amount", F.rand() * 500)
    .withColumn("txn_date", F.date_add(F.lit("2024-01-01"), (F.col("id") % 365).cast("int")))
    .withColumn("txn_type", F.when(F.col("id") % 5 == 0, "refund").otherwise("purchase"))
    .withColumn("notes", F.repeat(F.lit("transaction note "), 10))  # Bloat column
    .drop("id")
)

# Customers: 50K rows (small enough to broadcast)
customers = (
    spark.range(1, 50_002)
    .withColumnRenamed("id", "customer_id")
    .withColumn("name", F.concat(F.lit("Customer_"), F.col("customer_id").cast("string")))
    .withColumn("segment", F.when(F.col("customer_id") % 5 == 0, "premium").otherwise("standard"))
    .withColumn("region_id", (F.col("customer_id") % 20).cast("int"))
)

# Products: 1K rows (tiny)
products = (
    spark.range(0, 1_000)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id").cast("string")))
    .withColumn("category", F.concat(F.lit("Cat_"), (F.col("product_id") % 50).cast("string")))
    .withColumn("base_price", F.rand() * 200)
)

# Regions: 20 rows (tiny)
regions = (
    spark.range(0, 20)
    .withColumnRenamed("id", "region_id")
    .withColumn("region_name", F.concat(F.lit("Region_"), F.col("region_id").cast("string")))
    .withColumn("country", F.when(F.col("region_id") < 10, "US").otherwise("EU"))
)

# Cache source data
transactions.cache().count()
customers.cache().count()
products.cache().count()
regions.cache().count()

print("Source data ready:")
print(f"  transactions: {transactions.count():,} rows")
print(f"  customers:    {customers.count():,} rows")
print(f"  products:     {products.count():,} rows")
print(f"  regions:      {regions.count():,} rows")
```

### The Bad ETL Pipeline

This pipeline has 7 intentional performance issues. Read through it carefully and try to spot them before looking at the answers.

```python
# ============================================================
# BAD ETL PIPELINE -- Find and fix the 7 performance issues!
# ============================================================

@F.udf(StringType())
def classify_amount(amount):
    """Issue 1: UDF instead of native Spark expression"""
    if amount is None:
        return "unknown"
    elif amount > 200:
        return "high"
    elif amount > 50:
        return "medium"
    else:
        return "low"

start_total = time.time()

# Step 1: Read all columns including "notes" (Issue 2: SELECT *)
enriched = transactions.select("*")

# Step 2: Apply UDF filter (Issue 1: UDF prevents pushdown + is slow)
enriched = enriched.withColumn("amount_class", classify_amount(F.col("amount")))
enriched = enriched.filter(F.col("txn_type") == "purchase")

# Step 3: Join with customers -- SortMergeJoin (Issue 3: should broadcast customers)
# Also skewed on customer_id (Issue 4: data skew not handled)
enriched = enriched.join(customers, on="customer_id", how="inner")

# Step 4: Join with products -- SortMergeJoin (Issue 3 again: should broadcast products)
enriched = enriched.join(products, on="product_id", how="inner")

# Step 5: Join with regions -- SortMergeJoin (Issue 3 yet again)
enriched = enriched.join(regions, on="region_id", how="inner")

# Step 6: Aggregate
summary = (
    enriched
    .groupBy("region_name", "country", "category", "segment", "amount_class")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)

# Step 7: Write with too many partitions (Issue 5: small file problem)
(
    summary
    .repartition(500)  # Way too many partitions for an aggregation result
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{output_base}/bad_summary")
)

# Step 8: Write a detail table -- recomputes everything (Issue 6: no caching of shared computation)
detail = (
    transactions.select("*")
    .withColumn("amount_class", classify_amount(F.col("amount")))
    .filter(F.col("txn_type") == "purchase")
    .join(customers, on="customer_id", how="inner")
    .join(products, on="product_id", how="inner")
    .join(regions, on="region_id", how="inner")
)

(
    detail
    .repartition(500)  # Issue 5 again
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{output_base}/bad_detail")
)

# Issue 7: AQE is disabled (set at the top of this lab)

bad_total_time = time.time() - start_total
print(f"\nBAD ETL total time: {bad_total_time:.1f}s")
```

### Observe

Before fixing anything, examine the Spark UI for the bad pipeline:

1. **SQL tab**: Click on each query. Count the number of Exchange nodes. Note the join strategies.
2. **Stages tab**: Find the join stages. Look for skewed tasks (sort by duration descending).
3. **Stage detail for join on customer_id**: Find the task handling customer_id=1. Note its duration vs. others.
4. **File output**: Check how many files were written:

```python
for subdir in ["bad_summary", "bad_detail"]:
    path = f"{output_base}/{subdir}"
    try:
        files = dbutils.fs.ls(path)
        parquet_files = [f for f in files if f.name.endswith(".parquet")]
        total_size = sum(f.size for f in parquet_files)
        print(f"{subdir}: {len(parquet_files)} files, {total_size/(1024*1024):.1f} MB total, "
              f"{total_size/len(parquet_files)/1024:.1f} KB avg")
    except Exception as e:
        print(f"{subdir}: {e}")
```

### Issue Checklist

Find these 7 issues in the code above:

| # | Issue | Location | Category |
|---|---|---|---|
| 1 | UDF `classify_amount` instead of native `F.when()` | Step 2 | Catalyst bypass |
| 2 | `SELECT *` pulls unnecessary `notes` column | Step 1 | Wasted I/O |
| 3 | No broadcast hints on small dimension tables | Steps 3-5 | Unnecessary shuffles |
| 4 | Data skew on `customer_id=1` not handled | Step 3 | Skew |
| 5 | `repartition(500)` on small aggregation result | Step 7, Step 8 detail | Small files |
| 6 | Enriched data computed twice (no caching) | Steps 1-5 repeated for detail | Redundant computation |
| 7 | AQE disabled | Config at top | Missing runtime optimization |

### Fix

Apply all fixes systematically:

```python
# ============================================================
# FIXED ETL PIPELINE
# ============================================================

# Fix 7: Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

dbutils.fs.rm(f"{output_base}/good_summary", recurse=True)
dbutils.fs.rm(f"{output_base}/good_detail", recurse=True)

start_total = time.time()

# Fix 1: Replace UDF with native Spark expression
amount_class_expr = (
    F.when(F.col("amount") > 200, "high")
     .when(F.col("amount") > 50, "medium")
     .otherwise("low")
)

# Fix 2: Select only needed columns (drop "notes")
enriched = (
    transactions
    .select("txn_id", "customer_id", "product_id", "amount", "txn_date", "txn_type")
    .filter(F.col("txn_type") == "purchase")  # Native filter -- pushdown works
    .withColumn("amount_class", amount_class_expr)  # Native expression -- Catalyst optimizes
)

# Fix 3: Broadcast small dimension tables
# Fix 4: AQE handles skew on customer_id automatically (enabled above)
enriched = (
    enriched
    .join(F.broadcast(customers), on="customer_id", how="inner")
    .join(F.broadcast(products), on="product_id", how="inner")
    .join(F.broadcast(regions), on="region_id", how="inner")
)

# Fix 6: Cache the shared enriched DataFrame
enriched.cache()
enriched.count()  # Materialize once

# Summary aggregation
summary = (
    enriched
    .groupBy("region_name", "country", "category", "segment", "amount_class")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
)

# Fix 5: Let AQE coalesce partitions automatically, or use coalesce(1) for small results
(
    summary
    .coalesce(4)  # Small result set, 4 files is sufficient
    .write
    .format("delta")
    .mode("overwrite")
    .save(f"{output_base}/good_summary")
)

# Detail write reuses the cached enriched DataFrame (Fix 6)
(
    enriched
    .write
    .format("delta")
    .option("optimizeWrite", "true")  # Fix 5: auto-size files
    .mode("overwrite")
    .save(f"{output_base}/good_detail")
)

enriched.unpersist()

good_total_time = time.time() - start_total
print(f"\nGOOD ETL total time: {good_total_time:.1f}s")
```

### Verify

#### Compare execution times

```python
print("=" * 60)
print("FINAL COMPARISON")
print("=" * 60)
print(f"BAD ETL:  {bad_total_time:.1f}s")
print(f"GOOD ETL: {good_total_time:.1f}s")
print(f"Speedup:  {bad_total_time / good_total_time:.1f}x")
```

#### Compare file outputs

```python
for label, prefix in [("BAD", "bad"), ("GOOD", "good")]:
    print(f"\n--- {label} ---")
    for subdir in ["summary", "detail"]:
        path = f"{output_base}/{prefix}_{subdir}"
        try:
            files = dbutils.fs.ls(path)
            parquet_files = [f for f in files if f.name.endswith(".parquet")]
            total_size = sum(f.size for f in parquet_files)
            avg_size = total_size / len(parquet_files) if parquet_files else 0
            print(f"  {subdir}: {len(parquet_files)} files, "
                  f"{total_size/(1024*1024):.1f} MB total, "
                  f"{avg_size/(1024*1024):.2f} MB avg")
        except Exception:
            print(f"  {subdir}: not found")
```

#### Verify data correctness

```python
bad_summary = spark.read.format("delta").load(f"{output_base}/bad_summary")
good_summary = spark.read.format("delta").load(f"{output_base}/good_summary")

bad_total = bad_summary.agg(F.sum("total_amount"), F.sum("txn_count")).collect()[0]
good_total = good_summary.agg(F.sum("total_amount"), F.sum("txn_count")).collect()[0]

print(f"\nData validation:")
print(f"  BAD  total_amount: {bad_total[0]:.2f}, txn_count: {bad_total[1]}")
print(f"  GOOD total_amount: {good_total[0]:.2f}, txn_count: {good_total[1]}")
print(f"  Match: {abs(bad_total[0] - good_total[0]) < 0.01 and bad_total[1] == good_total[1]}")
```

#### Expected improvement summary

| Fix | What Changed | Expected Impact |
|---|---|---|
| 1. Replace UDF with native expression | Catalyst can optimize, Tungsten code generation | 2-5x faster on that operation |
| 2. Drop `notes` column | Reduces data scanned, shuffled, and cached | 30-50% less I/O |
| 3. Broadcast dimension tables | Eliminates 3 shuffle exchanges | Significant stage reduction |
| 4. AQE skew join handling | Splits customer_id=1 partition | Eliminates straggler task |
| 5. Right-sized output files | 4 files instead of 500 | Better downstream reads |
| 6. Cache shared computation | Compute enriched once, use twice | ~50% less total compute |
| 7. Enable AQE | Runtime partition coalescing + join optimization | Automatic tuning |

Combined, these fixes typically yield a 3-10x overall speedup depending on cluster size and configuration.

#### Clean up

```python
dbutils.fs.rm(output_base, recurse=True)
spark.catalog.clearCache()

# Restore defaults
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```
