# Spark Internals Foundation

This file covers the core internals that every Spark optimization effort depends on. Understanding these systems turns "guessing at configs" into informed, targeted tuning.

---

## Table of Contents

1. [Catalyst Optimizer](#1-catalyst-optimizer)
2. [Tungsten Execution Engine](#2-tungsten-execution-engine)
3. [Spark Memory Model](#3-spark-memory-model)
4. [Shuffle Architecture](#4-shuffle-architecture)
5. [Adaptive Query Execution (AQE)](#5-adaptive-query-execution-aqe)
6. [Spark Execution Model](#6-spark-execution-model)

---

## 1. Catalyst Optimizer

Catalyst is Spark SQL's extensible query optimizer. Every DataFrame operation and SQL query passes through Catalyst before any computation begins. It transforms a logical description of *what* you want into an optimized physical plan describing *how* to compute it.

### The Four Phases

```
                         Catalyst Optimizer Pipeline
 ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
 │  Analysis    │───▶│  Logical    │───▶│  Physical   │───▶│    Code     │
 │  (Resolve)   │    │ Optimization│    │  Planning   │    │ Generation  │
 └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
   Unresolved         Optimized          Selected           JVM
   Logical Plan       Logical Plan       Physical Plan      Bytecode
```

#### Phase 1: Analysis (Resolution)

The analyzer resolves references against the catalog. Column names are matched to schemas, table names are resolved to data sources, and types are verified.

```python
# This DataFrame expression:
df = spark.table("sales").filter("amount > 100").select("customer_id", "amount")

# Starts as an Unresolved Logical Plan:
#   'Project ['customer_id, 'amount]
#     'Filter ('amount > 100)
#       'UnresolvedRelation [sales]
#
# After Analysis, all references are resolved:
#   Project [customer_id#12, amount#14]
#     Filter (amount#14 > 100)
#       Relation[customer_id#12, name#13, amount#14, date#15] parquet
```

The analyzer applies ~40+ rules iteratively (fixed-point resolution) until the plan stops changing.

#### Phase 2: Logical Optimization

The optimizer applies rule-based and cost-based transformations to the resolved plan. This is where the heavy lifting happens.

```python
# Before optimization:
#   Project [customer_id, amount]
#     Filter (amount > 100 AND 1 = 1)
#       Join (sales.id = returns.sale_id)
#         Scan sales [id, customer_id, name, amount, date, region]
#         Scan returns [sale_id, reason]
#
# After optimization:
#   Project [customer_id, amount]
#     Join (id = sale_id)
#       Filter (amount > 100)          <-- predicate pushed below join
#         Scan sales [id, customer_id, amount]  <-- column pruning: name, date, region dropped
#       Scan returns [sale_id]                   <-- column pruning: reason dropped
#                                       <-- constant folding: "1 = 1" eliminated
```

#### Phase 3: Physical Planning

Catalyst generates one or more physical plans from the optimized logical plan, then selects the best one using a cost model. This is where abstract operations become concrete strategies:

| Logical Operation | Physical Candidates |
|---|---|
| Join | BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin, BroadcastNestedLoopJoin, CartesianProduct |
| Aggregate | HashAggregate, SortAggregate, ObjectHashAggregate |
| Scan | FileSourceScan (Parquet, ORC, CSV), DataSourceV2Scan |

```python
# View the physical plan with cost information:
df.explain("cost")

# View all candidate physical plans:
df.explain("extended")
```

#### Phase 4: Code Generation (Codegen)

Catalyst generates Java bytecode at runtime rather than interpreting the plan tree node by node. This eliminates virtual function dispatch and enables the JVM JIT compiler to optimize the generated code aggressively.

```python
# See the generated code (advanced debugging):
df.explain("codegen")
```

### Key Optimization Rules

#### Predicate Pushdown

Pushes filter conditions as close to the data source as possible, reducing the amount of data read from disk.

```python
# Spark pushes the filter into the Parquet scan:
df = spark.read.parquet("/data/sales").filter(F.col("year") == 2025)

# In the plan you'll see:
#   FileScan parquet [columns...] PushedFilters: [IsNotNull(year), EqualTo(year, 2025)]
```

```sql
-- Same in SQL. Catalyst pushes WHERE into the scan:
SELECT customer_id, amount
FROM sales
WHERE year = 2025 AND region = 'US'
-- PushedFilters: [EqualTo(year, 2025), EqualTo(region, US)]
```

Predicate pushdown also works *through* joins when the predicate applies to one side:

```
Before:                          After:
  Filter (a.year = 2025)           Join
    Join                             Filter (year = 2025)
      Scan a                           Scan a       <-- reads far less data
      Scan b                         Scan b
```

#### Constant Folding

Evaluates constant expressions at plan time instead of once per row.

```python
# Before: every row computes 60 * 60 * 24
df.filter(F.col("duration_seconds") > 60 * 60 * 24)

# After constant folding: comparison uses pre-computed 86400
# Filter (duration_seconds > 86400)
```

#### Column Pruning

Removes columns from scans and intermediate operations when they are not needed downstream. Critical for columnar formats like Parquet and Delta where pruning means entire column chunks are never read from disk.

```python
# Only 2 columns are needed -- Catalyst prunes the rest at the scan:
spark.table("events").select("user_id", "event_type").show()
# FileScan parquet [user_id, event_type]   <-- other columns never read
```

#### Join Reordering

When Cost-Based Optimization (CBO) is enabled and table statistics are available, Catalyst can reorder multi-way joins to minimize intermediate result sizes.

```python
# Enable CBO:
spark.conf.set("spark.sql.cbo.enabled", True)
spark.conf.set("spark.sql.cbo.joinReorder.enabled", True)

# Collect statistics so CBO has data to work with:
spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE customers COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE products COMPUTE STATISTICS FOR ALL COLUMNS")
```

```sql
-- Without CBO, joins execute left-to-right as written.
-- With CBO, Catalyst may reorder to join smallest tables first:
SELECT *
FROM sales s
  JOIN customers c ON s.cust_id = c.id       -- 100M rows x 1M rows
  JOIN products p ON s.product_id = p.id      -- result x 50K rows
-- CBO might reorder to: customers JOIN products (smaller intermediate), then JOIN sales
```

### Why UDFs Block Catalyst

UDFs (User-Defined Functions) are opaque to the optimizer. Catalyst cannot inspect, reorder, or push down UDF logic.

```python
# This UDF creates an optimization barrier:
@udf(returnType=StringType())
def normalize_name(name):
    return name.strip().lower()

df = df.withColumn("clean_name", normalize_name(F.col("name")))

# Catalyst CANNOT:
#  - Push a filter on "clean_name" into the data source
#  - Fold constants inside the UDF
#  - Eliminate the UDF even if the column is later dropped
#  - Generate whole-stage code through the UDF
```

**Alternatives that preserve optimization:**

```python
# Instead of a UDF, use built-in functions:
df = df.withColumn("clean_name", F.lower(F.trim(F.col("name"))))
# Now Catalyst can optimize freely -- these are known expressions.

# For complex logic, consider Pandas UDFs (vectorized):
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def normalize_name_vec(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower()
# Vectorized: processes batches via Arrow, much faster than row-at-a-time UDFs.
# Still opaque to Catalyst, but at least avoids per-row serialization overhead.
```

> **Key Takeaway:** Every built-in Spark SQL function you use instead of a UDF is an optimization the Catalyst optimizer can reason about, reorder, and push down. Replace UDFs with built-in functions whenever possible.

> **Photon:** Photon, the Databricks native vectorized engine, replaces Catalyst's code generation with its own C++ execution engine for supported operations. Photon can execute many built-in functions faster than JVM codegen, but UDFs still fall back to the JVM path. Photon has its own internal optimizer that works alongside Catalyst's logical optimization. See [4-databricks-diagnostics.md](4-databricks-diagnostics.md) for Photon-specific monitoring.

---

## 2. Tungsten Execution Engine

Tungsten is Spark's execution backend focused on CPU and memory efficiency. Introduced in Spark 1.4+ and significantly enhanced in Spark 2.0+, it addresses three bottlenecks: memory management, cache utilization, and code generation.

### Whole-Stage Code Generation (WholeStageCodegen)

Instead of interpreting the physical plan as a tree of iterator objects (the Volcano model), Tungsten fuses multiple operators into a single optimized function -- collapsing an entire pipeline stage into one tight loop.

```
Traditional Volcano Model:              Whole-Stage Codegen:

  ┌──────────┐                           ┌──────────────────────┐
  │  Project  │  ← virtual call          │  Generated Function  │
  └────┬─────┘                           │                      │
  ┌────┴─────┐                           │  for each row:       │
  │  Filter   │  ← virtual call          │    if (amount > 100) │
  └────┬─────┘                           │      emit(id, amt)   │
  ┌────┴─────┐                           │                      │
  │   Scan    │  ← virtual call          └──────────────────────┘
  └──────────┘                            One function, no virtual
  Per-row overhead × 3 operators          dispatch, JIT-friendly
```

#### Spotting WholeStageCodegen in Query Plans

```python
df.explain("formatted")
# Look for the asterisk (*) prefix on operators:
#
#  *(1) Project [customer_id#12, amount#14]
#  +- *(1) Filter (amount#14 > 100)
#     +- *(1) ColumnarToRow
#        +- FileScan parquet [customer_id#12, amount#14]
#
#  The *(1) means these operators are fused into WholeStageCodegen stage 1.
#  Operators WITHOUT * are NOT codegen'd (optimization barriers).
```

```python
# You can also check explicitly:
df.explain("extended")
# Shows: WholeStageCodegen (1)
#          Project [...]
#          Filter [...]
#          ColumnarToRow
#          Scan [...]
```

When you see operators **outside** a WholeStageCodegen boundary, it means something broke the codegen pipeline. Common codegen breakers:

| Codegen Breaker | Why |
|---|---|
| Python UDFs | Require serialization to Python process |
| Complex expressions exceeding 64KB bytecode | JVM method size limit |
| Some aggregate functions with complex state | Cannot be fused efficiently |
| `SortMergeJoin` (the join itself) | Breaks the pipeline; children can still be codegen'd |
| External data source scans | V1 sources may not support columnar reads |

#### Disabling Codegen (Debugging Only)

```python
# If you suspect codegen is producing incorrect results (very rare):
spark.conf.set("spark.sql.codegen.wholeStage", False)
# NEVER do this in production -- it dramatically reduces performance.
```

### Binary Memory Management

Tungsten manages memory directly using `sun.misc.Unsafe` instead of relying on JVM objects and garbage collection. Data is stored in a compact binary format in contiguous memory regions.

```
JVM Object Layout (wasteful):              Tungsten Binary Layout (compact):

┌──────────────────────┐                  ┌─────────────────────────────┐
│ Object Header (16B)  │                  │ null bitmap (1 bit/field)   │
│ Field 1 pointer ─────┼──▶ String obj    │ field1_value (4B int)       │
│ Field 2 (int, 4B)    │    (40B+)        │ field2_offset (4B)          │
│ Padding (4B)         │                  │ field3_value (8B long)      │
│ Field 3 (long, 8B)   │                  │ field1_string_data (varlen) │
└──────────────────────┘                  └─────────────────────────────┘
~80+ bytes per row                        ~25 bytes per row
Plus GC pressure                          No GC pressure, cache-friendly
```

Benefits:
- **No GC overhead**: Binary data is in off-heap or managed regions, invisible to the garbage collector
- **Cache-friendly**: Contiguous memory layout means CPU cache lines are used efficiently
- **Compact**: No object headers, no padding, no pointers -- just data
- **Efficient comparison**: Binary comparison (e.g., for sorting) without deserialization

### Cache-Friendly Data Structures

Tungsten uses data structures designed for CPU cache efficiency:

- **UnsafeRow**: Fixed-width binary format for rows, stored contiguously
- **UnsafeArrayData**: Compact array representation
- **BytesToBytesMap**: Hash map using open addressing with contiguous memory (used for aggregations and hash joins)
- **UnsafeExternalSorter**: Sorts pointers to binary data, not the data itself, minimizing memory movement

### When Tungsten Cannot Help

| Scenario | Why Tungsten Is Bypassed |
|---|---|
| Python UDFs | Data must be serialized to Arrow/Pickle, sent to Python, results deserialized |
| RDD operations (`.rdd`, `.mapPartitions` with non-Row types) | Falls back to JVM object-based execution |
| Complex nested types with UDTs | Custom types may not have binary representations |
| Extremely wide schemas (hundreds of columns) | Binary row format overhead grows; codegen may hit bytecode limits |

```python
# This kills Tungsten benefits -- data round-trips through Python:
@udf(returnType=DoubleType())
def slow_calculation(x):
    return x * 1.1

# This stays in Tungsten -- uses built-in expression:
df.withColumn("result", F.col("x") * 1.1)
```

> **Key Takeaway:** The asterisk `*` before operators in `explain()` output is your friend. If operators are inside `WholeStageCodegen`, Tungsten is working for you. If they fall outside, investigate why and try to restructure your query.

> **Photon:** Photon replaces Tungsten's JVM-based code generation with a native C++ vectorized engine. Where Tungsten processes one row at a time through generated code, Photon processes batches of column vectors using SIMD instructions. Photon typically provides 2-8x speedup over Tungsten for supported operations (scans, filters, aggregations, hash joins). In query plans on Photon-enabled clusters, you will see `PhotonGroupingAgg`, `PhotonShuffleExchangeSink`, and similar Photon-prefixed operators instead of their Tungsten equivalents. See [2-reading-query-plans.md](2-reading-query-plans.md) for identifying Photon vs Tungsten operators in plans.

---

## 3. Spark Memory Model

Understanding Spark's memory model is essential for diagnosing OOM errors, spill, and GC pressure. See [7-memory-and-spill.md](7-memory-and-spill.md) for hands-on tuning.

### The Three Memory Regions

Each executor JVM's heap is divided into three regions:

```
 ┌──────────────────────────────────────────────────────────────┐
 │                    Executor JVM Heap                         │
 │                                                              │
 │  ┌──────────┐  ┌─────────────────────────────────────────┐  │
 │  │ Reserved  │  │           User Memory (40%)             │  │
 │  │  (300MB)  │  │  UDFs, internal metadata, RDD structs,  │  │
 │  │          │  │  user data structures                    │  │
 │  └──────────┘  └─────────────────────────────────────────┘  │
 │                                                              │
 │  ┌───────────────────────────────────────────────────────┐  │
 │  │              Spark Memory (60%)                        │  │
 │  │    spark.memory.fraction = 0.6 (default)              │  │
 │  │                                                        │  │
 │  │  ┌─────────────────────┐  ┌────────────────────────┐  │  │
 │  │  │  Execution Memory   │  │   Storage Memory       │  │  │
 │  │  │  (50% of Spark)     │  │   (50% of Spark)       │  │  │
 │  │  │                     │◀─┤                        │  │  │
 │  │  │  Shuffles, joins,   │  │  Cached DataFrames,    │  │  │
 │  │  │  sorts, aggregates  │──▶│  broadcast variables   │  │  │
 │  │  │                     │  │                        │  │  │
 │  │  └─────────────────────┘  └────────────────────────┘  │  │
 │  │       ▲ Dynamic boundary -- can borrow from each other │  │
 │  └───────────────────────────────────────────────────────┘  │
 └──────────────────────────────────────────────────────────────┘
```

#### Reserved Memory (300 MB fixed)

- Hardcoded at 300 MB
- Used by Spark's internal objects and system overhead
- This is why executor memory below ~1 GB is problematic
- Spark will refuse to start if executor memory < 1.5x reserved (450 MB)

#### User Memory

```
User Memory = (Executor Heap - 300MB) * (1 - spark.memory.fraction)
            = (Executor Heap - 300MB) * 0.4    (with default fraction)
```

Used for:
- Your UDF data structures and variables
- Internal metadata for RDD operations
- Spark's internal bookkeeping that doesn't fit the unified model

This is **not** managed by Spark -- if you exceed it, you get an OOM with no graceful spill.

#### Spark Memory (Unified Memory Manager)

```
Spark Memory = (Executor Heap - 300MB) * spark.memory.fraction
             = (Executor Heap - 300MB) * 0.6    (with default fraction)
```

Split into two pools with a **dynamic boundary**:

### Execution Memory vs Storage Memory

| Property | Execution Memory | Storage Memory |
|---|---|---|
| **Purpose** | Shuffles, joins, sorts, aggregations | Cached DataFrames, broadcast variables |
| **Initial share** | 50% of Spark Memory | 50% of Spark Memory |
| **Can borrow from other?** | Yes -- can take from Storage | Yes -- can take from Execution |
| **Must return borrowed?** | No -- execution has priority | Yes -- evicts cached blocks if execution needs space |
| **On exhaustion** | Spills to disk | Evicts cached blocks (LRU) |

```
Dynamic Occupancy Example (4 GB Spark Memory):

Time 0: No work                 Time 1: Heavy shuffle           Time 2: Large cache
┌──────────┬──────────┐        ┌────────────────┬──────┐        ┌──────┬────────────────┐
│  Exec    │ Storage  │        │   Execution    │Stor. │        │Exec. │   Storage       │
│  2 GB    │  2 GB    │        │    3 GB        │ 1 GB │        │ 1 GB │    3 GB         │
│ (empty)  │ (empty)  │        │ (shuffle data) │      │        │      │ (cached tables) │
└──────────┴──────────┘        └────────────────┴──────┘        └──────┴────────────────┘
                                Execution borrowed               Storage borrowed
                                from Storage                     from Execution
```

**The critical asymmetry**: Execution can *force-evict* storage to reclaim memory. Storage cannot evict execution. This means heavy caching will never cause shuffle OOMs, but heavy shuffles can evict your cached data.

### Key Configuration Parameters

```python
# Default: 0.6 -- fraction of (heap - 300MB) allocated to Spark Memory
spark.conf.set("spark.memory.fraction", "0.6")

# Default: 0.5 -- initial fraction of Spark Memory for Storage
spark.conf.set("spark.memory.storageFraction", "0.5")

# Example calculation for 10 GB executor:
# Reserved:       300 MB
# Usable:         10240 - 300 = 9940 MB
# Spark Memory:   9940 * 0.6 = 5964 MB
# User Memory:    9940 * 0.4 = 3976 MB
# Exec (initial): 5964 * 0.5 = 2982 MB
# Storage (init): 5964 * 0.5 = 2982 MB
```

### Off-Heap Memory

Spark can allocate memory outside the JVM heap, avoiding GC entirely:

```python
spark.conf.set("spark.memory.offHeap.enabled", True)
spark.conf.set("spark.memory.offHeap.size", "4g")  # Must be set explicitly

# Off-heap memory is used for:
# - Tungsten's binary data storage
# - Shuffle data buffers
# - Some cached data

# It is NOT subject to GC, but IS subject to the unified memory model.
# Total Spark Memory = on-heap Spark Memory + off-heap size
```

### Spill: When and Why

Spill occurs when an operation's execution memory needs exceed available memory. Spark writes intermediate data to disk and reads it back, trading CPU/IO for memory.

```
Normal execution:                     Spill scenario:
┌─────────────────┐                  ┌─────────────────┐
│ Execution Memory│                  │ Execution Memory│
│ ┌─────────────┐ │                  │ ┌─────────────┐ │
│ │ Sort buffer │ │                  │ │ Sort buffer │ │ ◀── FULL
│ │  (fits)     │ │                  │ │  (full)     │ │
│ └─────────────┘ │                  │ └──────┬──────┘ │
│                 │                  │        │ spill  │
└─────────────────┘                  └────────┼────────┘
                                              ▼
                                     ┌─────────────────┐
                                     │  Local Disk      │
                                     │  (spill files)   │
                                     │  Slow! 10-100x   │
                                     └─────────────────┘
```

Operations that spill:
- **Sorts**: `ORDER BY`, sort-merge join
- **Shuffles**: Shuffle map output buffers
- **Aggregations**: Hash aggregate overflow
- **Joins**: Hash join build side too large

```python
# Detect spill in Spark UI -- look at the Stages tab for:
# "Shuffle Spill (Memory)" vs "Shuffle Spill (Disk)"
# A large gap between these indicates significant serialization overhead.

# The ratio tells you how compressible the spilled data is:
# Spill (Memory): 10 GB    <-- size in memory before spill
# Spill (Disk):    2 GB    <-- size on disk after serialization + compression
# Ratio: 5:1 compression -- typical for repetitive data
```

> **Key Takeaway:** Spill is Spark's safety valve, not a failure. But heavy spill (> 2x the executor memory) signals that you need more memory per executor or fewer partitions per task. See [7-memory-and-spill.md](7-memory-and-spill.md) for spill remediation.

> **Photon:** Photon uses its own off-heap memory manager written in C++ and is not subject to the JVM's garbage collection. Photon's memory management is separate from the Spark unified memory model. On Databricks, Photon's memory is configured via `spark.databricks.photon.memoryOverheadFactor`, and Photon can spill to local SSD with significantly less overhead than JVM-based spill due to its columnar format.

---

## 4. Shuffle Architecture

Shuffle is the most expensive operation in Spark. Understanding its mechanics is key to optimizing any job with joins, aggregations, or repartitioning. See [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for partition strategy and tuning.

### What Triggers a Shuffle

A shuffle occurs at every **wide dependency** -- where output partitions depend on data from *multiple* input partitions.

| Wide Transformation (Shuffle) | Narrow Transformation (No Shuffle) |
|---|---|
| `groupBy().agg()` | `filter()`, `where()` |
| `join()` (most types) | `select()`, `withColumn()` |
| `repartition()` | `map()`, `flatMap()` |
| `distinct()` | `union()` (without dedup) |
| `orderBy()` / `sort()` | `coalesce()` (decrease only) |
| `window()` with `partitionBy` | `limit()` (partial) |

### The Shuffle Process

```
  Stage 1 (Map Side)                          Stage 2 (Reduce Side)
  ──────────────────                          ────────────────────

  ┌─────────────┐        Shuffle Files         ┌─────────────┐
  │  Task 0     │──┐                       ┌──▶│  Task 0     │
  │  Partition 0│  │   ┌──────────────┐    │   │  Partition 0│
  └─────────────┘  ├──▶│ Shuffle File │────┤   └─────────────┘
                   │   │  (sorted by  │    │
  ┌─────────────┐  │   │  partition   │    │   ┌─────────────┐
  │  Task 1     │──┤   │  key)        │    ├──▶│  Task 1     │
  │  Partition 1│  │   └──────────────┘    │   │  Partition 1│
  └─────────────┘  │                       │   └─────────────┘
                   │   ┌──────────────┐    │
  ┌─────────────┐  │   │ Index File   │    │   ┌─────────────┐
  │  Task 2     │──┘   │ (.index)     │────┘──▶│  Task 2     │
  │  Partition 2│      └──────────────┘        │  Partition 2│
  └─────────────┘                              └─────────────┘

  Shuffle Write:                              Shuffle Read:
  1. Buffer records in memory                 1. Fetch shuffle blocks from
  2. Sort by partition ID + key                  all map tasks via network
  3. Write single sorted file                 2. Decompress and merge
  4. Write index file (offsets)               3. Process reduce function
```

### Sort-Based Shuffle Manager

Since Spark 1.6, the default (and only) shuffle manager sorts records by partition ID, then writes a single output file per map task plus an index file. This replaced the hash-based shuffle, which created one file per map task per reduce partition (O(M*R) files -- disastrous at scale).

```
Sort-Based Shuffle (per map task):

  ┌─────────────────────────────────────────────┐
  │              Shuffle Write Buffer            │
  │                                              │
  │  Records are appended, then sorted by:       │
  │    1. Target partition ID                    │
  │    2. Key (if sort-based aggregation)        │
  │                                              │
  │  If buffer full → spill sorted run to disk   │
  │  Final: merge all runs → one output file     │
  └─────────────────────────────────────────────┘
          │
          ▼
  ┌──────────────┐  ┌──────────────┐
  │  data file   │  │  index file  │
  │  (.data)     │  │  (.index)    │
  │              │  │              │
  │  P0 records  │  │  offset P0   │
  │  P1 records  │  │  offset P1   │
  │  P2 records  │  │  offset P2   │
  │  ...         │  │  ...         │
  └──────────────┘  └──────────────┘
```

### Push-Based Shuffle (Spark 3.2+)

Traditional shuffle requires reduce tasks to fetch blocks from *every* map task individually (all-to-all communication). Push-based shuffle pre-merges blocks on the shuffle service.

```
Traditional Shuffle:                   Push-Based Shuffle:
(N map × M reduce fetches)            (map tasks push, service pre-merges)

Map 0 ──▶ Reduce 0                    Map 0 ──push──▶ ┌────────────┐
Map 0 ──▶ Reduce 1                    Map 1 ──push──▶ │  Shuffle    │──▶ Reduce 0
Map 1 ──▶ Reduce 0                    Map 2 ──push──▶ │  Service    │──▶ Reduce 1
Map 1 ──▶ Reduce 1                                    │ (pre-merge) │──▶ Reduce 2
Map 2 ──▶ Reduce 0                                    └────────────┘
Map 2 ──▶ Reduce 1
                                       Fewer, larger fetches = less network overhead
6 connections                          3 pushes + 3 fetches = less connection overhead
```

```python
# Enable push-based shuffle (Spark 3.2+):
spark.conf.set("spark.shuffle.push.enabled", True)
spark.conf.set("spark.shuffle.push.maxBlockSizeToPush", "1m")  # default

# Requirements:
# - External shuffle service must support push-merge
# - On Databricks, this is handled automatically with enhanced shuffle
```

### Shuffle File Lifecycle

Shuffle files persist on local disk until:
1. The consuming stage completes successfully
2. The RDD/DataFrame is garbage collected
3. The executor is decommissioned (files migrated or recomputed)
4. Cleanup timeout (`spark.cleaner.referenceTracking.cleanCheckpoints`)

If shuffle files are lost (executor crash, node loss), Spark must **recompute** the parent stage. This is why long shuffle-heavy pipelines benefit from checkpointing.

### Network I/O Implications

```
Shuffle network cost for a join of two 100 GB tables:

  Table A (100 GB) ─── repartition by key ───▶  Up to 100 GB transferred
  Table B (100 GB) ─── repartition by key ───▶  Up to 100 GB transferred
                                                  ────────────────────────
                                                  Total: up to 200 GB
                                                  over the network

  Compare with Broadcast Join (if B fits in memory):
  Table A (100 GB) ─── no shuffle ───────────▶  0 GB shuffled
  Table B (1 GB)   ─── broadcast to all ─────▶  1 GB × N executors
                                                  ────────────────────────
                                                  Total: ~N GB (much less)
```

### Why Shuffle Is the Most Expensive Operation

1. **Disk I/O**: Shuffle data is written to local disk (map side) and read from disk (reduce side)
2. **Network I/O**: All shuffle data transfers between executors over the network
3. **Serialization**: Data is serialized for writing and deserialized on reading
4. **Memory pressure**: Shuffle buffers compete for execution memory; overflow causes spill
5. **Synchronization**: Reduce tasks cannot start until all map tasks in the stage complete (stage boundary)
6. **GC pressure**: Deserialization on the reduce side creates many JVM objects

```python
# Practical: count the shuffles in your plan
df.explain("formatted")
# Each "Exchange" node is a shuffle:
#   Exchange hashpartitioning(key#1, 200)    <-- SHUFFLE
#   Exchange SinglePartition                  <-- SHUFFLE (collect to 1 partition)
#   Exchange RoundRobinPartitioning(100)      <-- SHUFFLE (repartition)
```

> **Key Takeaway:** Every `Exchange` node in your query plan is a shuffle, and each shuffle is a stage boundary. Minimizing shuffles (through broadcast joins, proper pre-partitioning, and avoiding unnecessary repartitioning) is one of the highest-impact optimizations. See [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for strategies.

> **Photon:** Photon accelerates shuffle write and read with its columnar batch format and optimized serialization. On Databricks, the shuffle service can leverage Photon's encoding for faster transfers. Combined with local SSD caching, Photon shuffle can be 2-3x faster than standard Spark shuffle for supported operations.

---

## 5. Adaptive Query Execution (AQE)

AQE (introduced in Spark 3.0, enabled by default since Spark 3.2 and all Databricks runtimes) re-optimizes query plans at runtime using actual statistics collected during execution, rather than relying solely on pre-execution estimates.

### How AQE Works

```
Traditional Optimization:                AQE:

  Compile-time stats ──▶ Fixed plan      Compile-time stats ──▶ Initial plan
         │                                       │
         ▼                                       ▼
  Execute entire plan                    Execute Stage 1
         │                                       │
         ▼                                       ├──▶ Collect runtime stats
  Done                                           │    (partition sizes, row counts)
                                                 ▼
                                         Re-optimize remaining plan
                                                 │
                                                 ▼
                                         Execute Stage 2
                                                 │
                                                 ├──▶ Collect runtime stats
                                                 ▼
                                         Re-optimize again...
                                                 │
                                                 ▼
                                         Done
```

AQE can only re-optimize at **stage boundaries** (shuffle points), because that's when Spark materializes intermediate results and can measure their actual sizes.

### The Three Main Features

#### Feature 1: Coalescing Post-Shuffle Partitions

After a shuffle, some partitions may be tiny (a few KB) while others are large. AQE merges small adjacent partitions into larger ones to reduce task scheduling overhead and improve efficiency.

```
Before AQE coalescing (200 shuffle partitions):

  Part 0: 2 MB   Part 3: 100 KB   Part 6: 50 KB    ...  Part 199: 1 MB
  Part 1: 1 MB   Part 4: 200 KB   Part 7: 5 MB
  Part 2: 80 KB  Part 5: 3 MB     Part 8: 90 KB

  Many tiny partitions → many tiny tasks → scheduling overhead dominates

After AQE coalescing (e.g., target 64 MB):

  Coalesced 0: [Part 0 + 1 + 2 + 3 + 4] = 3.4 MB    (still < 64 MB target)
  Coalesced 1: [Part 5] = 3 MB
  Coalesced 2: [Part 6 + 7 + 8] = 5.1 MB
  ...
  Result: far fewer tasks, each doing meaningful work
```

```python
# Key AQE coalescing configs:
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)       # default: true
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")  # default: 1MB
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")    # target size

# With AQE, you can set shuffle.partitions high and let AQE coalesce:
spark.conf.set("spark.sql.shuffle.partitions", 2000)
# AQE will automatically merge the 2000 down to whatever makes sense.
```

#### Feature 2: Converting Sort-Merge Joins to Broadcast Hash Joins

If the runtime size of one side of a join turns out to be small enough (after filters reduce the data), AQE converts an expensive sort-merge join to a broadcast hash join -- eliminating a shuffle on the probe side.

```
Before AQE (estimated 500 MB, planned as SortMergeJoin):

  Table A (10 GB) ──shuffle──▶ ┌──────────────┐
                               │ SortMerge    │──▶ Result
  Table B (500 MB est)──shuffle──▶│ Join         │
                               └──────────────┘
  2 shuffles, expensive sort on both sides

After AQE (actual 8 MB after filters, converts to BroadcastHashJoin):

  Table A (10 GB) ─────────────▶ ┌──────────────┐
                                 │ Broadcast    │──▶ Result
  Table B (8 MB actual)──broadcast──▶│ HashJoin     │
                                 └──────────────┘
  0 shuffles on A, no sort, much faster
```

```python
# AQE broadcast threshold (separate from the static one):
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "30m")  # default = same as static

# The static config (used at compile time):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")  # default: 10 MB

# Tip: You can set the static threshold low and the AQE threshold higher,
# letting AQE make the broadcast decision with better information.
```

#### Feature 3: Optimizing Skew Joins

AQE detects partitions that are significantly larger than the median and automatically splits them, replicating the small side to join with each sub-partition.

```
Before AQE skew optimization:

  Partition 0: 100 MB  ──▶ Task 0: 5 sec
  Partition 1: 100 MB  ──▶ Task 1: 5 sec
  Partition 2: 10 GB   ──▶ Task 2: 500 sec   ← SKEW! This task runs 100x longer
  Partition 3: 100 MB  ──▶ Task 3: 5 sec

  Total wall time ≈ 500 sec (limited by the slowest task)

After AQE skew optimization:

  Partition 0: 100 MB   ──▶ Task 0: 5 sec
  Partition 1: 100 MB   ──▶ Task 1: 5 sec
  Partition 2a: 200 MB  ──▶ Task 2a: 10 sec  ← Split into ~50 sub-partitions
  Partition 2b: 200 MB  ──▶ Task 2b: 10 sec     Small side replicated to each
  ...                                            sub-task
  Partition 2z: 200 MB  ──▶ Task 2z: 10 sec
  Partition 3: 100 MB   ──▶ Task 3: 5 sec

  Total wall time ≈ 10 sec (massive improvement)
```

```python
# AQE skew join configs:
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)  # default: true

# A partition is considered skewed if BOTH conditions are met:
# 1. Size > skewedPartitionFactor × median partition size
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)  # default: 5

# 2. Size > skewedPartitionThresholdInBytes
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")  # default: 256 MB
```

### Complete AQE Configuration Reference

```python
# Master switch (default true in Databricks and Spark 3.2+):
spark.conf.set("spark.sql.adaptive.enabled", True)

# Force repartition to use AQE advisory size:
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", False)
# Default: true. When true, AQE prioritizes parallelism (more partitions).
# Set to false to prioritize target partition size.

# All AQE configs at a glance:
for k, v in spark.sparkContext.getConf().getAll():
    if "adaptive" in k:
        print(f"{k} = {v}")
```

> **Key Takeaway:** With AQE enabled, set `spark.sql.shuffle.partitions` to a high value (e.g., `auto` on Databricks, or 2000+) and let AQE coalesce dynamically. This is almost always better than trying to manually pick the "right" partition count. See [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) for advanced partitioning with AQE.

> **Photon:** Photon works synergistically with AQE. Photon's faster execution makes AQE's runtime statistics available sooner, and AQE's dynamic optimizations complement Photon's vectorized execution. On Databricks, Photon-accelerated shuffles produce the same runtime statistics that AQE uses for re-optimization.

---

## 6. Spark Execution Model

Understanding how Spark decomposes your code into jobs, stages, and tasks is fundamental to reading the Spark UI and diagnosing performance bottlenecks. See [3-spark-ui-guide.md](3-spark-ui-guide.md) for navigating these concepts in the UI.

### The Execution Hierarchy

```
Application
  └── Job 1  (triggered by an action: .count(), .show(), .write(), etc.)
  │     └── Stage 0  (reads from source, no shuffle needed)
  │     │     └── Task 0  (processes partition 0)
  │     │     └── Task 1  (processes partition 1)
  │     │     └── Task 2  (processes partition 2)
  │     │     └── ...
  │     └── Stage 1  (after shuffle boundary)
  │           └── Task 0  (processes shuffle partition 0)
  │           └── Task 1  (processes shuffle partition 1)
  │           └── ...
  └── Job 2  (next action)
        └── Stage 2  (may reuse Stage 0's output if cached)
              └── ...
```

**Key distinctions:**
- **Job**: Created by each *action* (anything that triggers computation)
- **Stage**: Bounded by shuffle dependencies; all transformations within a stage execute without data exchange
- **Task**: One unit of work processing one partition in one stage; the smallest unit of parallelism

### How DAG Scheduling Works

The DAGScheduler converts the logical execution graph into a physical execution plan of stages:

```python
# Example pipeline:
result = (
    spark.read.parquet("/data/sales")           # ──┐
        .filter(F.col("year") == 2025)          #   ├── Stage 0 (narrow)
        .withColumn("tax", F.col("amount")*0.1) # ──┘
        .groupBy("region")                      #      ← shuffle boundary
        .agg(F.sum("amount"), F.count("*"))     # ──┐
        .orderBy(F.desc("sum(amount)"))         #   ├── Stage 1 → Stage 2
        .show()                                 # ──┘   (two shuffles: groupBy, orderBy)
)
```

```
DAG for the above pipeline:

  ┌──────────────────────────────────────────┐
  │ Stage 0 (Narrow transformations)         │
  │                                          │
  │  FileScan ──▶ Filter ──▶ Project         │
  │  (parquet)    (year=2025)  (+tax col)    │
  └──────────────────┬───────────────────────┘
                     │ Exchange (hash partition by region)
                     ▼
  ┌──────────────────────────────────────────┐
  │ Stage 1 (Partial + Final Aggregate)      │
  │                                          │
  │  ShuffleRead ──▶ HashAggregate           │
  │                   (sum, count by region) │
  └──────────────────┬───────────────────────┘
                     │ Exchange (range partition for sort)
                     ▼
  ┌──────────────────────────────────────────┐
  │ Stage 2 (Sort + Collect)                 │
  │                                          │
  │  ShuffleRead ──▶ Sort ──▶ Limit ──▶ Show│
  └──────────────────────────────────────────┘
```

### Narrow vs Wide Dependencies

This distinction is the foundation of stage boundaries:

```
Narrow Dependency:                      Wide Dependency:
Each output partition depends on        Each output partition depends on
ONE input partition.                    MULTIPLE input partitions.

Input:  [A] [B] [C] [D]               Input:  [A] [B] [C] [D]
         │   │   │   │                          │╲  │╲  │╲  │╲
         │   │   │   │                          │ ╲ │ ╲ │ ╲ │ ╲
         ▼   ▼   ▼   ▼                         ▼  ▼ ▼  ▼ ▼  ▼
Output: [A'] [B'] [C'] [D']           Output: [W] [X] [Y] [Z]

Examples:                               Examples:
  map, filter, select,                    groupBy, join (most),
  withColumn, union,                      repartition, distinct,
  coalesce (decrease)                     orderBy, window

Can be PIPELINED in one stage.          Requires SHUFFLE → new stage.
```

### Pipeline Boundaries

Within a stage, operations are pipelined -- data flows through all transformations without materializing intermediate results. This is extremely efficient:

```python
# All of these execute in a single pass over each partition (one stage):
df = (spark.read.parquet("/data/events")
    .filter(F.col("type") == "click")          # Pipelined
    .withColumn("hour", F.hour("timestamp"))    # Pipelined
    .select("user_id", "hour", "page")          # Pipelined
    .filter(F.col("hour").between(9, 17))       # Pipelined
)
# No intermediate data is materialized between these operations.
# Tungsten/WholeStageCodegen fuses them into a single generated function.
```

Pipeline boundaries are created by:
- **Shuffles** (wide dependencies)
- **Cache/persist** (materialization point)
- **Certain physical operators**: e.g., sort-merge join requires sorting both sides

### Task Scheduling and Data Locality

Each task processes one partition. The scheduler assigns tasks to executors considering data locality:

| Locality Level | Description | Performance |
|---|---|---|
| `PROCESS_LOCAL` | Data is in the same executor's memory (cached) | Fastest |
| `NODE_LOCAL` | Data is on the same node's disk (HDFS/local) | Fast |
| `RACK_LOCAL` | Data is on a node in the same rack | Moderate |
| `ANY` | Data is on a remote node | Slowest |

```python
# Control how long the scheduler waits for better locality:
spark.conf.set("spark.locality.wait", "3s")          # default: 3s
spark.conf.set("spark.locality.wait.node", "3s")     # wait for NODE_LOCAL
spark.conf.set("spark.locality.wait.process", "3s")  # wait for PROCESS_LOCAL
spark.conf.set("spark.locality.wait.rack", "3s")     # wait for RACK_LOCAL

# In cloud environments (Databricks, EMR), data is typically in object storage
# (S3, ADLS, GCS), so locality is less relevant for initial reads.
# However, PROCESS_LOCAL matters a lot for cached data.
```

### Parallelism: How Many Tasks Run

```
Total task slots = num_executors × cores_per_executor

Example: 10 executors × 4 cores = 40 task slots
         400 partitions → 10 "waves" of 40 tasks each

  Wave 1:  Tasks  0-39  ──▶ execute in parallel
  Wave 2:  Tasks 40-79  ──▶ execute in parallel
  ...
  Wave 10: Tasks 360-399 ──▶ execute in parallel

Ideal: 2-3 waves per stage (enough parallelism, not too much overhead).
Too few partitions: underutilizes the cluster.
Too many partitions: excessive scheduling overhead, small tasks.
```

```python
# Check current parallelism:
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"DataFrame partitions: {df.rdd.getNumPartitions()}")
```

> **Key Takeaway:** Actions create jobs. Shuffles create stage boundaries. Stages contain pipelined narrow transformations. Tasks process individual partitions. Understanding this hierarchy is the key to reading the Spark UI effectively. See [3-spark-ui-guide.md](3-spark-ui-guide.md) for connecting these concepts to what you see in the UI.

> **Photon:** On Photon-enabled clusters, task execution within each stage is handled by Photon's C++ engine for supported operators. Photon tasks may show different metrics in the Spark UI (e.g., "Photon time" instead of JVM execution time). The stage/task structure remains the same -- Photon replaces the execution engine within tasks, not the scheduling model. See [4-databricks-diagnostics.md](4-databricks-diagnostics.md) for interpreting Photon task metrics.

---

## Summary: How Everything Connects

```
Your Code (DataFrame / SQL)
       │
       ▼
┌─────────────────────┐
│  Catalyst Optimizer  │  Phases: Analyze → Optimize → Plan → Codegen
│  (Logical → Physical)│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Tungsten Engine     │  WholeStageCodegen, binary memory, UnsafeRow
│  (Execution)         │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Memory Manager      │  Execution vs Storage memory, spill to disk
│  (Unified Model)     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Shuffle Service     │  Sort-based, network I/O, stage boundaries
│  (Data Exchange)     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  AQE                 │  Runtime re-optimization at stage boundaries
│  (Dynamic Tuning)    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  DAG Scheduler       │  Jobs → Stages → Tasks, locality, parallelism
│  (Execution Model)   │
└─────────────────────┘
```

Each of these systems interacts with the others:
- Catalyst decides *which* shuffles are needed; AQE may change that decision at runtime.
- Tungsten manages memory for shuffles, joins, and aggregations; when it runs out, the Memory Manager triggers spill.
- The DAG Scheduler creates stages at shuffle boundaries; AQE may coalesce or split partitions after each stage.
- Your query plan (from Catalyst) determines which Tungsten code paths run (codegen vs interpreted).

For the next step, see [2-reading-query-plans.md](2-reading-query-plans.md) to learn how to read and interpret the plans that Catalyst produces.
