# 8 — I/O and Storage Optimization

> **Summary:** I/O is the dominant cost in most Spark jobs. Choosing the right file format, pushing filters into the storage layer, pruning columns, controlling file sizes, and leveraging Delta Lake features can turn a 45-minute job into a 3-minute one — often without touching a single line of transformation logic.

---

## Table of Contents

1. [File Format Selection](#1-file-format-selection)
2. [Predicate Pushdown](#2-predicate-pushdown)
3. [Column Pruning](#3-column-pruning)
4. [Small File Problem](#4-small-file-problem)
5. [Delta Lake Optimization](#5-delta-lake-optimization)
6. [Write Optimization](#6-write-optimization)
7. [Caching Strategies](#7-caching-strategies)
8. [Data Layout Optimization](#8-data-layout-optimization)
9. [Read Optimization](#9-read-optimization)
10. [Configuration Reference](#10-configuration-reference)

---

## 1. File Format Selection

### Format Comparison

| Feature | Parquet | Delta Lake | ORC | CSV | JSON |
|---------|---------|------------|-----|-----|------|
| **Type** | Columnar | Columnar (Parquet + txn log) | Columnar | Row-based | Row-based |
| **Schema evolution** | Limited | Full (add, rename, reorder) | Limited | None | None |
| **ACID transactions** | No | Yes | No | No | No |
| **Predicate pushdown** | Yes | Yes + data skipping | Yes | No | No |
| **Column pruning** | Yes | Yes | Yes | No | No |
| **Compression** | Excellent (snappy, zstd, gzip) | Excellent (inherits Parquet) | Excellent (zlib, snappy) | Poor | Poor |
| **Splittable** | Yes | Yes | Yes | Yes (uncompressed) | Yes (uncompressed) |
| **Human readable** | No | No | No | Yes | Yes |
| **Typical compression ratio** | 5–10x vs CSV | 5–10x vs CSV | 5–10x vs CSV | 1x (baseline) | 0.5–0.8x (larger than CSV) |
| **Metadata overhead** | Low | Moderate (txn log) | Low | None | None |

> **Takeaway:** For analytics workloads on Databricks, Delta Lake is the default choice. It inherits Parquet's columnar performance and adds ACID, time travel, and advanced optimization features. Use raw Parquet only for interop with non-Delta systems. CSV/JSON should be limited to data ingestion landing zones.

### Why Columnar Formats Win for Analytics

Most analytic queries touch a subset of columns from wide tables. Columnar formats store data by column, so Spark can skip entire columns it doesn't need.

```
Row-oriented layout (CSV/JSON):          Columnar layout (Parquet/ORC/Delta):
┌────────────────────────────────┐       ┌──────────┬──────────┬──────────┐
│ id, name, age, city, salary    │       │ Column:  │ Column:  │ Column:  │
│ 1, Alice, 30, NYC, 100000     │       │ id       │ name     │ age      │
│ 2, Bob, 25, SF, 95000         │       │ ──────── │ ──────── │ ──────── │
│ 3, Carol, 35, LA, 110000      │       │ 1        │ Alice    │ 30       │
│ ...                            │       │ 2        │ Bob      │ 25       │
│ (must read ALL columns         │       │ 3        │ Carol    │ 35       │
│  even if query uses only 2)    │       │ ...      │ ...      │ ...      │
└────────────────────────────────┘       └──────────┴──────────┴──────────┘
                                          ↑ Read ONLY the columns you need
```

A query like `SELECT AVG(salary) FROM employees WHERE age > 30` on a 100-column table:
- **CSV:** Must read and parse all 100 columns → 100% I/O
- **Parquet:** Reads only `salary` and `age` columns → ~2% I/O

### Parquet Internal Structure

Understanding Parquet internals helps you tune I/O at a granular level.

```
┌──────────────────────────────────────────────────┐
│                  Parquet File                     │
├──────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────┐    │
│  │            Row Group 0                    │    │
│  │  ┌────────────┐ ┌────────────┐           │    │
│  │  │ Column      │ │ Column      │          │    │
│  │  │ Chunk: id   │ │ Chunk: name │ ...      │    │
│  │  │ ┌────────┐  │ │ ┌────────┐  │          │    │
│  │  │ │ Page 0 │  │ │ │ Page 0 │  │          │    │
│  │  │ ├────────┤  │ │ ├────────┤  │          │    │
│  │  │ │ Page 1 │  │ │ │ Page 1 │  │          │    │
│  │  │ └────────┘  │ │ └────────┘  │          │    │
│  │  └────────────┘ └────────────┘           │    │
│  └──────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────┐    │
│  │            Row Group 1                    │    │
│  │  (same structure)                         │    │
│  └──────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────┐    │
│  │  Footer (schema, row group metadata,      │    │
│  │          column stats: min/max/null_count) │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

**Key terms:**

| Component | Description | Typical Size |
|-----------|-------------|--------------|
| **Row Group** | Horizontal slice of rows. Each row group contains column chunks for all columns. | 128 MB (default) |
| **Column Chunk** | All data for one column within one row group. | Varies |
| **Page** | Unit of encoding/compression within a column chunk. | 1 MB (default) |
| **Footer** | File-level metadata: schema, row group offsets, column statistics (min, max, null count). | Small (KB) |

**Why this matters:**
- Row group stats (min/max) enable **row group skipping** — entire row groups can be skipped if the filter falls outside their min/max range.
- Pages can use **dictionary encoding**, enabling page-level dictionary filtering.
- The footer is read first (a single seek), then only relevant row groups and columns are read.

### Delta Lake's Transaction Log

Delta Lake wraps Parquet files with a JSON-based transaction log (`_delta_log/`):

```
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json    ← initial commit
│   ├── 00000000000000000001.json    ← add/remove files
│   ├── 00000000000000000002.json
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet  ← checkpoint every 10 commits
├── part-00000-xxxx.snappy.parquet
├── part-00001-xxxx.snappy.parquet
└── ...
```

**Transaction log overhead:**
- Each read must reconstruct the current table state by reading the log from the last checkpoint forward.
- With frequent micro-commits (e.g., streaming every 10 seconds), the log grows fast → listing and replay slows down.
- **Checkpoints** (every 10 commits by default) compact the log into a single Parquet file for fast state reconstruction.

**Transaction log benefits:**
- ACID guarantees — no partial reads, no dirty data.
- Time travel — query any historical version.
- File-level statistics stored in the log enable **data skipping** without reading Parquet footers.
- Schema enforcement at write time.

> **Photon:** Photon accelerates Parquet decoding with native C++ vectorized readers, achieving 2–5x faster scan throughput compared to the JVM Parquet reader, particularly on wide tables with many string columns.

---

## 2. Predicate Pushdown

Predicate pushdown moves filter evaluation from Spark's execution engine down to the data source, reducing the volume of data read from storage.

### Three Levels of Pushdown

```
Level 0 — No pushdown:
  Read ALL data → Filter in Spark
  Cost: 100% I/O

Level 1 — File/Partition pruning:
  Skip entire FILES or PARTITIONS that cannot match
  Cost: Fraction of I/O (e.g., 1 of 365 daily partitions = 0.3%)

Level 2 — Row group pushdown:
  Read file, but skip ROW GROUPS whose min/max stats exclude the filter
  Cost: Reduced I/O within each file

Level 3 — Page-level pushdown:
  Within a row group, use dictionary encoding to skip PAGES
  Cost: Minimal I/O — only matching pages decoded

┌──────────────────────────────────────────────────────┐
│ Query: WHERE date = '2025-01-15' AND status = 'FAIL' │
└──────────┬───────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│ Level 1: Partition Pruning       │
│ Skip all partitions except       │
│ date=2025-01-15/                 │
│ (364 of 365 partitions skipped)  │
└──────────┬──────────────────────┘
           │
           ▼
┌──────────────────────────────────┐
│ Level 2: Row Group Skipping       │
│ Check min/max of 'status' column  │
│ Row Group 0: min=ACTIVE max=WARN  │
│   → status='FAIL' is in range     │
│   → MUST READ                     │
│ Row Group 1: min=OK max=OK        │
│   → status='FAIL' not in range    │
│   → SKIP                          │
└──────────┬───────────────────────┘
           │
           ▼
┌──────────────────────────────────┐
│ Level 3: Page Dictionary Filter   │
│ Page 0 dictionary: [ACTIVE, FAIL] │
│   → 'FAIL' found → READ page     │
│ Page 1 dictionary: [ACTIVE, WARN] │
│   → 'FAIL' not found → SKIP page │
└──────────────────────────────────┘
```

### Verifying Pushdown in Query Plans

```python
# Check if predicates are pushed down
df = spark.read.format("delta").load("/data/events")
filtered = df.filter("event_type = 'click' AND user_id > 1000")
filtered.explain("formatted")
```

Look for `PushedFilters` in the `FileScan` node:

```
== Physical Plan ==
*(1) Filter (isnotnull(event_type#12) AND (event_type#12 = click))
+- *(1) ColumnarToRow
   +- FileScan parquet [user_id#11,event_type#12,timestamp#13]
        Batched: true
        Format: Parquet
        Location: PreparedDeltaFileIndex[dbfs:/data/events]
        PartitionFilters: []
        PushedFilters: [IsNotNull(event_type), EqualTo(event_type,click),
                         GreaterThan(user_id,1000)]          ← ✅ PUSHED
        ReadSchema: struct<user_id:bigint,event_type:string,timestamp:timestamp>
```

**If pushdown is working:** filters appear in `PushedFilters`.
**If pushdown failed:** filters appear only in the `Filter` node above `FileScan`, not in `PushedFilters`.

### What Blocks Predicate Pushdown

| Blocker | Example | Fix |
|---------|---------|-----|
| **UDFs** | `.filter(my_udf(col("x")) > 5)` | Replace with built-in functions |
| **Complex expressions** | `.filter(F.coalesce(F.col("a"), F.col("b")) == "val")` | Simplify or restructure the filter |
| **Type mismatches** | Filtering string column with integer literal | Ensure matching types |
| **Non-deterministic functions** | `F.rand()`, `F.current_timestamp()` | These cannot be pushed by design |
| **Casts across types** | `.filter(F.col("int_col").cast("string") == "42")` | Filter on original type |
| **OR with non-pushable arm** | `filter("a = 1 OR my_udf(b) = 2")` | Split into two queries if possible |
| **After aggregation/join** | Filters placed after `groupBy` or `join` | Move filters before the aggregation/join |

### Partition Pruning — A Special Case

Partition pruning is file-system-level pushdown: Spark skips entire directories.

```python
# Table partitioned by (year, month, day)
# This query reads only 1 directory instead of thousands:
df = spark.read.format("delta").load("/data/events") \
    .filter("year = 2025 AND month = 1 AND day = 15")
```

In the query plan, partition filters appear separately:

```
PartitionFilters: [isnotnull(year#1), isnotnull(month#2),
                   (year#1 = 2025), (month#2 = 1), (day#3 = 15)]   ← ✅
PushedFilters: [IsNotNull(event_type), EqualTo(event_type,click)]   ← additional
```

> **Takeaway:** Always filter on partition columns first. This is the single highest-leverage I/O optimization — it can eliminate 99%+ of data before any file is opened.

---

## 3. Column Pruning

Column pruning is the columnar format counterpart to predicate pushdown: instead of skipping rows, Spark skips **columns** it doesn't need.

### How It Works

```
Table has 100 columns, query uses 3:

Without column pruning (CSV):
┌──────────────────────────────────────────┐
│ Read ALL 100 columns from every row      │
│ Parse ALL 100 columns from every row     │
│ Project down to 3 columns in memory      │
│ I/O: 100% │ Parse: 100% │ Memory: 100%  │
└──────────────────────────────────────────┘

With column pruning (Parquet/Delta):
┌──────────────────────────────────────────┐
│ Read ONLY 3 column chunks from each      │
│ row group                                │
│ Parse ONLY 3 columns                     │
│ I/O: ~3% │ Parse: ~3% │ Memory: ~3%     │
└──────────────────────────────────────────┘
```

### Verifying Column Pruning in Query Plans

```python
# Suppose the table has columns: id, name, age, city, salary, dept, hire_date, ...
df = spark.read.format("delta").load("/data/employees")
result = df.select("name", "salary").filter("salary > 100000")
result.explain("formatted")
```

Check the `ReadSchema` field:

```
FileScan parquet [name#1,salary#4]
    ReadSchema: struct<name:string,salary:double>     ← ✅ Only 2 columns read
```

If you see all columns in `ReadSchema`, column pruning is not working.

### Why `SELECT *` Is Expensive

```python
# BAD — reads all columns even if downstream only uses 2
df = spark.sql("SELECT * FROM employees")
result = df.select("name", "salary")   # Catalyst MAY optimize this away, but not always

# GOOD — explicit column selection
df = spark.sql("SELECT name, salary FROM employees")
```

**When Catalyst cannot optimize away `SELECT *`:**
- When the DataFrame is cached (cache stores all columns).
- When the DataFrame is used in multiple downstream paths with different column needs (Spark may read the union of all needed columns).
- When the DataFrame crosses a shuffle boundary before column selection.

### Column Pruning vs Predicate Pushdown

| | Column Pruning | Predicate Pushdown |
|--|----------------|-------------------|
| **Reduces** | Columns read | Rows read |
| **Mechanism** | Skip column chunks in Parquet | Skip files, row groups, pages |
| **Requires** | Columnar format | Filter expressions + stats |
| **Plan indicator** | `ReadSchema` | `PushedFilters` / `PartitionFilters` |
| **Works with CSV** | No | No |

> **Takeaway:** Column pruning and predicate pushdown are complementary. The best queries use both: select only the columns you need and filter early. Together, they can reduce I/O by 95–99%.

---

## 4. Small File Problem

### Why Small Files Are Bad

```
1000 files × 1 MB each = 1 GB total

vs.

8 files × 128 MB each = 1 GB total

Same data. Wildly different performance.
```

| Problem | Explanation |
|---------|-------------|
| **Scheduling overhead** | Each file becomes a task. 1,000 tasks × ~100ms scheduling overhead each = significant parallelism startup cost. |
| **Metadata overhead** | Driver must list all files, read all footers, track all task states. With millions of small files, the driver can OOM. |
| **Poor I/O utilization** | Object storage (S3/ADLS/GCS) has high per-request latency (~5–50ms). Many small reads are far slower than fewer large sequential reads. |
| **Delta log bloat** | Each file is tracked in the transaction log. More files = larger log = slower reads. |
| **Poor compression** | Smaller files mean smaller dictionaries and less compressible data within each column chunk. |

### How to Detect Small Files

```python
# Delta tables — check file count and average size
from pyspark.sql import functions as F

detail = spark.sql("DESCRIBE DETAIL my_database.my_table")
detail.select("numFiles", "sizeInBytes").show()

# Calculate average file size
file_stats = (
    spark.sql("DESCRIBE DETAIL my_database.my_table")
    .select(
        "numFiles",
        "sizeInBytes",
        (F.col("sizeInBytes") / F.col("numFiles") / 1024 / 1024).alias("avg_file_size_mb")
    )
)
file_stats.show()
# Target: avg_file_size_mb between 64 MB and 256 MB

# Check file size distribution for a Delta table
file_sizes = spark.sql("""
    SELECT
        ROUND(size / (1024 * 1024), 1) AS size_mb,
        COUNT(*) AS file_count
    FROM (SELECT size FROM delta.`/path/to/table` VERSION AS OF 0)
    GROUP BY 1
    ORDER BY 1
""")
```

```sql
-- Alternatively, inspect the Delta log directly
DESCRIBE DETAIL my_database.my_table;
-- Look at numFiles and sizeInBytes columns
-- avg_size = sizeInBytes / numFiles
-- If avg_size < 32 MB → you have a small file problem
```

### Common Causes

| Cause | How It Happens |
|-------|----------------|
| **Over-partitioning** | `partitionBy("year", "month", "day", "hour", "minute")` — too many partition levels create many tiny directories |
| **Frequent appends** | Hourly/minutely jobs each writing a few MB |
| **Streaming micro-batches** | Structured Streaming with short trigger intervals (e.g., `trigger(processingTime='10 seconds')`) |
| **High shuffle partition count** | `spark.sql.shuffle.partitions = 200` on a 1 GB dataset → 200 × 5 MB files |
| **Overwrite of partitions** | Dynamic partition overwrite replacing one partition at a time |

### Fixes

#### Fix 1: `OPTIMIZE` Command

```sql
-- Compact all small files in a Delta table
OPTIMIZE my_database.my_table;

-- Compact only specific partitions
OPTIMIZE my_database.my_table WHERE date = '2025-01-15';

-- OPTIMIZE with ZORDER (combines compaction with data layout optimization)
OPTIMIZE my_database.my_table ZORDER BY (user_id);
```

`OPTIMIZE` rewrites small files into larger files (target size: 1 GB by default on Databricks). It is an idempotent operation — running it multiple times is safe.

#### Fix 2: Auto Compaction

```python
# Enable auto compaction — automatically runs a mini-OPTIMIZE after each write
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Or set at table level
spark.sql("""
    ALTER TABLE my_database.my_table
    SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
""")
```

Auto Compaction runs asynchronously after writes. It targets files smaller than 128 MB and compacts them. It is less aggressive than a full `OPTIMIZE` but prevents small file accumulation.

#### Fix 3: Optimized Writes

```python
# Enable optimized writes — Spark adaptively coalesces partitions before writing
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Or set at table level
spark.sql("""
    ALTER TABLE my_database.my_table
    SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
```

Optimized Writes adds a shuffle before the write to produce fewer, larger files. This has a small runtime cost (the extra shuffle) but prevents small files at the source.

#### Fix 4: coalesce / repartition Before Write

```python
# Reduce partition count before writing (no shuffle)
df.coalesce(8).write.format("delta").mode("append").save("/data/output")

# Repartition with shuffle (use when you also need even distribution)
df.repartition(8).write.format("delta").mode("append").save("/data/output")

# Rule of thumb: target 128 MB per file
# data_size_mb / 128 = target_partition_count
```

> **Caution:** `coalesce(1)` creates a single file but eliminates all write parallelism. Use `coalesce(N)` where N is the number of target files.

#### Fix 5: Bin-Packing

Bin-packing is what `OPTIMIZE` does internally: it groups small files into larger ones based on a target file size. When using `OPTIMIZE`, bin-packing is automatic. For custom scenarios:

```python
# Manual bin-packing: read small files, write fewer large files
df = spark.read.format("delta").load("/data/small_files_table")
df.repartition(target_file_count).write.format("delta").mode("overwrite").save("/data/compacted_table")
```

> **Photon:** Photon significantly accelerates `OPTIMIZE` operations because the compaction process involves reading and rewriting Parquet files, which benefits from Photon's native vectorized I/O.

### Decision Flow: Which Fix to Use

```
Is this a Delta table?
├── Yes
│   ├── Streaming workload?
│   │   ├── Yes → Enable Auto Compaction + Optimized Writes
│   │   └── No → Schedule regular OPTIMIZE jobs
│   ├── Want fire-and-forget?
│   │   └── Yes → Enable both auto compaction + optimized writes at table level
│   └── Need ZORDER too?
│       └── Yes → Schedule OPTIMIZE ... ZORDER BY (or migrate to Liquid Clustering)
└── No (raw Parquet)
    └── coalesce/repartition before write
```

---

## 5. Delta Lake Optimization

### OPTIMIZE and ZORDER

```sql
-- Basic compaction
OPTIMIZE my_table;

-- Compaction + data co-location by user_id
OPTIMIZE my_table ZORDER BY (user_id);

-- Compaction + multi-column ZORDER
OPTIMIZE my_table ZORDER BY (user_id, event_date);
```

**ZORDER** reorganizes data within files so that rows with similar values in the Z-ordered columns are colocated. This dramatically improves data skipping for filters on those columns.

**Limitations of ZORDER:**
- Must be re-run after each batch of writes — it does not apply to new data automatically.
- Multi-column Z-ordering reduces effectiveness per column (curse of dimensionality).
- Cannot be combined with `PARTITION BY` on the same column.
- Requires a full rewrite of affected data.

### Liquid Clustering

Liquid Clustering is the modern replacement for `PARTITION BY` + `ZORDER`. It is incremental, automatic, and easier to manage.

```sql
-- Create a table with Liquid Clustering
CREATE TABLE my_table (
    id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_date DATE,
    payload STRING
)
USING DELTA
CLUSTER BY (user_id, event_date);

-- Change clustering keys without rewriting data
ALTER TABLE my_table CLUSTER BY (event_type, event_date);

-- Trigger clustering (happens automatically on Databricks, or manually)
OPTIMIZE my_table;
```

**Liquid Clustering vs ZORDER + PARTITION BY:**

| Feature | PARTITION BY + ZORDER | Liquid Clustering |
|---------|----------------------|-------------------|
| Setup | Define partitions at creation, ZORDER separately | Single `CLUSTER BY` clause |
| Incremental | No — ZORDER rewrites all affected data | Yes — only processes new/unclustered data |
| Key changes | Cannot change partition columns after creation | `ALTER TABLE ... CLUSTER BY` — change anytime |
| Automation | Must schedule OPTIMIZE | Automatic on Databricks (or triggered via OPTIMIZE) |
| Multi-column | Partition + ZORDER (separate concerns) | Single unified mechanism |
| Small tables | Over-partitioning risk | Works well at any scale |
| Streaming | Poor (ZORDER not incremental) | Good (incremental by design) |

> **Takeaway:** For new Delta tables on Databricks, prefer Liquid Clustering over `PARTITION BY` + `ZORDER`. It is simpler, more flexible, and performs better for most workloads.

### Data Skipping

Delta Lake stores file-level min/max statistics for the first N columns (default 32) of each data file in the transaction log. When a query has a filter, Delta uses these stats to skip files whose data range cannot match.

```
Query: WHERE user_id = 42

File 1: min(user_id) = 1,   max(user_id) = 100   → MIGHT contain 42 → READ
File 2: min(user_id) = 101, max(user_id) = 200   → Cannot contain 42  → SKIP
File 3: min(user_id) = 201, max(user_id) = 300   → Cannot contain 42  → SKIP

Result: 1 of 3 files read = 67% I/O savings
```

**Maximizing data skipping effectiveness:**
- Use ZORDER or Liquid Clustering on filter columns to minimize min/max overlap across files.
- Place frequently filtered columns in the first 32 columns of the schema (or increase `delta.dataSkippingNumIndexedCols`).
- Avoid random UUIDs as the first column — they have no locality and make stats useless.

```python
# Check how many columns have stats collected
spark.conf.get("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols")
# Default: "32"

# Increase for wide tables where filter columns are beyond position 32
spark.sql("""
    ALTER TABLE my_table
    SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '50')
""")
```

### Deletion Vectors

Deletion Vectors (DVs) mark rows as deleted without rewriting the underlying Parquet file. This makes `DELETE`, `UPDATE`, and `MERGE` operations faster by avoiding full file rewrites.

```
Traditional DELETE (without DVs):
  File A (1 GB) → delete 1 row → rewrite entire File A' (≈1 GB)
  Write amplification: 1 GB written to delete 1 row

With Deletion Vectors:
  File A (1 GB) → delete 1 row → write small DV bitmap (few KB)
  Write amplification: ~0

  Later, OPTIMIZE purges DVs by rewriting only affected files
```

```sql
-- Enable Deletion Vectors on a table
ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

**Trade-off:** Reads must check the DV bitmap to skip deleted rows, adding a small read overhead. Periodic `OPTIMIZE` purges DVs and eliminates this overhead.

### VACUUM

`VACUUM` removes old files that are no longer referenced by the current version of the table (files from previous versions, aborted transactions, etc.).

```sql
-- Remove files older than 7 days (default retention)
VACUUM my_table;

-- Remove files older than 24 hours
VACUUM my_table RETAIN 24 HOURS;

-- Dry run — show files that WOULD be deleted without deleting them
VACUUM my_table DRY RUN;
```

**Performance implications:**
- `VACUUM` does not rewrite data — it only deletes orphaned files.
- After `VACUUM`, time travel to versions older than the retention period is no longer possible.
- `VACUUM` with very short retention (< 7 days) requires setting `spark.databricks.delta.retentionDurationCheck.enabled = false` and risks breaking concurrent readers.
- On tables with millions of files, `VACUUM` can be slow because it must list all files and compare against the transaction log. Run it during off-peak hours.

### Transaction Log Checkpointing

Every 10 commits (by default), Delta writes a checkpoint: a Parquet file that captures the full table state. This prevents the need to replay potentially thousands of JSON log entries.

```
_delta_log/
├── 00000000000000000000.json
├── 00000000000000000001.json
├── ...
├── 00000000000000000009.json
├── 00000000000000000010.checkpoint.parquet   ← checkpoint
├── 00000000000000000010.json
├── 00000000000000000011.json
└── ...
```

**When checkpointing matters:**
- Streaming jobs that commit every few seconds generate many log entries. Without checkpoints, the log replay at read time slows down.
- Multi-part checkpoints (for very large tables) split the checkpoint across multiple files.
- If you notice slow table reads despite small data size, check whether the `_delta_log` directory has accumulated many JSON files without a recent checkpoint.

```python
# Force a checkpoint
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/data/my_table")
dt.generate("symlink_format_manifest")  # Also triggers checkpoint
```

---

## 6. Write Optimization

### Partitioning Strategies for Writes

```python
# Partition by a low-cardinality date column
df.write.format("delta") \
    .partitionBy("event_date") \
    .mode("append") \
    .saveAsTable("my_database.events")
```

**Partitioning guidelines:**

| Guideline | Recommendation |
|-----------|---------------|
| **Cardinality** | Partition column should have < 1,000 distinct values. Higher cardinality → too many small directories. |
| **Partition size** | Each partition should contain at least 1 GB of data. Smaller → small file problem. |
| **Query patterns** | Partition on columns that appear in `WHERE` clauses of most queries. |
| **Date columns** | Most common partitioning strategy. Use `event_date` (not `event_timestamp`). |
| **Over-partitioning** | `partitionBy("year", "month", "day", "hour")` on a small table → thousands of tiny directories. |

> **Takeaway:** With Liquid Clustering available, consider using `CLUSTER BY` instead of `partitionBy` for new tables. It avoids the over-partitioning trap and adapts automatically.

### partitionBy vs Bucketing

| Feature | `partitionBy` | Bucketing (`bucketBy`) |
|---------|--------------|----------------------|
| **Mechanism** | Separate directories per partition value | Fixed number of files, rows hashed into buckets |
| **Best for** | Low-cardinality filter columns | High-cardinality join/aggregation keys |
| **Avoids shuffles** | No (only avoids file reads) | Yes (join/aggregate on bucket key is shuffle-free) |
| **Databricks support** | Full | Limited (Delta + managed tables only) |
| **Maintenance** | None (directories are self-describing) | Must maintain bucket metadata |

```python
# Bucketing example (primarily useful in Hive-compatible contexts)
df.write.format("parquet") \
    .bucketBy(64, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("my_database.events_bucketed")
```

### Append vs Overwrite vs Merge Performance

| Operation | I/O Pattern | Performance | Use When |
|-----------|-------------|-------------|----------|
| **Append** | Write only new files | Fastest write | Adding new data, no dedup needed |
| **Overwrite** | Write new files, mark old as removed | Fast write, but replaces all data | Full table rebuild, idempotent reprocessing |
| **Dynamic partition overwrite** | Overwrite only affected partitions | Moderate — reads partition list | Reprocessing specific date partitions |
| **MERGE (upsert)** | Read + match + write affected files | Slowest — must scan target table | CDC, deduplication, SCD |

```python
# Append — fastest, no read of existing data
df.write.format("delta").mode("append").saveAsTable("events")

# Overwrite — replaces entire table
df.write.format("delta").mode("overwrite").saveAsTable("events")

# Dynamic partition overwrite — only replaces affected partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("delta").mode("overwrite").partitionBy("event_date").saveAsTable("events")
```

### Write Amplification in Delta MERGE

MERGE is the most expensive write pattern because it must:
1. **Read** the target table (or the relevant portion) to find matches.
2. **Rewrite** entire files that contain any matched rows — even if only 1 row in a 1 GB file is updated.
3. **Write** new files for inserts.

```
MERGE INTO target USING source ON target.id = source.id
  WHEN MATCHED THEN UPDATE ...
  WHEN NOT MATCHED THEN INSERT ...

File-level impact:
┌────────────────┐     ┌────────────────┐
│ Target File A   │     │ Target File B   │
│ 1,000,000 rows  │     │ 1,000,000 rows  │
│ 3 rows matched  │     │ 0 rows matched  │
│ → FULL REWRITE  │     │ → NO REWRITE    │
└────────────────┘     └────────────────┘
Write amplification: 1 GB rewritten to update 3 rows
```

**Reducing MERGE write amplification:**
- **Cluster target table** by the merge key → matched rows are colocated in fewer files.
- **Use Deletion Vectors** → updates mark rows as deleted instead of rewriting files.
- **Partition target table** by a column that allows pruning → MERGE only scans relevant partitions.
- **Filter the target** in the MERGE condition to limit scanned files:

```sql
-- BAD: scans entire target table
MERGE INTO target USING source ON target.id = source.id ...

-- GOOD: scans only one day's partition
MERGE INTO target USING source
  ON target.id = source.id AND target.event_date = '2025-01-15' ...
```

---

## 7. Caching Strategies

### When to Cache

Cache when:
- A DataFrame is used **multiple times** in the same job.
- The DataFrame is **expensive to compute** (complex joins, aggregations).
- The DataFrame is **much smaller** than available memory after filtering.

Do NOT cache when:
- The DataFrame is used **only once** — caching adds overhead with no reuse benefit.
- The data is **too large** to fit in memory — it spills to disk and may be slower than recomputation.
- The **source is already fast** (e.g., a small Delta table with data skipping).
- You are in a **streaming job** — caching interferes with incremental processing.

### Storage Levels

```python
from pyspark import StorageLevel

# Default: deserialized Java objects in memory
df.cache()  # Equivalent to df.persist(StorageLevel.MEMORY_AND_DISK)

# All storage levels
df.persist(StorageLevel.MEMORY_ONLY)          # Memory only, recompute if evicted
df.persist(StorageLevel.MEMORY_AND_DISK)      # Spill to disk if memory is full
df.persist(StorageLevel.DISK_ONLY)            # Disk only (useful for very large DataFrames)
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized in memory (less memory, more CPU)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized, spill to disk
```

| Storage Level | Memory Use | CPU Cost | Disk Use | Best For |
|--------------|------------|----------|----------|----------|
| `MEMORY_ONLY` | High | Low | None | Small DataFrames that fit entirely in memory |
| `MEMORY_AND_DISK` | High | Low | Overflow | Default — good general choice |
| `DISK_ONLY` | None | Moderate | High | Large DataFrames, memory-constrained clusters |
| `MEMORY_ONLY_SER` | Medium | Higher (ser/deser) | None | When memory is tight but data must be in-memory |
| `MEMORY_AND_DISK_SER` | Medium | Higher | Overflow | Memory-tight with disk fallback |

### Cache Invalidation

```python
# Unpersist when done
df.unpersist()

# Check what's cached
spark.catalog.isCached("my_table_name")

# Clear all caches
spark.catalog.clearCache()
```

**Common mistake:** Caching a DataFrame, then modifying the underlying table and re-reading — the cache still holds the old data.

```python
# WRONG: stale cache
df = spark.table("events")
df.cache()
df.count()  # Triggers caching

# ... some other process updates the "events" table ...

df.count()  # Returns OLD count from cache!

# FIX: unpersist and re-read
df.unpersist()
df = spark.table("events")
df.cache()
df.count()  # Fresh data
```

### Delta Cache on Databricks

Databricks clusters with SSDs provide an automatic **Delta cache** (also called the **disk cache**) that caches remote data on local NVMe SSDs.

```python
# Delta cache is enabled by default on clusters with SSDs
# No code changes needed — it is transparent

# Verify it's enabled
spark.conf.get("spark.databricks.io.cache.enabled")  # "true" on SSD-backed clusters

# Manually configure
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
```

**Delta cache vs `.cache()` (Spark cache):**

| Feature | Delta Cache | Spark `.cache()` |
|---------|------------|-----------------|
| **Location** | Local SSD | Executor JVM heap / disk |
| **What's cached** | Remote file data (Parquet) | Deserialized DataFrame rows |
| **Automatic** | Yes | No (explicit `.cache()` call) |
| **Affects GC** | No (off-heap, on disk) | Yes (on-heap objects create GC pressure) |
| **Eviction** | LRU on disk | LRU in memory, spill to disk |
| **Scope** | Per-node, survives across queries | Per-DataFrame, cleared on unpersist |
| **Best for** | Repeated reads of same files | Repeated use of computed DataFrames |

> **Photon:** Photon works synergistically with the Delta cache. Photon's native C++ reader can consume cached data from the SSD without the JVM serialization overhead, making repeated scans of cached data significantly faster.

### Decision Tree for Caching

```
Is the DataFrame used more than once in this job?
├── No → Do NOT cache
└── Yes
    ├── Does it fit in executor memory (< 30% of available)?
    │   ├── Yes → cache() / persist(MEMORY_AND_DISK)
    │   └── No
    │       ├── Can you tolerate serialization overhead?
    │       │   ├── Yes → persist(MEMORY_AND_DISK_SER)
    │       │   └── No → persist(DISK_ONLY) or don't cache
    │       └── Is the computation very expensive?
    │           ├── Yes → persist(DISK_ONLY) — disk read still faster than recompute
    │           └── No → Don't cache, let Spark recompute
    └── Is the cluster SSD-backed (Databricks)?
        └── Yes → Delta cache handles file-level caching automatically;
                   only use .cache() for expensive computed DataFrames
```

### Cost of Caching

| Cost | Description |
|------|-------------|
| **Memory pressure** | Cached data competes with execution memory (shuffles, aggregations). Can cause spills. |
| **GC overhead** | Deserialized cached objects (MEMORY_ONLY) create long-lived heap objects that slow GC. |
| **Materialization cost** | The first access triggers full computation and storage. If the cluster crashes, the cache is lost. |
| **Staleness** | Cached data doesn't update when the underlying source changes. |
| **Memory fragmentation** | Large cached DataFrames can cause memory fragmentation, reducing effective memory. |

> **Takeaway:** Caching is not free. Profile your job with and without caching. A well-tuned Delta table with data skipping often outperforms a naive `.cache()` approach because the Delta cache handles file-level caching transparently.

---

## 8. Data Layout Optimization

### Z-Ordering

Z-ordering maps multi-dimensional data to one dimension while preserving locality. It interleaves the bits of the column values to produce a Z-order curve, then sorts data by this value.

```
Traditional sort by (x, y):         Z-order by (x, y):
┌───┬───┬───┬───┐                  ┌───┬───┬───┬───┐
│ 1 │ 2 │ 3 │ 4 │  ← All y=0      │ 1 │ 2 │ 5 │ 6 │
├───┼───┼───┼───┤    together      ├───┼───┼───┼───┤
│ 5 │ 6 │ 7 │ 8 │  ← All y=1      │ 3 │ 4 │ 7 │ 8 │
├───┼───┼───┼───┤    together      ├───┼───┼───┼───┤
│ 9 │10 │11 │12 │                  │ 9 │10 │13 │14 │
├───┼───┼───┼───┤                  ├───┼───┼───┼───┤
│13 │14 │15 │16 │                  │11 │12 │15 │16 │
└───┴───┴───┴───┘                  └───┴───┴───┴───┘

Sort by x → filters on x are great,   Z-order → filters on EITHER x or y
             filters on y are useless            hit localized regions
```

**When to use Z-ordering:**
- You filter on multiple columns that are not partition columns.
- Those columns have high cardinality (many distinct values).
- The table is large enough that data skipping matters (> 1 GB).

**When NOT to use Z-ordering:**
- Single column filters → regular sort or partitioning is simpler.
- Low-cardinality columns → partitioning is more effective.
- Very small tables → overhead outweighs benefit.

```sql
-- Z-order on one column
OPTIMIZE events ZORDER BY (user_id);

-- Z-order on two columns (effectiveness decreases per column)
OPTIMIZE events ZORDER BY (user_id, event_type);

-- Recommendation: limit to 2-4 ZORDER columns for best results
```

### Liquid Clustering: Migration from Z-ORDER

```sql
-- Step 1: Enable Liquid Clustering on an existing table
ALTER TABLE events CLUSTER BY (user_id, event_date);

-- Step 2: Trigger initial clustering (processes all unclustered data)
OPTIMIZE events;

-- Step 3: Subsequent OPTIMIZE calls are incremental
-- (only clusters newly written data)
OPTIMIZE events;

-- Change cluster keys as query patterns evolve
ALTER TABLE events CLUSTER BY (event_type, event_date);
```

**Cluster key selection guidelines:**
- Choose columns that are most commonly used in `WHERE` clauses.
- High-cardinality columns benefit most (user IDs, timestamps).
- Limit to 2–4 cluster keys.
- Order matters less than with ZORDER — Liquid Clustering uses Hilbert curves internally, which handle multi-dimensional locality better than Z-curves.

### Bloom Filters for Point Lookups

Bloom filters are probabilistic data structures that can quickly determine "this value is definitely NOT in this file" or "this value MIGHT be in this file."

```sql
-- Create a bloom filter index on a column
CREATE BLOOMFILTER INDEX ON TABLE events FOR COLUMNS(transaction_id OPTIONS (fpp=0.01, numItems=10000000));
```

```python
# Enable bloom filter usage for reads
spark.conf.set("spark.databricks.io.skipping.bloomFilter.enabled", "true")
```

**When to use bloom filters:**
- Point lookups on high-cardinality columns (e.g., `WHERE transaction_id = 'abc123'`).
- Columns where min/max stats are useless (e.g., UUID columns — min/max spans the entire range).
- Use alongside, not instead of, ZORDER / Liquid Clustering.

**Trade-offs:**
- Bloom filters add storage overhead (the index is written alongside data files).
- False positives mean some unnecessary files are read, but never false negatives.
- `fpp` (false positive probability) controls accuracy vs size: lower fpp = larger index = fewer false positives.

### Statistics Collection

```sql
-- Collect column statistics (used by Catalyst optimizer for join strategies, etc.)
ANALYZE TABLE events COMPUTE STATISTICS;

-- Collect stats for specific columns
ANALYZE TABLE events COMPUTE STATISTICS FOR COLUMNS user_id, event_date, event_type;

-- Check collected statistics
DESCRIBE EXTENDED events user_id;
```

**What stats are collected:**
- Row count, size in bytes.
- Per-column: distinct count, null count, min, max, average length (strings), histogram (optional).

**When to run ANALYZE TABLE:**
- After large data loads or significant data changes.
- Before complex queries with multiple joins where Catalyst needs accurate cardinality estimates.
- Not necessary for Delta data skipping (Delta maintains its own file-level stats automatically).

> **Takeaway:** `ANALYZE TABLE` helps the Catalyst optimizer choose better join strategies and aggregation plans. Delta data skipping works independently of these statistics.

---

## 9. Read Optimization

### Dynamic File Pruning

Dynamic File Pruning (DFP) extends data skipping to work with joins. When a dimension table filter reduces the set of join keys, DFP pushes those keys to the fact table scan, skipping files that don't contain matching keys.

```sql
-- Without DFP: full scan of fact_orders (billions of rows)
-- With DFP: only files containing matching product_ids are read

SELECT o.*, p.product_name
FROM fact_orders o
JOIN dim_products p ON o.product_id = p.product_id
WHERE p.category = 'Electronics';

-- DFP pushes the filtered product_ids from dim_products
-- down to the fact_orders scan, enabling file-level skipping
```

```python
# DFP is enabled by default on Databricks
spark.conf.get("spark.databricks.optimizer.dynamicFilePruning")  # "true"
```

**Requirements for DFP:**
- The fact table must be a Delta table with stats on the join column.
- The dimension side must be small enough to broadcast (or AQE converts it to broadcast).
- ZORDER or Liquid Clustering on the join column in the fact table dramatically increases DFP effectiveness.

### maxPartitionBytes Tuning

`spark.sql.files.maxPartitionBytes` controls how much data each read task processes.

```python
# Default: 128 MB per partition
spark.conf.get("spark.sql.files.maxPartitionBytes")  # "134217728" (128 MB)

# Increase for wide tables with heavy computation per row
spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")

# Decrease for narrow tables or when you need more parallelism
spark.conf.set("spark.sql.files.maxPartitionBytes", "64m")
```

**Tuning guidelines:**

| Scenario | Setting | Reason |
|----------|---------|--------|
| Default | 128 MB | Good balance for most workloads |
| Wide tables (100+ columns) | 64 MB | More parallelism compensates for per-row processing cost |
| Narrow tables, simple scans | 256 MB | Fewer tasks, less scheduling overhead |
| Memory-constrained executors | 64 MB | Smaller partitions reduce peak memory per task |
| Heavy downstream computation | 64 MB | More parallelism to offset processing time |

### Splittable vs Non-Splittable Formats

| Format | Splittable? | Notes |
|--------|------------|-------|
| Parquet | Yes | Each row group is independently readable |
| Delta | Yes | Inherits Parquet splittability |
| ORC | Yes | Each stripe is independently readable |
| CSV (uncompressed) | Yes | Can split on line boundaries |
| JSON (uncompressed) | Yes | Can split on line boundaries (line-delimited JSON) |
| CSV + gzip | **No** | gzip is not splittable — one task reads entire file |
| CSV + bzip2 | Yes | bzip2 supports block-level splitting |
| JSON + gzip | **No** | Same as CSV + gzip |
| Avro | Yes | Each block is independently readable |

**Why splittability matters:** A non-splittable 5 GB gzip file forces a single task to read and decompress the entire file, creating a bottleneck while other executors sit idle.

```python
# PROBLEM: 10 GB gzip CSV file → 1 task, no parallelism
df = spark.read.csv("/data/huge_file.csv.gz")  # Single task!

# FIX: Decompress and convert to Parquet first
df = spark.read.csv("/data/huge_file.csv.gz")
df.repartition(80).write.parquet("/data/huge_file_parquet/")
# Now subsequent reads are parallel and splittable
```

### Handling Compressed Files

| Codec | Splittable | Speed | Ratio | Recommendation |
|-------|-----------|-------|-------|----------------|
| **Snappy** | No (file) / Yes (in Parquet) | Fast | Moderate | Default for Parquet. Best balance. |
| **ZSTD** | No (file) / Yes (in Parquet) | Moderate | Excellent | Best compression ratio. Good for cold storage. |
| **LZ4** | No (file) / Yes (in Parquet) | Fastest | Lower | Best for speed-sensitive workloads. |
| **Gzip** | No | Slow | Good | Avoid for large files. OK for small ingestion files. |
| **Bzip2** | Yes | Slowest | Best | Rarely used. Splittable but too slow. |

```python
# Set Parquet compression codec
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

# Write with specific compression
df.write.option("compression", "zstd").parquet("/data/output")
```

> **Photon:** Photon includes native decompression for Snappy and ZSTD, bypassing the JVM decompression path. This makes ZSTD a more attractive option on Photon clusters — you get better compression ratios without the typical CPU penalty.

### Object Storage Optimization (S3 / ADLS / GCS)

Object storage is high-throughput but high-latency. Optimizing for it differs from local/HDFS storage.

```
Object storage characteristics:
┌────────────────────────────────────────────────┐
│ ✓ Virtually unlimited capacity                  │
│ ✓ High throughput for large sequential reads     │
│ ✗ High latency per request (~5-50ms)             │
│ ✗ No random access / seeks                       │
│ ✗ LIST operations are slow and expensive          │
│ ✗ Rename = copy + delete (not atomic)             │
└────────────────────────────────────────────────┘
```

**Optimization strategies:**

| Strategy | How | Why |
|----------|-----|-----|
| **Avoid small files** | OPTIMIZE, coalesce before write | Fewer files = fewer LIST/GET calls |
| **Use Delta Lake** | Transaction log tracks files | Avoids expensive directory listing |
| **Increase read buffer** | `spark.hadoop.fs.s3a.readahead.range=16m` | Larger reads amortize per-request latency |
| **Enable S3 request parallelism** | `spark.hadoop.fs.s3a.connection.maximum=200` | More concurrent connections for parallel reads |
| **Use regional co-location** | Place compute and storage in same region | Avoid cross-region data transfer latency and cost |
| **Avoid renames on write** | Use Delta (commit protocol avoids rename) | S3 rename = copy + delete, very slow for large files |

```python
# S3 optimization settings
spark.conf.set("spark.hadoop.fs.s3a.readahead.range", "16777216")  # 16 MB readahead
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "200")
spark.conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")  # For random access patterns

# ADLS Gen2 optimization
spark.conf.set("spark.hadoop.fs.azure.readaheadqueue.depth", "16")
```

> **Takeaway:** On object storage, the biggest performance wins come from reducing the number of I/O requests — not the size of each request. Fewer, larger files and Delta Lake's transaction log (which eliminates file listing) are the two most impactful optimizations.

---

## 10. Configuration Reference

### I/O and Storage Configurations

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.sql.files.maxPartitionBytes` | `128m` | Max bytes per partition when reading files |
| `spark.sql.files.openCostInBytes` | `4m` | Estimated cost to open a file (affects partition planning) |
| `spark.sql.parquet.filterPushdown` | `true` | Enable Parquet predicate pushdown |
| `spark.sql.parquet.compression.codec` | `snappy` | Compression codec for Parquet writes |
| `spark.sql.orc.filterPushdown` | `true` | Enable ORC predicate pushdown |
| `spark.sql.hive.convertMetastoreParquet` | `true` | Convert Hive Parquet tables to use Spark's Parquet reader |

### Delta Lake Configurations

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | Coalesce small partitions on write |
| `spark.databricks.delta.autoCompact.enabled` | `false` | Auto-compact small files after write |
| `spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols` | `32` | Number of columns to collect file-level stats for |
| `spark.databricks.delta.retentionDurationCheck.enabled` | `true` | Prevent VACUUM with retention < 7 days |
| `spark.databricks.io.cache.enabled` | `true` (SSD clusters) | Enable Delta cache on local SSDs |
| `spark.databricks.io.cache.maxDiskUsage` | `50g` | Max disk space for Delta cache |
| `spark.databricks.io.skipping.bloomFilter.enabled` | `true` | Enable bloom filter data skipping |
| `spark.databricks.optimizer.dynamicFilePruning` | `true` | Enable Dynamic File Pruning |

### Table Properties (set via ALTER TABLE)

| Property | Description |
|----------|-------------|
| `delta.autoOptimize.optimizeWrite` | Enable optimized writes for this table |
| `delta.autoOptimize.autoCompact` | Enable auto compaction for this table |
| `delta.dataSkippingNumIndexedCols` | Number of columns to collect stats for |
| `delta.enableDeletionVectors` | Enable deletion vectors for faster updates |
| `delta.checkpoint.writeStatsAsStruct` | Write stats as struct in checkpoint (faster reads) |
| `delta.checkpoint.writeStatsAsJson` | Write stats as JSON in checkpoint |

---

## Summary: I/O Optimization Priority Order

When optimizing I/O, apply these strategies in order of impact:

```
 Priority    Strategy                         Typical Impact
 ────────    ────────                         ──────────────
    1        Use Delta Lake / columnar format   10–100x vs CSV
    2        Partition pruning                  10–100x on partitioned columns
    3        Column pruning (select needed)     2–50x on wide tables
    4        Predicate pushdown                 2–10x with good stats
    5        Fix small files (OPTIMIZE)         2–5x on fragmented tables
    6        Z-ORDER / Liquid Clustering        2–5x on multi-column filters
    7        Dynamic File Pruning               2–5x on star schema joins
    8        Caching (when appropriate)         2–10x on repeated reads
    9        Compression tuning (ZSTD)          1.2–2x storage and I/O savings
   10        Object storage tuning              1.2–2x on cloud storage
```

> **Takeaway:** The first five optimizations (format, partitioning, column pruning, predicate pushdown, and file compaction) address 90% of I/O performance problems. Start there before exploring advanced features like bloom filters or custom compression codecs.
