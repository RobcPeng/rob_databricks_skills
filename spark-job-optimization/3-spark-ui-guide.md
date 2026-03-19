# Spark UI Deep Dive: A Practitioner's Guide

The Spark UI is the single most important diagnostic tool for understanding Spark job behavior. Every optimization story starts here. This guide walks through each tab, explains what to look for, and teaches you to recognize the visual patterns that indicate common performance problems.

---

## Table of Contents

1. [Spark UI Overview](#1-spark-ui-overview)
2. [Jobs Tab](#2-jobs-tab)
3. [Stages Tab](#3-stages-tab)
4. [Tasks Tab/Table](#4-tasks-tabtable)
5. [Storage Tab](#5-storage-tab)
6. [SQL/DataFrame Tab](#6-sqldataframe-tab)
7. [Environment Tab](#7-environment-tab)
8. [Executors Tab](#8-executors-tab)
9. [Timeline Views](#9-timeline-views)
10. [Pattern Recognition](#10-pattern-recognition)

---

## 1. Spark UI Overview

### How to Access the Spark UI

**On Databricks:**
- Navigate to the cluster or job run.
- Click the **Spark UI** link in the cluster details page, or click into a running/completed job and select the **Spark UI** tab.
- For completed jobs, the Spark UI is available through the **Driver Logs** page or through the job run's detail view.
- Databricks retains Spark UI data for the lifetime of the cluster plus a configurable retention period.

**Spark History Server (open-source Spark):**
- Spark writes event logs to a configured directory (`spark.eventLog.dir`).
- The History Server (`$SPARK_HOME/sbin/start-history-server.sh`) reads those logs and serves the UI at port 18080 by default.
- Access at `http://<history-server-host>:18080`.

**During a running application:**
- The Spark UI is served live on port 4040 of the driver (4041, 4042, etc. if multiple apps run on the same host).

### The Main Tabs

```
+--------+--------+---------+---------+------+-------------+-----------+----------+
|  Jobs  | Stages | Storage |  Envir  | Exec |  SQL/       | Streaming | (JDBC/   |
|        |        |         | onment  | utors| DataFrame   | (if used) |  ODBC)   |
+--------+--------+---------+---------+------+-------------+-----------+----------+
```

| Tab | What It Tells You |
|---|---|
| **Jobs** | High-level view of all actions. Overall success/failure, duration, stage count per job. |
| **Stages** | The core diagnostic tab. Task-level metrics, shuffle stats, skew evidence, spill, GC. |
| **Storage** | What is cached, how much memory/disk it uses, how it is partitioned. |
| **Environment** | All active Spark configurations, system properties, classpath entries. |
| **Executors** | Resource utilization per executor: memory, cores, GC time, shuffle I/O. |
| **SQL/DataFrame** | Visual query plans with per-operator metrics. The best starting point for DataFrame/SQL workloads. |

> **Key Insight:** For most diagnostic work, you will bounce between the **SQL tab** (to understand the logical plan) and the **Stages tab** (to understand the physical execution). The SQL tab tells you *what* Spark decided to do; the Stages tab tells you *how well* it did it.

---

## 2. Jobs Tab

### What Is a "Job"?

Every Spark **action** triggers a job. Actions are operations that force Spark to compute a result rather than just building a plan. Common actions include:

```python
# Each of these triggers a separate Job
df.count()               # Job 0
df.write.parquet(path)   # Job 1
df.collect()             # Job 2
df.show()                # Job 3
df.toPandas()            # Job 4
```

A single SQL query executed with `spark.sql(...).write` typically creates one job, but complex queries with subqueries, CTEs, or multiple shuffles can create multiple jobs.

### The Job Listing Page

```
+----------------------------------------------------------------------+
| Spark Jobs (4 total: 3 succeeded, 1 failed)                         |
+----------------------------------------------------------------------+
| Event Timeline  [====|=====|===========|===]                         |
+----------------------------------------------------------------------+
| Job Id | Description        | Submitted    | Duration | Stages      |
|--------|--------------------|--------------|----------|-------------|
| 3      | save at script:45  | 14:32:01     | 12.4 min | 4/4         |
| 2      | count at script:38 | 14:31:55     | 5.2 s    | 1/1         |
| 1      | save at script:30  | 14:25:12     | 6.8 min  | 3/3         |
| 0      | count at script:15 | 14:25:10     | 1.3 s    | 1/1 (1 skp) |
+----------------------------------------------------------------------+
```

**What to look for:**

- **Duration column:** Which jobs are taking the longest? Focus optimization efforts there.
- **Stages column:** Format is `completed/total (skipped)`. Skipped stages mean Spark reused cached or shuffle data from a previous computation. A high skip count often means caching is working.
- **Description:** Shows the action and the line number in your code. Use this to map jobs back to your source code.
- **Failed jobs:** Shown in red. Click through to see the exception and the failing stage/task.

### Job Details Page

Clicking a job takes you to its detail view, showing:

- A **DAG visualization** of the stages in this job (boxes are stages, arrows are shuffle dependencies).
- A timeline showing when each stage ran.
- Stage listing with per-stage metrics.

**Duration breakdown thinking:**

A job's wall-clock duration is determined by its critical path through stages. Stages within the same job can sometimes run in parallel (if they are independent branches of the DAG), but stages connected by shuffle dependencies must run sequentially.

```
Job 3 DAG:
  Stage 5 (scan table A) ──┐
                            ├── Stage 7 (join + aggregate) ── Stage 8 (write)
  Stage 6 (scan table B) ──┘

Wall clock: max(Stage 5, Stage 6) + Stage 7 + Stage 8
```

> **Key Insight:** If two scan stages are running sequentially instead of in parallel, you may have a resource bottleneck (not enough executors) or a scheduling issue. Check the timeline view.

### Identifying Failed and Slow Jobs

- **Failed jobs** have a red status indicator. Click into them to find the failing stage, then the failing task, then the error message and stack trace.
- **Slow jobs** are relative to your expectations. If a daily job suddenly takes 3x longer, compare the Spark UI from today vs. yesterday. Look for changes in data volume (input size), partition count, or shuffle sizes.

---

## 3. Stages Tab

The Stages tab is the **workhorse of performance debugging**. This is where you spend most of your time.

### Stage Listing

```
+--------------------------------------------------------------------------+
| Stages (12 total: 10 completed, 1 active, 1 pending)                    |
+--------------------------------------------------------------------------+
| Stage Id | Description          | Duration | Tasks    | Input  | Output |
|----------|----------------------|----------|----------|--------|--------|
| 11       | save at script:45    | (active) | 45/200   | --     | --     |
| 10       | hashaggregate ...    | 8.2 min  | 200/200  | 2.1 GB | 156 MB |
| 9        | exchange ...         | 4.5 min  | 200/200  | --     | --     |
| 8        | scan parquet ...     | 32 s     | 150/150  | 45 GB  | 12 GB  |
| ...      |                      |          |          |        |        |
+--------------------------------------------------------------------------+
```

### Stage Details Page Deep Dive

Clicking a stage reveals the richest diagnostic information in the entire Spark UI. Here is what the page contains and what each section means.

#### Summary Metrics (Aggregated Across All Tasks)

```
+------------------------------------------------------------------------+
| Summary Metrics for 200 Completed Tasks                                |
+------------------------------------------------------------------------+
| Metric              | Min    | 25th % | Median | 75th % |  Max        |
|---------------------|--------|--------|--------|--------|-------------|
| Duration            | 0.5 s  | 1.2 s  | 1.8 s  | 2.5 s  | 14.2 min   |
| GC Time             | 0 ms   | 12 ms  | 45 ms  | 120 ms | 8.5 min    |
| Input Size          | 10 MB  | 48 MB  | 52 MB  | 55 MB  | 1.2 GB     |
| Shuffle Read        | 0.8 MB | 4.2 MB | 5.1 MB | 5.8 MB | 890 MB     |
| Shuffle Write       | 1.1 MB | 5.0 MB | 5.5 MB | 6.2 MB | 920 MB     |
| Shuffle Spill (Mem) | 0 B    | 0 B    | 0 B    | 0 B    | 4.5 GB     |
| Shuffle Spill (Disk)| 0 B    | 0 B    | 0 B    | 0 B    | 1.8 GB     |
+------------------------------------------------------------------------+
```

This table is a goldmine. Here is exactly what to examine:

#### Input/Output/Shuffle Read/Shuffle Write Sizes

- **Input Size:** How much data each task reads from the data source (e.g., Parquet files, Delta tables). If one task reads 1.2 GB while the median is 52 MB, you have skewed input partitions (common with Delta/Parquet files of uneven sizes).
- **Output Size:** Data written out. Uneven output can indicate skewed keys in the final write partitioning.
- **Shuffle Read:** Data pulled from other executors during a shuffle. Large shuffle reads mean the stage depends heavily on the output of a prior stage. Large variance indicates data skew in the shuffle key.
- **Shuffle Write:** Data written to shuffle files for downstream stages. This is the "cost" this stage imposes on the next stage.

#### Task Distribution (Min / Median / Max Duration, GC Time)

The spread between min and max task duration tells the skew story:

- **Healthy:** Max is within 2-3x of the median.
- **Suspicious:** Max is 10x the median.
- **Severe skew:** Max is 100x+ the median (e.g., median 2s, max 15 min).

**GC Time** shows how much time the JVM spent on garbage collection within each task:
- **Healthy:** GC time is < 5% of task duration.
- **Warning:** GC time is 10-20% of task duration.
- **Critical:** GC time is > 30% of task duration. Tasks are spending more time cleaning up memory than doing useful work.

#### Shuffle Spill (Memory and Disk)

Shuffle spill occurs when Spark cannot hold all the shuffle data in memory and must write intermediate data to disk.

- **Shuffle Spill (Memory):** The amount of data that was in memory before being spilled. This is the deserialized (in-memory) size.
- **Shuffle Spill (Disk):** The amount of data written to disk. This is the serialized (on-disk) size, so it is typically smaller than the memory figure.

**Any spill > 0 is a signal to investigate.** Small amounts of spill (a few MB) are often harmless. Large spill (GBs) means Spark is thrashing between memory and disk, which is devastating for performance.

```
Spill relationship:
  Shuffle Spill (Memory) >= Shuffle Spill (Disk)

  The ratio tells you about compression effectiveness:
    Memory: 4.5 GB, Disk: 1.8 GB  =>  ~2.5x compression ratio
```

**Common causes of spill:**
- Not enough memory per task (`spark.executor.memory` too low, or too many cores per executor).
- Data skew causing one task to handle vastly more data than others.
- Aggregations or joins on high-cardinality keys with skewed distributions.

**Fixes:**
- Increase `spark.executor.memory` or decrease `spark.executor.cores`.
- Repartition to spread data more evenly.
- Use salting techniques for skewed joins.
- Enable AQE skew join optimization.

#### Data Skew Visible in Task Metrics

Skew is visible as a dramatic difference between the median and max in any metric:

```
SKEWED STAGE (look at the spread):
  Duration:      Min=0.3s   Median=1.5s   Max=22 min    <-- 880x spread
  Shuffle Read:  Min=0.1MB  Median=5MB    Max=4.2GB     <-- 840x spread
  Spill (Disk):  Min=0B     Median=0B     Max=3.1GB     <-- only the max task spills

HEALTHY STAGE (tight distribution):
  Duration:      Min=1.1s   Median=2.0s   Max=3.8s      <-- 3.5x spread
  Shuffle Read:  Min=4.2MB  Median=5.1MB  Max=6.8MB     <-- 1.6x spread
  Spill (Disk):  Min=0B     Median=0B     Max=0B        <-- no spill
```

#### Locality Levels

Task locality describes how close the task's computation is to its data:

| Level | Meaning | Performance |
|---|---|---|
| `PROCESS_LOCAL` | Data is in the same JVM (cached in executor memory) | Best |
| `NODE_LOCAL` | Data is on the same node (local disk or another executor on the node) | Good |
| `RACK_LOCAL` | Data is on the same rack | Acceptable |
| `ANY` | Data is on a remote node | Worst |

In cloud environments (including Databricks), locality is less meaningful because storage is typically remote (S3, ADLS, GCS). You will often see `ANY` locality, which is expected. Locality matters more when reading cached data or shuffle files.

> **Key Takeaway:** On the Stages detail page, immediately check the Summary Metrics table. Look at the **spread** (min vs max) for Duration, Shuffle Read, and Shuffle Spill. A wide spread is the signature of data skew. Any Shuffle Spill (Disk) > 0 deserves investigation.

---

## 4. Tasks Tab/Table

### Reading the Task-Level Table

At the bottom of each Stage detail page is the task table. This table lists every individual task with its full set of metrics.

```
+---------------------------------------------------------------------------------+
| Tasks (200 total, showing 1-20)                                                 |
+---------------------------------------------------------------------------------+
| Index | Task Id | Attempt | Status | Locality | Executor | Duration | GC Time  |
|-------|---------|---------|--------|----------|----------|----------|----------|
| 0     | 4521    | 0       | SUCCESS| ANY      | exec-3   | 1.2 s    | 15 ms   |
| 1     | 4522    | 0       | SUCCESS| ANY      | exec-1   | 1.8 s    | 22 ms   |
| ...   |         |         |        |          |          |          |         |
| 147   | 4668    | 0       | SUCCESS| ANY      | exec-2   | 14.2 min | 8.5 min |
+---------------------------------------------------------------------------------+
  (continued columns)
| Sched Delay | Deser Time | Compute | Ser Time | Shuf Write | Shuf Read | Result |
|-------------|------------|---------|----------|------------|-----------|--------|
| 3 ms        | 5 ms       | 1.1 s   | 1 ms     | 5.5 MB     | 5.1 MB    | 2 KB   |
| 4 ms        | 6 ms       | 1.7 s   | 1 ms     | 5.8 MB     | 4.8 MB    | 2 KB   |
| ...         |            |         |          |            |           |        |
| 5 ms        | 45 ms      | 5.2 min | 12 ms    | 920 MB     | 890 MB    | 2 KB   |
+---------------------------------------------------------------------------------+
```

### Sorting to Find Stragglers

Click the **Duration** column header to sort descending. The top rows are your stragglers -- the tasks that determine the wall-clock time of the entire stage. If the stage took 14 minutes, it is because the slowest task took 14 minutes, even if 199 other tasks finished in 2 seconds.

### Understanding Task Time Components

Each task's duration is broken down into phases. Here is what each phase means and what healthy values look like:

```
Task Execution Timeline (for a single task):
|--Sched--|--Deser--|----------Compute----------|--Ser--|--Shuf Write--|--Result--|
   Delay    Time      (includes GC pauses)        Time     Time           Time
```

| Metric | What It Is | Healthy | Unhealthy |
|---|---|---|---|
| **Scheduler Delay** | Time from task being scheduled to executor starting it. Includes serializing the task, network transfer, and queuing. | < 50 ms | > 1 s (indicates executor overload or network issues) |
| **Deserialization Time** | Time to deserialize the task's closure and dependencies on the executor. | < 100 ms | > 1 s (indicates large broadcast variables or complex closures) |
| **Compute Time** | The actual work: reading data, applying transformations, running UDFs. GC time is included within this. | Dominates task duration | N/A (this is expected to be the bulk) |
| **GC Time** | Time the JVM spent in garbage collection during this task. A subset of compute time. | < 5% of compute | > 20% of compute |
| **Serialization Time** | Time to serialize the task's result. | < 10 ms | > 1 s (indicates large result being sent back to driver) |
| **Shuffle Write Time** | Time to write shuffle output to local disk. | < 10% of task time | > 30% (indicates disk I/O bottleneck) |
| **Getting Result Time** | Time for the driver to fetch the task result from the executor. | < 50 ms | > 1 s (indicates large task results or driver overload) |

### Healthy vs. Unhealthy Task Distributions

**Healthy distribution:**
```
Task durations (200 tasks):
  |
  |  ████████████████████
  |  ████████████████████████
  |  ████████████████████████████
  |  ████████████████████████████████
  |  ████████████████████████████
  |  ████████████████████████
  |  ████████████████████
  |  ██████████████
  +------------------------------------>
  1.0s   1.5s   2.0s   2.5s   3.0s   3.5s

  Looks like: Normal/bell-curve distribution. Tight range.
  Result:     Stage finishes quickly. All tasks share the load evenly.
```

**Unhealthy distribution (skew):**
```
Task durations (200 tasks):
  |
  |  ██████████████████████████████████████   (199 tasks here)
  |
  |
  |
  |                                                          █  (1 task here)
  +-------------------------------------------------------------->
  1s    2s    3s    4s    5s   ...   10min   12min   14min

  Looks like: A wall of fast tasks and one or two extreme outliers.
  Result:     Stage wall-clock time = outlier task time. All other
              executors sit idle waiting for the straggler.
```

**Unhealthy distribution (under-partitioned):**
```
Task durations (8 tasks on a 64-core cluster):
  |
  |  ████████
  |  ████████
  |  ████████
  +----------->
  4min   5min   6min

  Looks like: Few tasks, all taking a long time. Cluster is mostly idle.
  Result:     Only 8 of 64 cores are utilized. Repartition to create
              more parallel work units.
```

> **Key Takeaway:** Sort tasks by duration descending. If the slowest task is orders of magnitude slower than the median, you have a skew problem. If GC time dominates compute time, you have a memory problem. If scheduler delay is high, you have a resource contention problem.

---

## 5. Storage Tab

### What the Storage Tab Shows

The Storage tab displays all currently cached (persisted) RDDs and DataFrames.

```
+------------------------------------------------------------------------+
| Storage                                                                |
+------------------------------------------------------------------------+
| RDD Name             | Storage Level    | Cached Parts | Size in Mem  |
|----------------------|------------------|--------------|--------------|
| *(2) Delta [path...] | Disk Memory Deser| 150/200      | 8.4 GB       |
| *(1) InMemoryRel...  | Memory Deser 1x  | 200/200      | 2.1 GB       |
+------------------------------------------------------------------------+
```

Clicking an entry shows how the cached data is distributed across executors.

### Memory vs. Disk Usage

Spark's storage levels determine where cached data lives:

| Storage Level | Where | Serialized? | Replicas | Use When |
|---|---|---|---|---|
| `MEMORY_ONLY` | Heap memory | No (deserialized Java objects) | 1 | Default. Best performance if it fits. |
| `MEMORY_AND_DISK` | Memory, spills to disk | No | 1 | When data is too large for memory. Avoids recomputation. |
| `DISK_ONLY` | Local disk | Yes | 1 | When memory is scarce but recomputation is expensive. |
| `MEMORY_ONLY_SER` | Heap memory | Yes (serialized bytes) | 1 | Trades CPU for memory savings (~2-5x smaller). |
| `OFF_HEAP` | Off-heap memory | Yes | 1 | Avoids GC overhead. Requires explicit config. |

In Databricks, `df.cache()` uses `MEMORY_AND_DISK` by default (different from open-source Spark's `MEMORY_ONLY`).

### Partition Distribution Across Executors

The detail view for a cached dataset shows which executor holds which partitions:

```
+------------------------------------------------------------+
| Data Distribution (200 partitions across 10 executors)     |
+------------------------------------------------------------+
| Executor | Address          | Mem Used  | Disk Used | Parts|
|----------|------------------|-----------|-----------|------|
| exec-0   | 10.0.0.1:44231   | 1.05 GB   | 0 B       | 20   |
| exec-1   | 10.0.0.2:44232   | 1.02 GB   | 0 B       | 20   |
| exec-2   | 10.0.0.3:44233   | 0.85 GB   | 124 MB    | 20   |
| ...      |                  |           |           |      |
+------------------------------------------------------------+
```

Uneven distribution here (e.g., one executor holding 80% of cached data) indicates skewed partitions.

### When Caching Helps vs. Hurts

**Caching helps when:**
- A DataFrame is used multiple times in the same job (e.g., once for an aggregate, again for a join).
- The recomputation cost is high (complex transformations, expensive scans).
- The data fits comfortably in memory.

**Caching hurts when:**
- The data is used only once. Caching adds overhead with no benefit.
- The cached data is so large it evicts other useful cached data or causes GC pressure.
- The data is read from a fast source (e.g., Delta table with data skipping) and caching provides minimal speedup.
- You cache before a filter that dramatically reduces data volume. Cache *after* the filter instead.

```python
# Bad: caching before filter
df = spark.read.table("huge_table").cache()  # caches 500 GB
filtered = df.filter(col("date") == "2026-01-01")  # only need 5 GB

# Good: caching after filter
df = spark.read.table("huge_table")
filtered = df.filter(col("date") == "2026-01-01").cache()  # caches 5 GB
```

> **Key Takeaway:** Check the Storage tab to verify that your `.cache()` or `.persist()` calls are actually caching data. If "Cached Partitions" is less than the total, some partitions were evicted. If the cached size is much larger than expected, you may be caching too early in the pipeline.

---

## 6. SQL/DataFrame Tab

The SQL tab is the **most informative starting point** for diagnosing DataFrame and Spark SQL workloads. It shows the physical query plan as a graphical DAG with runtime metrics embedded in each operator node.

### Accessing the SQL Tab

Every DataFrame action or SQL query generates an entry in the SQL tab:

```
+------------------------------------------------------------------------+
| SQL / DataFrame                                                        |
+------------------------------------------------------------------------+
| Id | Description                          | Submitted    | Duration    |
|----|--------------------------------------|--------------|-------------|
| 3  | save at NativeMethodAccessorImpl:...  | 14:32:01     | 12.4 min    |
| 2  | count                                | 14:31:55     | 5.2 s       |
| 1  | save at NativeMethodAccessorImpl:...  | 14:25:12     | 6.8 min     |
| 0  | count                                | 14:25:10     | 1.3 s       |
+------------------------------------------------------------------------+
```

### Reading the DAG Visualization

Clicking a query shows a top-to-bottom or bottom-to-top DAG (depending on your Spark version). Each box is a physical operator. The DAG reads from data sources at the leaves up to the final output at the root.

```
Example DAG for: SELECT a.id, b.name, SUM(a.amount)
                 FROM transactions a JOIN customers b ON a.cust_id = b.id
                 WHERE a.date > '2026-01-01'
                 GROUP BY a.id, b.name

  +--------------------------+
  | WholeStageCodegen (6)    |
  | HashAggregate            |        <-- Final aggregation
  | rows output: 1,245,000  |
  +--------------------------+
            |
  +--------------------------+
  | Exchange (hashpart...)   |        <-- Shuffle for aggregation
  | data size: 156 MB        |
  +--------------------------+
            |
  +--------------------------+
  | WholeStageCodegen (5)    |
  | HashAggregate            |        <-- Partial (pre-shuffle) aggregation
  | rows output: 2,890,000  |
  +--------------------------+
            |
  +--------------------------+
  | BroadcastHashJoin        |        <-- Join (broadcast chosen by optimizer)
  | rows output: 48,000,000 |
  +--------------------------+
        /            \
  +-----------+   +------------------+
  | Filter    |   | BroadcastExch    |    <-- customers table broadcast
  | date >... |   | data: 12 MB      |
  | rows:     |   +------------------+
  | 48M       |          |
  +-----------+   +------------------+
       |          | Scan parquet     |    <-- customers scan
  +-----------+   | rows: 500,000   |
  | Scan      |   | size: 12 MB     |
  | parquet   |   +------------------+
  | (transact)|
  | rows: 48M |
  | size: 2 GB|
  +-----------+
```

### Node Metrics

Each operator node includes runtime metrics. The most important ones:

| Metric | What It Tells You |
|---|---|
| **number of output rows** | How many rows this operator produces. A sudden explosion in row count (e.g., a join that outputs 100x more rows than its inputs) indicates a Cartesian-like join or a bad join key. |
| **data size** | Amount of data flowing through this point. Useful for understanding where data volume increases or decreases. |
| **time** (when available) | Wall-clock time spent in this operator. Available in some Spark versions and on Photon. |
| **number of files read** | For scan operators, how many files were read. |
| **metadata time** | Time spent listing files and reading metadata. High values indicate a large number of small files. |
| **scan time** | Time spent actually reading file contents. |

### Identifying Expensive Operators

Look for operators that are:

1. **Producing an unexpected number of rows.** A join that outputs 10 billion rows when both inputs have 10 million rows each is a red flag (possible many-to-many join on a non-unique key).

2. **Exchange (Shuffle) nodes with large data sizes.** Each Exchange node represents a shuffle. If an Exchange moves 500 GB, that is 500 GB of data being serialized, written to disk, transferred over the network, and deserialized. Consider whether the shuffle can be avoided (e.g., by using a broadcast join instead).

3. **Sort nodes.** Sorting is expensive. If you see a Sort node that is not required by your query, check whether it is being introduced by a SortMergeJoin that could be converted to a BroadcastHashJoin.

4. **Filter nodes with low selectivity placed after expensive operations.** If a Filter outputs 1% of its input, but it appears after a join, the join processed 100x more data than necessary. Push the filter earlier (Spark usually does this via predicate pushdown, but UDFs can prevent it).

### Correlating SQL Tab with Stages Tab

Each WholeStageCodegen block in the SQL DAG maps to one or more stages. The Exchange operators are the stage boundaries. To trace a slow stage back to the query plan:

1. Note the stage ID of the slow stage from the Stages tab.
2. In the SQL tab DAG, look for the Exchange node that corresponds to that stage boundary.
3. The operators between two Exchange nodes (or between a scan and the first Exchange) form the work done in that stage.

Spark versions 3.0+ annotate the DAG nodes with stage IDs, making this correlation easier.

### Duration Breakdown per Operator

**On Databricks with Photon**, the SQL tab shows per-operator timing, making it easy to see which operator consumed the most time. Look for operators highlighted with longer bars or higher time values.

In standard Spark, per-operator timing is less granular. You infer operator cost from the stage durations and the operator-to-stage mapping.

> **Photon Note:** When Photon is enabled, the SQL tab displays Photon-specific operators (e.g., `PhotonGroupingAgg`, `PhotonBroadcastHashJoin`, `PhotonShuffleExchangeSink`). These are native (C++) replacements for their JVM counterparts. Photon operators generally appear faster in the metrics because they avoid JVM overhead and benefit from vectorized execution. If you see a mix of Photon and non-Photon operators, the non-Photon ones (e.g., those involving UDFs or unsupported expressions) may be bottlenecks.

> **Key Takeaway:** Start at the SQL tab. Read the DAG bottom-to-top. Look at row counts to verify your mental model of the query. Find Exchange nodes with large data sizes -- those are the expensive shuffles. Cross-reference with the Stages tab for task-level detail.

---

## 7. Environment Tab

The Environment tab shows every Spark configuration that is active for the application. This is essential for verifying that your tuning settings are actually in effect.

### Key Configurations to Check

**Adaptive Query Execution (AQE):**
```
spark.sql.adaptive.enabled                          = true
spark.sql.adaptive.coalescePartitions.enabled        = true
spark.sql.adaptive.skewJoin.enabled                  = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor    = 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
spark.sql.adaptive.advisoryPartitionSizeInBytes      = 128MB
```

**Broadcast Thresholds:**
```
spark.sql.autoBroadcastJoinThreshold                 = 10485760  (10 MB)
```
If you set this to 100MB but the Environment tab still shows 10MB, your configuration was not applied correctly (perhaps it was set after the SparkSession was created, or overridden by a cluster policy).

**Memory Settings:**
```
spark.executor.memory                                = 8g
spark.executor.memoryOverhead                        = 2g
spark.memory.fraction                                = 0.6
spark.memory.storageFraction                         = 0.5
spark.sql.shuffle.partitions                         = 200
```

**Shuffle and I/O:**
```
spark.sql.shuffle.partitions                         = 200
spark.default.parallelism                            = 400
spark.sql.files.maxPartitionBytes                    = 134217728  (128 MB)
```

**Photon (Databricks):**
```
spark.databricks.photon.enabled                      = true
```

### Common Verification Scenarios

- "I set `spark.sql.shuffle.partitions` to 2000 but it still seems like 200." Check the Environment tab. If it shows 200, your config was not applied. If it shows 2000 but AQE is coalescing partitions, the actual runtime partition count will be lower (which is fine -- AQE is doing its job).
- "I increased broadcast threshold but the plan still shows SortMergeJoin." Check the Environment tab to verify the threshold. Then check if the table's estimated size exceeds the threshold (Spark uses catalog statistics, which may be stale).

> **Key Takeaway:** Always verify your configurations in the Environment tab. It is the source of truth for what Spark is actually using, regardless of what you think you set.

---

## 8. Executors Tab

### Executor Summary

```
+--------------------------------------------------------------------------------+
| Executors (10 + driver)                                                        |
+--------------------------------------------------------------------------------+
|          | RDD    | Storage | Disk    | Active | Failed | Complete | Total     |
| Executor | Blocks | Memory  | Used    | Tasks  | Tasks  | Tasks    | Duration  |
|----------|--------|---------|---------|--------|--------|----------|-----------|
| driver   | 0      | 0 B     | 0 B     | 0      | 0      | 0        | 0 ms      |
| exec-0   | 20     | 1.05 GB | 0 B     | 4      | 0      | 185      | 12.4 min  |
| exec-1   | 20     | 1.02 GB | 0 B     | 4      | 0      | 192      | 11.8 min  |
| exec-2   | 20     | 850 MB  | 124 MB  | 4      | 0      | 178      | 28.5 min  |
| ...      |        |         |         |        |        |          |           |
+--------------------------------------------------------------------------------+
  (continued columns)
| Total GC  | Shuf Read | Shuf Write | Blacklisted |
|-----------|-----------|------------|-------------|
| 0 ms      | 0 B       | 0 B        | No          |
| 45 s      | 12.5 GB   | 14.2 GB    | No          |
| 38 s      | 11.8 GB   | 13.5 GB    | No          |
| 8.2 min   | 35.2 GB   | 38.1 GB    | No          |
| ...       |           |            |             |
+--------------------------------------------------------------------------------+
```

### What to Monitor

**Active Tasks:**
Shows how many tasks are currently running on each executor. If most executors show 0 active tasks while one shows its full core count, work distribution is uneven (likely caused by data skew at the stage level).

**Memory Usage:**
- **Storage Memory:** Memory used for cached data.
- **Execution Memory:** Memory used for shuffles, joins, sorts, aggregations (not shown directly, but high GC + spill implies it is under pressure).

**GC Time:**
Total garbage collection time across all tasks run on that executor. Compare GC time to total task duration:
- exec-0: 45s GC / 12.4 min total = 6% -- acceptable.
- exec-2: 8.2 min GC / 28.5 min total = 29% -- problematic.

If one executor has dramatically more GC time than others, it is likely processing a skewed partition that creates many objects in memory.

**Shuffle Read/Write:**
- Roughly even shuffle read/write across executors indicates balanced work.
- One executor with 3x the shuffle read of others is handling a skewed partition.

**Failed Tasks:**
Tasks can fail and be retried on the same or different executor. Frequent failures on a specific executor may indicate a bad node (disk issues, memory errors). Spark will eventually blacklist such executors.

### Identifying Underutilized or Overloaded Executors

**Underutilized executors** show:
- Low total task duration relative to wall-clock time.
- Few completed tasks compared to other executors.
- Cause: Not enough partitions (fewer partitions than executor cores), or data locality preferences causing uneven scheduling.

**Overloaded executors** show:
- High GC time.
- High shuffle read/write (handling skewed data).
- Disk used > 0 (shuffle spill).
- Possible blacklisting.
- Cause: Data skew sending disproportionate data to one executor, or insufficient executor memory.

> **Key Takeaway:** Compare executor metrics side by side. Healthy workloads show relatively even GC time, shuffle I/O, and task counts across executors. One outlier executor with dramatically higher metrics is the smoking gun for data skew.

---

## 9. Timeline Views

Timeline views provide a temporal perspective that the tabular metrics cannot. They answer the question: "What was happening at each moment during execution?"

### Job Timeline

Found at the top of the Jobs tab. Shows all jobs on a horizontal timeline:

```
Time:  14:25        14:27        14:29        14:31        14:33
       |            |            |            |            |
Job 0: [==]
Job 1: [=========================]
Job 2:                                       [==]
Job 3:                                         [========================]
```

**What to look for:**
- Jobs running sequentially when they could be parallel (suggests single-threaded driver code).
- Gaps between jobs (time spent on driver-side computation, or waiting for resources).

### Stage Timeline

Found on the Job detail page. Shows stages within a job:

```
Time:  14:25        14:27        14:29        14:31
       |            |            |            |
Stage 5: [======]                                    (scan A)
Stage 6: [====]                                      (scan B)
Stage 7:         [========================]          (join)
Stage 8:                                    [======] (write)
```

**What to look for:**
- **Parallelism:** Independent stages (like two scans) should overlap. If they run sequentially, you may lack resources.
- **Scheduling gaps:** Dead time between stages indicates driver-side overhead, scheduling delays, or waiting for resources.
- **Dominant stage:** The longest stage determines the job's wall-clock time. Focus optimization there.

### Task Timeline (Event Timeline)

Found on the Stage detail page. Shows individual tasks on a timeline, colored by executor:

```
Time:     0s        10s       20s       30s       40s       50s       60s
          |         |         |         |         |         |         |
Exec-0:   [task][task][task][task][task][task]
Exec-1:   [task][task][task][task][task][task]
Exec-2:   [task][task][task][task][task][task]
Exec-3:   [task][task][task][task][task][taaaaaaaaaaaaaaaaaaaaaaaaask]
Exec-4:   [task][task][task][task][task]
```

**What to look for:**

**Long tail tasks (stragglers):**
```
Exec-0:   [=====][=====][=====]
Exec-1:   [=====][=====][=====]
Exec-2:   [=====][=====][=====]
Exec-3:   [=====][=====][=====][======================================]  <-- straggler
                                ^
                                All other executors are idle, waiting
```
The stage cannot complete until the straggler finishes. This is classic data skew.

**Scheduling gaps:**
```
Exec-0:   [=====]     [=====]     [=====]     [=====]
                  ^^^         ^^^         ^^^
                  gaps = scheduling delay or resource contention
```
If tasks have gaps between them, the executor is idle between tasks. This can indicate high scheduler delay or task serialization overhead.

**Low parallelism / under-partitioned:**
```
Exec-0:   [==============================]
Exec-1:   [==============================]
Exec-2:
Exec-3:
Exec-4:
```
Only 2 of 5 executors are doing work. The stage has only 2 partitions. Repartition to at least match the total core count.

**Good parallelism:**
```
Exec-0:   [===][===][===][===][===]
Exec-1:   [===][===][===][===][===]
Exec-2:   [===][===][===][===][===]
Exec-3:   [===][===][===][===][===]
Exec-4:   [===][===][===][===][===]
```
All executors are busy with similarly-sized tasks. This is the ideal pattern.

> **Key Takeaway:** The task timeline is the fastest way to visually identify skew (one long bar), under-partitioning (idle executors), and scheduling overhead (gaps between tasks). Always check it when a stage is slower than expected.

---

## 10. Pattern Recognition

This section provides a visual checklist for the most common Spark performance problems. For each pattern, we describe what you would see in the Spark UI.

### Pattern: Data Skew

**Where to look:** Stages tab > Stage detail > Summary Metrics, Task Timeline.

**What it looks like:**

```
Summary Metrics:
  Duration:     Min=0.5s   Median=2.1s   Max=18.3 min
  Shuffle Read: Min=0.1MB  Median=5.2MB  Max=6.8 GB
  Spill (Disk): Min=0B     Median=0B     Max=4.2 GB

Task Timeline:
  [==][==][==][==][==][==][==][==][==][================================================]
  (199 tasks finish quickly)                 (1 task takes 10x longer)

Executors Tab:
  Most executors:  GC=30s,  Shuffle Read=8GB
  One executor:    GC=9min, Shuffle Read=52GB
```

**Signature:** Extreme variance between median and max in any metric. One or two tasks dominate the stage duration.

**Remediation:**
- Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled = true`
- Salt skewed join keys
- Pre-filter or pre-aggregate before the join
- Isolate and handle the hot key separately

```python
# Salting example for skewed join key
from pyspark.sql.functions import col, lit, rand, concat

salt_buckets = 10
# Salt the skewed (left) side
df_left_salted = df_left.withColumn("salt", (rand() * salt_buckets).cast("int"))
df_left_salted = df_left_salted.withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)
# Explode the right side to match all salts
from pyspark.sql.functions import explode, array
df_right_exploded = df_right.withColumn(
    "salt", explode(array([lit(i) for i in range(salt_buckets)]))
)
df_right_exploded = df_right_exploded.withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)
# Join on salted key
result = df_left_salted.join(df_right_exploded, "salted_key")
```

---

### Pattern: Shuffle Spill

**Where to look:** Stages tab > Stage detail > Summary Metrics (Shuffle Spill columns).

**What it looks like:**

```
Summary Metrics:
  Shuffle Spill (Memory): Min=0B  Median=512MB  Max=8.5GB
  Shuffle Spill (Disk):   Min=0B  Median=180MB  Max=3.2GB
  GC Time:                Min=5ms Median=2.1s   Max=45s

Stage duration is 3-5x what you would expect based on data volume.
```

**Signature:** Non-zero values in the Shuffle Spill columns. Often accompanied by elevated GC time (because Spark repeatedly allocates and frees memory as it spills and reads back).

**Remediation:**
- Increase executor memory: `spark.executor.memory`
- Decrease cores per executor (gives each task more memory): `spark.executor.cores`
- Increase partitions to reduce data per task: `spark.sql.shuffle.partitions`
- Fix underlying data skew (spill often accompanies skew)

---

### Pattern: GC Pressure

**Where to look:** Stages tab > Summary Metrics (GC Time), Executors tab (Total GC Time).

**What it looks like:**

```
Summary Metrics:
  Duration:  Min=2.1s   Median=8.5s   Max=45s
  GC Time:   Min=0.8s   Median=4.2s   Max=38s    <-- GC is ~50% of duration

Executors Tab:
  Executor  | Total Duration | Total GC Time | GC %
  exec-0    | 12.5 min       | 5.8 min       | 46%
  exec-1    | 11.2 min       | 5.1 min       | 45%
```

**Signature:** GC time is a significant fraction (>20%) of task duration across many or all tasks. Not isolated to one task (that would be skew), but widespread.

**Remediation:**
- Increase executor memory.
- Use fewer cores per executor (reduces concurrent tasks competing for the same heap).
- Use `MEMORY_ONLY_SER` or `OFF_HEAP` storage levels for cached data to reduce object count.
- Avoid collecting large datasets to the driver (`collect()`, `toPandas()` on large DataFrames).
- On Databricks, enable Photon -- it uses off-heap memory and is not subject to JVM GC.

> **Photon Note:** Photon-enabled operators avoid JVM garbage collection entirely because they operate in native (C++) memory. If GC pressure is a recurring problem, enabling Photon can eliminate it for the operators it supports. The SQL tab will show which operators are running on Photon (prefixed with `Photon`).

---

### Pattern: Under-Partitioned Job

**Where to look:** Stages tab (Tasks column), Task Timeline.

**What it looks like:**

```
Stages Tab:
  Stage 5 | Duration: 25 min | Tasks: 8/8 | Shuffle Read: 80 GB

  (8 tasks processing 80 GB = 10 GB per task, on a cluster with 64 cores)

Task Timeline (Stage 5):
  Exec-0: [====================================]  (10 GB, 25 min)
  Exec-1: [====================================]  (10 GB, 24 min)
  Exec-2: [====================================]  (10 GB, 25 min)
  ...
  Exec-7: [====================================]  (10 GB, 23 min)
  Exec-8:                                          (idle)
  Exec-9:                                          (idle)

Summary Metrics:
  Duration:  Min=23 min  Median=24.5 min  Max=25 min
  Input:     Min=9.8 GB  Median=10 GB     Max=10.2 GB

  (Uniform distribution -- not skew. Just too few partitions.)
```

**Signature:** Small number of tasks relative to available cores. All tasks take roughly the same long time (this is NOT skew -- it is uniform but there is just not enough parallelism). Many executors sit idle.

**Remediation:**
- Increase `spark.sql.shuffle.partitions` (for operations after a shuffle).
- Use `.repartition(n)` to explicitly increase partition count.
- For file-based reads, decrease `spark.sql.files.maxPartitionBytes` to create more partitions.
- Enable AQE coalescing, which can also *split* large partitions in newer versions.
- Rule of thumb: aim for 128 MB per partition and at least 2-3x as many partitions as total cores.

---

### Pattern: A Healthy Job

**Where to look:** All tabs should look "boring."

**What it looks like:**

```
Stages Tab:
  All stages completed. No failed stages.
  Duration is proportional to data volume.

Summary Metrics (for each stage):
  Duration:     Min=1.2s   Median=2.0s   Max=3.1s     <-- tight distribution
  GC Time:      Min=5ms    Median=20ms   Max=80ms      <-- < 5% of duration
  Shuffle Read: Min=4.5MB  Median=5.0MB  Max=5.8MB     <-- even distribution
  Spill:        All zeros                               <-- no spill

Task Timeline:
  Exec-0: [===][===][===][===][===]
  Exec-1: [===][===][===][===][===]
  Exec-2: [===][===][===][===][===]
  Exec-3: [===][===][===][===][===]
  (All executors equally busy, tasks equally sized)

SQL Tab:
  Row counts make sense at each operator.
  No unexpected data explosions.
  Exchange data sizes are reasonable.

Executors Tab:
  GC time < 5% for all executors.
  Shuffle read/write roughly even across executors.
  No failed tasks.
  All executors utilized.
```

**Signature:** Tight distributions in all metrics. No spill. Low GC. Even executor utilization. Stage durations proportional to data volume. A "boring" Spark UI is a healthy Spark UI.

---

## Quick Reference: Diagnostic Checklist

When investigating a slow Spark job, follow this sequence:

```
1. SQL Tab
   [ ] Does the query plan make sense?
   [ ] Are row counts reasonable at each operator?
   [ ] Are joins using the expected strategy (broadcast vs shuffle)?
   [ ] Which Exchange nodes move the most data?

2. Jobs Tab
   [ ] Which job is the slowest?
   [ ] Are there failed jobs?

3. Stages Tab (focus on the slowest stage)
   [ ] What is the min/median/max task duration spread?
   [ ] Is there shuffle spill?
   [ ] Is GC time a large fraction of task duration?
   [ ] How many tasks are there vs. available cores?

4. Task Timeline
   [ ] Are all executors busy?
   [ ] Is there a long-tail straggler?
   [ ] Are there scheduling gaps?

5. Executors Tab
   [ ] Is GC time even across executors?
   [ ] Is shuffle I/O even across executors?
   [ ] Are any executors blacklisted?

6. Environment Tab
   [ ] Are AQE settings correct?
   [ ] Is the broadcast threshold what you expect?
   [ ] Are memory settings applied?
```

> **Final Key Takeaway:** The Spark UI is not just a monitoring tool -- it is a diagnostic instrument. Every performance problem leaves a signature in the UI. Learn to read the patterns and you can diagnose most issues in minutes rather than hours. The sequence is always: SQL tab (understand the plan) then Stages tab (find the bottleneck) then Tasks/Timeline (identify the root cause).
