# Databricks Diagnostics for Spark Job Optimization

This reference covers the full diagnostic toolkit available on Databricks: cluster metrics, Ganglia, event logs, Photon internals, SQL warehouse diagnostics, system tables, configuration tuning, and cost optimization.

---

## 1. Databricks Cluster Metrics

### Available Metrics in the Cluster UI

The **Metrics** tab on a running cluster exposes real-time and historical data across four dimensions:

| Category | Key Metrics | What to Watch |
|----------|-------------|---------------|
| **CPU** | Utilization %, system vs user time | Sustained >85% = CPU-bound; <20% = over-provisioned or I/O-blocked |
| **Memory** | JVM heap used/committed, off-heap, RSS | Heap approaching max = GC pressure risk |
| **Network** | Bytes sent/received per node | Spikes during shuffle; sustained high = data-intensive shuffle |
| **Disk** | Read/write throughput, IOPS | High disk wait = spill-to-disk or slow storage |

### Interpreting Utilization Patterns

**Under-provisioned cluster signals:**
- CPU consistently above 90% on all nodes
- Frequent GC pauses (visible as CPU dips followed by spikes)
- Disk write spikes during shuffle (spill to disk)
- Tasks queuing — more pending tasks than available cores

**Over-provisioned cluster signals:**
- CPU below 20% across nodes for most of the job
- Memory utilization under 30%
- Many idle executors (check Spark UI Executors tab)
- Autoscaling repeatedly removing nodes

**Healthy cluster pattern:**
- CPU between 50–80% across nodes
- Memory usage 50–70% of heap
- Network I/O correlates with shuffle stages, not constant
- Disk I/O minimal outside of read/write stages

> **Key takeaway:** A well-tuned job shows *bursty* resource usage aligned with stage transitions — not flat-line high or flat-line low. If every metric is pegged at 100%, you need more resources. If everything is idle, you need fewer resources or have a bottleneck elsewhere (e.g., a single-partition stage).

---

## 2. Ganglia Metrics

### What Ganglia Shows on Databricks

Ganglia is available on Databricks clusters via the **Metrics** tab → **Ganglia UI** link. It provides cluster-wide and per-node views of system-level metrics.

### Key Ganglia Dashboards

**CPU Utilization Across Nodes:**
- The grid view shows each node's CPU as a colored tile
- Uniform color = good load distribution
- One hot node + cold nodes = data skew or driver bottleneck
- All nodes cycling between hot/cold = stage boundaries

**Memory Usage:**
- `mem_free` vs `mem_total` — watch for nodes approaching zero free memory
- `mem_cached` — OS page cache; high cache is normal and beneficial for repeated reads
- `swap_used` — any swap usage on a Spark cluster is a red flag; Spark should never swap

**Network I/O:**
- `bytes_in` / `bytes_out` — correlate with shuffle stages
- Asymmetric network (one node sending much more) = skewed partitions
- Sustained high network = large shuffle; consider reducing shuffle by repartitioning upstream

**Disk I/O:**
- `disk_read` / `disk_write` on local SSDs
- High disk write during compute stages = spill to disk (not enough memory for shuffle)
- High disk read at stage start = reading shuffle files or cached data

### Common Patterns

| Pattern | Ganglia Signature | Root Cause |
|---------|-------------------|------------|
| **CPU-bound** | CPU 100%, memory/disk/network low | Complex transformations, UDFs, insufficient parallelism |
| **I/O-bound** | CPU low, disk or network high | Large shuffles, reading from slow storage, spill |
| **Memory-bound** | CPU has GC spikes, memory at max | Too much data cached, large broadcast variables, skewed partitions |
| **Skew** | One node hot, others idle | Uneven partition sizes — one task has 100x the data |
| **Straggler** | Most nodes finish, one lags | Slow node (spot instance degradation), skewed task, or speculative execution needed |

> **Key takeaway:** Ganglia's most valuable view is the per-node grid. Uniform coloring means balanced work. Patchwork coloring means something is uneven — investigate partition distribution and task durations in the Spark UI.

---

## 3. Spark Event Logs

### Where Event Logs Are Stored

On Databricks, Spark event logs are written to the cluster log delivery location configured in your workspace. Common paths:

```
dbfs:/cluster-logs/<cluster-id>/eventlog/
s3://your-bucket/databricks/cluster-logs/<cluster-id>/eventlog/
abfss://container@account.dfs.core.windows.net/cluster-logs/<cluster-id>/eventlog/
gs://your-bucket/databricks/cluster-logs/<cluster-id>/eventlog/
```

To configure log delivery:

```
Workspace Settings → Admin Console → Cluster Log Delivery → Set destination path
```

Event logs can also be accessed through:
- **Spark UI** → Environment tab shows the path under `spark.eventLog.dir`
- **Cluster details** → Logs tab → Spark Driver logs and Event logs

### Key Events in Event Logs

Event logs are newline-delimited JSON files. Each line is a Spark listener event.

| Event | What It Tells You |
|-------|-------------------|
| `SparkListenerJobStart` | Job ID, submission time, stage IDs in the job |
| `SparkListenerJobEnd` | Job ID, completion time, result (success/failure) |
| `SparkListenerStageSubmitted` | Stage ID, number of tasks, RDD info, parent stages |
| `SparkListenerStageCompleted` | Stage duration, input/output/shuffle read/write bytes, task count, failure info |
| `SparkListenerTaskEnd` | Per-task metrics: duration, GC time, shuffle bytes, spill bytes, peak memory, executor ID |
| `SparkListenerBlockManagerAdded/Removed` | Executor memory allocation changes |
| `SparkListenerEnvironmentUpdate` | All Spark configs at job start (useful for auditing) |
| `org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart` | SQL query text, physical plan, submission time |
| `org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd` | SQL query completion time |

### Extracting Metrics from Event Logs

Read event logs as JSON for post-mortem analysis:

```python
# Read event logs into a DataFrame for analysis
event_log_path = "dbfs:/cluster-logs/<cluster-id>/eventlog/"
events = spark.read.json(event_log_path)

# Find the slowest stages
from pyspark.sql.functions import col, from_json, explode

stage_events = events.filter(col("Event") == "SparkListenerStageCompleted")

stage_events.select(
    col("Stage Info.Stage ID").alias("stage_id"),
    col("Stage Info.Stage Name").alias("stage_name"),
    col("Stage Info.Number of Tasks").alias("num_tasks"),
    col("Stage Info.Submission Time").alias("start_ms"),
    col("Stage Info.Completion Time").alias("end_ms"),
    (col("Stage Info.Completion Time") - col("Stage Info.Submission Time")).alias("duration_ms")
).orderBy(col("duration_ms").desc()).show(10, truncate=False)
```

```python
# Find tasks with excessive GC time or spill
task_events = events.filter(col("Event") == "SparkListenerTaskEnd")

task_events.select(
    col("Stage ID").alias("stage_id"),
    col("Task Info.Task ID").alias("task_id"),
    col("Task Info.Host").alias("host"),
    col("Task Metrics.Executor Run Time").alias("run_time_ms"),
    col("Task Metrics.JVM GC Time").alias("gc_time_ms"),
    col("Task Metrics.Memory Bytes Spilled").alias("mem_spill"),
    col("Task Metrics.Disk Bytes Spilled").alias("disk_spill"),
    col("Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written").alias("shuffle_write"),
    col("Task Metrics.Shuffle Read Metrics.Remote Bytes Read").alias("shuffle_remote_read")
).filter(
    (col("Task Metrics.JVM GC Time") > 5000) |
    (col("Task Metrics.Disk Bytes Spilled") > 0)
).orderBy(col("Task Metrics.Executor Run Time").desc()).show(20)
```

> **Key takeaway:** Event logs are the ground truth for post-mortem debugging. When the Spark UI is no longer available (cluster terminated), event logs are your only source of task-level detail. Always configure cluster log delivery.

---

## 4. Cluster Event Logs

### What Cluster Events Capture

Cluster event logs are separate from Spark event logs. They record infrastructure-level events. Access them via:

```
Cluster details → Event Log tab
```

Or via the Clusters API:

```bash
# List events for a cluster
curl -X GET "https://<workspace-url>/api/2.0/clusters/events" \
  -H "Authorization: Bearer <token>" \
  -d '{"cluster_id": "<cluster-id>", "limit": 50}'
```

### Key Event Types

| Event Type | Meaning | Impact |
|------------|---------|--------|
| `AUTOSCALING` | Workers added or removed | Ramp-up delay for new nodes; scale-down may strand cached data |
| `NODES_LOST` | Spot/preemptible instances reclaimed | Tasks on lost nodes must be re-executed; shuffle data lost |
| `DRIVER_HEALTHY` / `DRIVER_NOT_RESPONDING` | Driver health check | Driver OOM or unresponsive = job failure risk |
| `INIT_SCRIPTS_FINISHED` | Init script completed (or failed) | Failed init = node unusable; watch for library install failures |
| `SPARK_EXCEPTION` | Unhandled Spark exception | Often OOM; check exception type |
| `DBFS_DOWN` | DBFS unavailable | All reads/writes to DBFS will fail |
| `METASTORE_DOWN` | Metastore unreachable | Catalog operations and table reads fail |
| `CREATING` → `RUNNING` → `TERMINATING` | Cluster lifecycle | Long CREATING time = cloud API throttling or capacity issues |

### Correlating Cluster Events with Job Performance

Common diagnostic correlations:

**Scenario: Job suddenly got slower at 2:15 PM**
1. Check cluster events around 2:15 PM → Found `NODES_LOST` — 3 spot instances reclaimed
2. Fewer workers = less parallelism = slower stages
3. Autoscaling event 5 min later = new nodes requested but not yet available

**Scenario: Job failed with OOM**
1. Check cluster events → `DRIVER_NOT_RESPONDING` followed by `SPARK_EXCEPTION`
2. Look at event details for memory info
3. Correlate with Spark event log — which stage was running? How much shuffle data?

**Scenario: Job slower than yesterday**
1. Compare cluster events between two runs
2. Check for autoscaling differences — did today's run get fewer nodes?
3. Check for `NODES_LOST` events — spot instance interruptions are non-deterministic

> **Key takeaway:** When a job's performance changes between runs with the same code and data, the answer is almost always in the cluster event logs — spot losses, autoscaling behavior, or init script failures.

---

## 5. Photon Engine Deep Dive

### What Photon Is

Photon is Databricks' native C++ vectorized query engine. It replaces portions of the JVM-based Spark SQL execution engine with compiled native code. Photon is available on Photon-enabled cluster types and is the default on DBR 14.x+ Photon runtimes.

### When Photon Is Used vs Falls Back to JVM

Photon handles a growing subset of Spark SQL operations natively. It falls back to JVM execution for unsupported operations transparently — individual operators in a query plan can be either Photon or JVM.

**Operations Photon accelerates well:**
- Scans (Parquet, Delta — shown as `PhotonScan` in plans)
- Filters and projections
- Hash aggregations
- Hash joins (inner, left, right, semi, anti)
- Sort and sort-merge joins
- Shuffle exchanges
- Window functions (common ones)
- String operations (like, substring, regex)
- Common math and date functions

**Operations that fall back to JVM:**
- Python/Scala UDFs (always JVM; Photon cannot execute arbitrary user code)
- Complex types with deeply nested schemas
- Unsupported data types or functions (check release notes per DBR version)
- Streaming micro-batches (partial Photon support as of DBR 14.x)
- Some advanced window functions
- Certain file formats (non-Parquet, non-Delta readers)

### How to Tell If Photon Is Active

**In the query plan (SQL tab → query → Details):**
- Look for operator names prefixed with `Photon`: `PhotonScan`, `PhotonFilter`, `PhotonHashAggregate`, `PhotonShuffleExchangeExec`
- JVM fallback operators show standard names: `Scan`, `Filter`, `HashAggregate`, `ShuffleExchangeExec`
- A single query can have a mix of Photon and JVM operators

**In the Spark UI:**
- Photon-executed stages show Photon-specific metrics (Photon rows processed, Photon time)
- The SQL plan visualization color-codes Photon operators differently

**Programmatically:**

```python
# Check if Photon is enabled
spark.conf.get("spark.databricks.photon.enabled")
# Returns "true" if Photon is on

# Inspect a query plan for Photon usage
df = spark.read.table("my_table").filter("col_a > 100").groupBy("col_b").count()
df.explain(True)
# Look for "Photon" in the physical plan output
```

### Photon-Specific Performance Characteristics

#### Vectorized Execution Benefits

Photon processes data in columnar batches (vectors) rather than row-by-row. This enables:
- CPU cache efficiency — columnar data fits L1/L2 caches better
- SIMD instruction utilization on modern CPUs
- Reduced JVM overhead — no object creation per row, no GC pressure
- Typically 2–5x faster for scan/filter/aggregate workloads

#### Native Shuffle vs JVM Shuffle

Photon implements its own shuffle writer and reader in C++:
- Serialization is faster (no Java serialization overhead)
- Compression is done natively (faster LZ4/ZSTD)
- Memory management is off-heap, avoiding GC pauses during shuffle spill
- Shuffle spill to disk is more efficient with native I/O

#### Memory Management Differences

| Aspect | JVM Spark | Photon |
|--------|-----------|--------|
| Heap management | JVM garbage collector | Native C++ allocator (off-heap) |
| GC pauses | Yes — can be significant | No GC pauses |
| Memory tracking | Approximate (Spark memory manager) | Precise (native tracking) |
| Spill strategy | Spill to disk when memory fraction exceeded | Spill to disk with native memory pressure tracking |
| OOM behavior | `java.lang.OutOfMemoryError` | Photon-specific OOM with memory usage details |

#### When Photon Is Faster

- Large scans with filters — Photon's vectorized scan + predicate pushdown is its sweet spot
- Aggregations on large datasets — hash aggregation in native code with no GC
- Joins with reasonable key cardinality — hash join in Photon avoids JVM overhead
- String-heavy workloads — Photon's native string processing is much faster than JVM
- Shuffle-heavy workloads — native shuffle reduces serialization time

#### When Photon Doesn't Help

- UDF-heavy workloads — UDFs always run in JVM; data must cross the Photon/JVM boundary
- Very small datasets — overhead of Photon initialization outweighs benefits
- Workloads already bottlenecked on I/O (cloud storage latency) — Photon can't make S3 faster
- Unsupported operations — fallback to JVM means no benefit for those operators

#### Photon-Specific Configs

```python
# Enable/disable Photon
spark.conf.set("spark.databricks.photon.enabled", "true")

# Photon memory allocation (fraction of total off-heap memory)
# Default varies by DBR version; typically 0.5
spark.conf.set("spark.databricks.photon.memoryFraction", "0.6")

# Photon shuffle compression
spark.conf.set("spark.databricks.photon.shuffleCompression.enabled", "true")

# Force Photon for all supported operators (disable selective fallback)
spark.conf.set("spark.databricks.photon.allDataSources.enabled", "true")

# Photon scan thread count (per executor)
# Higher values = more parallelism for scan-heavy workloads
spark.conf.set("spark.databricks.photon.scan.threads", "4")
```

> **Key takeaway:** Photon gives the biggest gains on scan → filter → aggregate → join pipelines over large Delta/Parquet tables. If your query plan shows most operators prefixed with `Photon`, you are benefiting. If you see frequent JVM fallbacks, check whether UDFs or unsupported operations can be rewritten using built-in SQL functions.

---

## 6. Databricks SQL Warehouse Diagnostics

### Query History

DBSQL warehouses maintain a query history accessible via:

```
SQL Warehouses → <warehouse> → Query History
```

Each query entry shows:
- **Status**: succeeded, failed, canceled
- **Duration**: wall-clock time
- **Rows produced**: result set size
- **User**: who submitted the query
- **Warehouse**: which warehouse ran it
- **Timestamp**: submission and completion time

Filter and sort by duration to find slow queries. Filter by status to find failures.

### Query Profiles

The **Query Profile** (available per query in query history) is the DBSQL equivalent of the Spark SQL tab, but with a more visual, analyst-friendly presentation:

- **Tree view**: shows operators as a tree with time spent per operator
- **Graph view**: shows data flow between operators with row counts on edges
- **Metrics per operator**: rows processed, bytes scanned, time spent
- **Photon indicators**: shows which operators ran in Photon vs JVM

### How DBSQL Diagnostics Differ from Notebook Spark UI

| Feature | Notebook Spark UI | DBSQL Query Profile |
|---------|-------------------|---------------------|
| Access | Live cluster Spark UI link | Query history, persists after warehouse stops |
| Granularity | Job → Stage → Task | Operator → metrics (more SQL-oriented) |
| Physical plan | Text-based `explain()` | Visual tree/graph |
| Historical queries | Lost when cluster terminates (unless event logs configured) | Retained in query history for 30 days |
| Multi-statement | Each action is a separate job | Each SQL statement is a separate query entry |
| Photon visibility | Check operator names in plan | Visual indicator per operator |

### DBSQL-Specific Diagnostics

```sql
-- Check warehouse query history via system tables (if enabled)
SELECT
  statement_id,
  executed_by,
  statement_text,
  status,
  total_duration_ms,
  rows_produced,
  start_time,
  end_time
FROM system.query.history
WHERE warehouse_id = '<warehouse-id>'
  AND start_time > current_timestamp() - INTERVAL 24 HOURS
ORDER BY total_duration_ms DESC
LIMIT 20;
```

> **Key takeaway:** DBSQL query profiles are more persistent and user-friendly than the Spark UI. For production SQL workloads, prefer DBSQL warehouses partly because their diagnostic tooling is better for ongoing monitoring and troubleshooting.

---

## 7. System Tables for Performance

Databricks system tables (available in Unity Catalog) provide historical metadata for performance analysis and cost attribution.

### Compute Clusters Table

```sql
-- Historical cluster configurations and usage
SELECT
  cluster_id,
  cluster_name,
  cluster_source,
  driver_node_type,
  node_type_id,
  autoscale_min_workers,
  autoscale_max_workers,
  num_workers,
  spark_version,
  custom_tags,
  start_time,
  delete_time
FROM system.compute.clusters
WHERE start_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY start_time DESC;
```

### Billing Usage Table

```sql
-- Cost by cluster over the past 30 days
SELECT
  usage_metadata.cluster_id,
  sku_name,
  SUM(usage_quantity) AS total_dbus,
  COUNT(DISTINCT usage_date) AS active_days
FROM system.billing.usage
WHERE usage_date > current_date() - INTERVAL 30 DAYS
  AND usage_metadata.cluster_id IS NOT NULL
GROUP BY usage_metadata.cluster_id, sku_name
ORDER BY total_dbus DESC
LIMIT 20;
```

```sql
-- Daily cost trend for a specific cluster
SELECT
  usage_date,
  sku_name,
  SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_metadata.cluster_id = '<cluster-id>'
  AND usage_date > current_date() - INTERVAL 30 DAYS
GROUP BY usage_date, sku_name
ORDER BY usage_date;
```

```sql
-- Cost per job (using job_id tag if set)
SELECT
  usage_metadata.job_id,
  SUM(usage_quantity) AS total_dbus,
  MIN(usage_date) AS first_run,
  MAX(usage_date) AS last_run,
  COUNT(DISTINCT usage_date) AS run_days
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL
  AND usage_date > current_date() - INTERVAL 30 DAYS
GROUP BY usage_metadata.job_id
ORDER BY total_dbus DESC
LIMIT 20;
```

### Query History System Table

```sql
-- Slowest queries across all warehouses in the past 7 days
SELECT
  statement_id,
  executed_by,
  warehouse_id,
  total_duration_ms,
  rows_produced,
  statement_text,
  start_time
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 7 DAYS
  AND status = 'FINISHED'
ORDER BY total_duration_ms DESC
LIMIT 50;
```

```sql
-- Query volume and latency by warehouse
SELECT
  warehouse_id,
  COUNT(*) AS query_count,
  AVG(total_duration_ms) AS avg_duration_ms,
  PERCENTILE_APPROX(total_duration_ms, 0.95) AS p95_duration_ms,
  MAX(total_duration_ms) AS max_duration_ms
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 7 DAYS
  AND status = 'FINISHED'
GROUP BY warehouse_id
ORDER BY query_count DESC;
```

> **Key takeaway:** System tables are the foundation for cost attribution and historical trend analysis. Query `system.billing.usage` weekly to catch cost regressions before they compound. Join with `system.compute.clusters` to map costs to cluster configurations.

---

## 8. Spark Configurations on Databricks

### Databricks Defaults That Differ from OSS Spark

Databricks Runtime (DBR) ships with tuned defaults that differ from open-source Apache Spark. Understanding these prevents misconfiguration.

| Config | OSS Spark Default | Databricks Default | Notes |
|--------|-------------------|-------------------|-------|
| `spark.sql.adaptive.enabled` | `false` (pre-3.2), `true` (3.2+) | `true` | AQE is always on in DBR |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Auto-coalesces shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Splits skewed partitions during joins |
| `spark.sql.shuffle.partitions` | `200` | `200` (but AQE adjusts dynamically) | AQE makes this less critical on Databricks |
| `spark.sql.files.maxPartitionBytes` | `128MB` | `128MB` | Controls scan partition size |
| `spark.databricks.delta.optimizeWrite.enabled` | N/A | `true` (on some DBR versions) | Coalesces small files on write |
| `spark.databricks.delta.autoCompact.enabled` | N/A | `false` (can be enabled) | Runs OPTIMIZE after writes |
| `spark.sql.broadcastTimeout` | `300s` | `300s` | Timeout for broadcast joins |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | `10MB` | Tables below this threshold are broadcast |
| `spark.databricks.io.cache.enabled` | N/A | `true` (on supported instances) | Delta cache / disk cache for remote data |

### Delta-Specific Configs

```python
# Optimize write — coalesces small output files automatically
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Auto-compact — runs lightweight OPTIMIZE after every write
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.minNumFiles", "50")

# Target file size for OPTIMIZE
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128MB

# Z-order on write (preview in some DBR versions)
# Generally use explicit OPTIMIZE ZORDER BY instead

# Delta caching
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")

# Data skipping — enabled by default, stats collected on first 32 columns
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "32")

# Deletion vectors (DBR 14.x+)
# Enabled by default for new tables in Unity Catalog
spark.conf.set("spark.databricks.delta.delete.deletionVectors.persistent", "true")
```

### DBR Version Considerations

| DBR Version | Key Changes for Performance |
|-------------|----------------------------|
| **DBR 12.x** | Photon GA, AQE improvements, Delta 2.2 |
| **DBR 13.x** | Photon shuffle improvements, predictive I/O, improved autotune |
| **DBR 14.x** | Deletion vectors default-on, liquid clustering GA, Photon streaming support |
| **DBR 15.x** | Improved Photon coverage, serverless compute foundations, UniForm GA |

```python
# Check your DBR version
spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# List all Databricks-specific configs and their current values
for k, v in sorted(spark.sparkContext.getConf().getAll()):
    if "databricks" in k.lower():
        print(f"{k} = {v}")
```

> **Key takeaway:** Don't blindly apply OSS Spark tuning guides on Databricks. AQE, Photon, Delta caching, and optimize-write change the performance equation. Check what DBR has already set before overriding. In particular, `spark.sql.shuffle.partitions = 200` is rarely a problem on Databricks because AQE dynamically adjusts the actual number of post-shuffle partitions.

---

## 9. Cost Optimization

### Right-Sizing Instances

Databricks instance types fall into categories. Choosing the right family avoids paying for resources you don't use.

| Workload Profile | Recommended Instance Family | AWS Example | Azure Example | Why |
|------------------|-----------------------------|-------------|---------------|-----|
| **CPU-bound** (complex transforms, UDFs, ML training) | Compute-optimized | `c5.4xlarge` | `Standard_F16s_v2` | High CPU-to-memory ratio; don't pay for unused RAM |
| **Shuffle/join heavy** (large joins, aggregations, wide tables) | Memory-optimized | `r5.4xlarge` | `Standard_E16s_v3` | More memory = less spill = faster shuffles |
| **Scan-heavy** (large reads, ETL pipelines) | Storage-optimized with local SSDs | `i3.4xlarge` | `Standard_L16s_v2` | Fast local disk for Delta cache; reduces cloud storage reads |
| **General/mixed** | General-purpose | `m5.4xlarge` | `Standard_D16s_v3` | Balanced; good default when unsure |
| **ML/Deep Learning** | GPU instances | `p3.2xlarge` | `Standard_NC6s_v3` | GPU for training; overkill for pure Spark SQL |

**How to identify the right family:**
1. Run your job on general-purpose instances
2. Check Ganglia — which resource is the bottleneck?
3. Switch to the family that has more of that resource

### Spot vs On-Demand Tradeoffs

| Factor | Spot/Preemptible | On-Demand |
|--------|------------------|-----------|
| **Cost** | 60–90% cheaper | Full price |
| **Availability** | Can be reclaimed with 2-min warning | Always available |
| **Best for** | Workers in fault-tolerant jobs, dev/test | Drivers (always), production SLA-critical workers |
| **Risk** | Lost nodes → recomputation, lost shuffle data | None |
| **Mitigation** | Use spot for workers only; enable graceful decommission; use Delta cache (persists across recomputation) | N/A |

**Recommended pattern for production jobs:**

```json
{
  "driver": { "instance_type": "m5.2xlarge", "on_demand": true },
  "workers": {
    "instance_type": "r5.4xlarge",
    "spot": true,
    "spot_fallback": "on_demand",
    "min_workers": 2,
    "max_workers": 10
  }
}
```

- Driver: always on-demand (driver loss = complete job failure)
- Workers: spot with on-demand fallback
- Set a minimum worker count on on-demand to guarantee baseline capacity

### Autoscaling Configuration

```python
# Cluster config — autoscaling settings
{
    "autoscale": {
        "min_workers": 2,    # Always have at least this many workers
        "max_workers": 16    # Scale up to this during peak stages
    },
    "spark_conf": {
        # How aggressively to scale down
        "spark.databricks.aggressiveWindowDownS": "120",
        # Don't scale down during active shuffle
        "spark.databricks.isv.autoscale.safeguard.enabled": "true"
    }
}
```

**Autoscaling tuning tips:**
- **Min workers**: set to what your job needs during its lightest stage. Too high = wasted cost during scan phases. Too low = slow ramp-up.
- **Max workers**: set to what the heaviest shuffle/join stage needs. Check task count — max workers should roughly equal `num_tasks / cores_per_worker` for the biggest stage.
- **Scale-down aggressiveness**: default is conservative (waits before removing nodes). For batch jobs with distinct phases, a more aggressive scale-down saves money between stages.

### DBU Cost Awareness

DBUs (Databricks Units) are the billing unit. Different workload types have different DBU rates.

| Workload Type | DBU Multiplier | Notes |
|---------------|---------------|-------|
| All-Purpose Compute | Highest | Interactive clusters; use only for development |
| Jobs Compute | Lower (~30–50% of all-purpose) | Automated workflows; always use for production |
| SQL Warehouse (Serverless) | Per-query pricing | Good for ad-hoc SQL; auto-scales to zero |
| SQL Warehouse (Classic/Pro) | Per-DBU while running | More predictable; good for sustained workloads |
| Delta Live Tables | DLT-specific pricing | Higher than Jobs Compute but includes orchestration |

**Cost reduction checklist:**

- [ ] Use Jobs Compute (not All-Purpose) for production workloads
- [ ] Use spot instances for workers
- [ ] Enable autoscaling with appropriate min/max
- [ ] Right-size instance types based on resource utilization
- [ ] Use Photon runtime (higher DBU rate but typically lower total cost due to faster execution)
- [ ] Enable Delta caching on storage-optimized instances to reduce cloud I/O costs
- [ ] Set cluster auto-termination for interactive clusters (e.g., 30 minutes)
- [ ] Use `system.billing.usage` to track cost trends weekly
- [ ] Tag clusters and jobs with cost center tags for attribution

```sql
-- Estimate monthly cost savings from switching all-purpose to jobs compute
-- (Requires knowing your DBU list price)
WITH usage AS (
  SELECT
    usage_metadata.cluster_id,
    sku_name,
    SUM(usage_quantity) AS dbus_30d
  FROM system.billing.usage
  WHERE usage_date > current_date() - INTERVAL 30 DAYS
    AND sku_name LIKE '%ALL_PURPOSE%'
  GROUP BY usage_metadata.cluster_id, sku_name
)
SELECT
  cluster_id,
  sku_name,
  dbus_30d,
  dbus_30d * 0.40 AS estimated_savings_at_40pct_reduction
FROM usage
ORDER BY dbus_30d DESC;
```

> **Key takeaway:** The single biggest cost lever is workload type — switching from All-Purpose to Jobs Compute can save 30–50% immediately. The second biggest lever is spot instances for workers. Only after those two are in place should you focus on instance sizing and autoscaling tuning.

---

## Quick Reference: Diagnostic Decision Tree

```
Job is slow or failed
│
├── Check Spark UI → SQL tab
│   ├── One stage much slower? → Skew or spill (see task metrics)
│   ├── Many stages slow? → Under-provisioned or bad plan
│   └── Shuffle write huge? → Reduce shuffle (repartition, broadcast join)
│
├── Check Cluster Event Log
│   ├── NODES_LOST? → Spot instance reclaimed; use fallback
│   ├── DRIVER_NOT_RESPONDING? → Driver OOM; increase driver memory
│   └── AUTOSCALING delays? → Set higher min_workers
│
├── Check Ganglia
│   ├── CPU 100% everywhere? → Add cores or optimize transforms
│   ├── One node hot? → Data skew
│   ├── High disk I/O? → Spill to disk; add memory
│   └── Memory at max? → Increase executor memory or reduce cache
│
├── Check Query Plan (for SQL workloads)
│   ├── BroadcastHashJoin missing? → Table too large; check stats
│   ├── Photon operators absent? → UDFs or unsupported ops; rewrite
│   └── CartesianProduct? → Missing join condition; fix query
│
└── Check System Tables
    ├── Cost spike? → system.billing.usage; find the cluster
    ├── Cluster config changed? → system.compute.clusters; compare
    └── Query regression? → system.query.history; compare durations
```
