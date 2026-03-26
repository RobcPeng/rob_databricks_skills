---
name: databricks-free-tier-guardrails
description: >
  Use when generating any Databricks artifact (notebooks, scripts, pipelines, jobs) to verify
  it is compatible with Databricks free tier and serverless compute. Triggers on "free tier",
  "community edition", "serverless", "no cluster", "free Databricks", "trial account",
  "starter warehouse", or any code generation task where cluster type is unspecified.
  Apply AFTER other generation skills as a compatibility filter.
---

# Databricks Free Tier Guardrails

## Overview

Databricks free tier runs exclusively on **Serverless compute**, which uses **Spark Connect** instead of the classic Spark driver/executor model. This restricts a significant subset of PySpark APIs. Use this skill as a final compatibility pass on any generated artifact.

## What Free Tier Provides

| Feature | Available | Notes |
|---------|-----------|-------|
| Serverless compute (Spark Connect) | Yes | CPU only, no GPU |
| Unity Catalog | Yes | Limited to default catalog |
| Databricks SQL | Yes | One serverless SQL warehouse |
| Notebooks (Python, SQL, Markdown) | Yes | Standard notebooks |
| MLflow tracking | Yes | Basic experiment tracking |
| Volumes (file storage) | Yes | `/Volumes/...` paths |
| Spark Declarative Pipelines (SDP/DLT) | Yes | Serverless only |
| Jobs (scheduled workflows) | Yes | Serverless only |
| AI/BI Dashboards | Yes | Via SQL warehouse |
| Databricks Apps | Limited | Subject to compute limits |
| Model Serving | Limited | Pay-per-token endpoints |

## What Free Tier Does NOT Provide

| Feature | Status | Workaround |
|---------|--------|------------|
| Classic clusters (interactive or job) | Not available | Use serverless compute |
| GPU clusters | Not available | Use pay-per-token model serving |
| Custom Databricks Runtime | Not available | Use serverless defaults |
| Arbitrary `%pip install` (heavy) | Limited | Prefer pre-installed packages |
| Multiple SQL warehouses | Not available | Use Serverless Starter Warehouse |
| External network access | Limited | Download + upload datasets manually |
| Repos / Git integration | Limited | Use workspace files |
| Advanced security (IP ACLs, CMK) | Not available | N/A |
| Cluster pools | Not available | N/A |

## Compute Limits

| Resource | Limit |
|----------|-------|
| SQL warehouse | 1 per workspace, max 50 DBUs/hr |
| Serverless compute (notebooks, jobs, pipelines) | Capped at 50 DBUs/hr |
| Vector Search endpoints | 1, capped at 1 VS unit |
| GPU access | None |

---

## Banned APIs (will fail silently or raise errors)

| Banned Pattern | Why | Safe Alternative |
|----------------|-----|------------------|
| `spark.sparkContext` | SparkContext = RDD API, not Spark Connect | Remove or rewrite without it |
| `sc.*` | Same as above | Remove |
| `.rdd.*` | RDD operations not supported | Use DataFrame API |
| `df.cache()` / `df.persist()` | Cache APIs not supported on serverless | Remove; use a temp Delta table |
| `spark.sparkContext.setLogLevel(...)` | SparkContext access | Remove entirely |
| `spark.sparkContext.defaultParallelism` | SparkContext access | Use a hardcoded `partitions=` value |
| `spark.sparkContext.broadcast(...)` | SparkContext access | Use `F.broadcast(df)` hint instead |
| `spark.sparkContext.addFile(...)` | SparkContext access | Upload to Volume, read with `spark.read` |
| `spark.sparkContext.accumulator(...)` | SparkContext access | Use aggregation functions instead |
| `spark.sparkContext.textFile(...)` | SparkContext access | Use `spark.read.text(...)` |
| `spark.sparkContext.wholeTextFiles(...)` | SparkContext access | Use `spark.read.text(...)` |
| `df.foreach(...)` / `df.foreachPartition(...)` | RDD-based execution | Use `.write` or DataFrame ops |
| `df.toLocalIterator()` | Not supported in Spark Connect | Use `.collect()` on small data |
| `spark.createDataFrame(rdd)` | RDD input | Use list of Rows or pandas DataFrame |
| `spark.catalog.listDatabases()` | SparkContext-backed catalog API | Use `SHOW DATABASES` SQL |
| Scala / JVM UDFs | JVM not accessible via Spark Connect | Rewrite as Python UDFs or SQL expressions |
| `%sh` magic with system-level ops | Restricted shell access | Use Volume paths + `spark.sql` |
| `dbutils.fs.*` (some methods) | Limited on serverless | Use Volume paths instead |
| Custom `SparkConf` / `SparkSession.builder.config(...)` | Cannot customize Spark config | Use defaults |

### Banned API Detection Regex

Use these patterns to scan code for compatibility issues:

```
spark\.sparkContext
\bsc\.
\.rdd[.\[]
\.cache\(\)
\.persist\(
\.unpersist\(
setLogLevel
defaultParallelism
sparkContext\.broadcast
sparkContext\.addFile
sparkContext\.accumulator
sparkContext\.textFile
sparkContext\.wholeTextFiles
\.foreach\(
\.foreachPartition\(
\.toLocalIterator\(
createDataFrame\(.*rdd
dbutils\.fs\.
SparkConf\(
```

---

## Library Restrictions

Free tier supports a **fixed set of pre-installed libraries**. Treat every `%pip install` as a risk.

| Library | Available? | Notes |
|---------|------------|-------|
| `pyspark` | Pre-installed | Core, always available |
| `pandas` | Pre-installed | Available, but avoid for large data |
| `numpy` | Pre-installed | Always available |
| `mlflow` | Pre-installed | Always available |
| `scikit-learn` | Pre-installed | Available for ML |
| `matplotlib` / `seaborn` | Pre-installed | Available for visualization |
| `requests` | Pre-installed | HTTP client |
| `faker` | Requires `%pip install` | Works fine on serverless |
| `dbldatagen` | Requires `%pip install` | Pin `>=0.4.0` (older versions use `sparkContext`) |
| `geopandas` | Requires `%pip install` | Works but pulls heavy deps (GDAL) |
| `arcgis` | Requires `%pip install` | May fail due to system deps |
| `tensorflow` / `torch` | Requires `%pip install` | CPU only, very slow, not recommended |
| `holidays` | Requires `%pip install` | Works fine |
| `scipy` | Sometimes available | Don't rely on it being pre-installed |
| Custom wheels / private packages | Not supported | Requires classic cluster |
| Packages needing C compilation | Risky | May fail without system libraries |

**Rule:** If a library requires `%pip install`, flag it. Prefer native PySpark SQL functions (`rand()`, `randn()`, `expr()`, `sequence()`, `date_add()`, etc.) for data generation tasks.

### Safe Package Installation Pattern

```python
# Always use %pip (not pip or !pip) in notebooks
%pip install faker dbldatagen>=0.4.0

# In scripts executed via MCP, install first via execute_databricks_command:
# Tool: execute_databricks_command
# code: "%pip install faker dbldatagen>=0.4.0"
```

---

## Native PySpark SQL — Always Safe

These require zero external dependencies and work under Spark Connect:

```python
from pyspark.sql import functions as F
from pyspark.sql import Window

# Random numbers
F.rand(seed=42)          # uniform [0, 1)
F.randn(seed=42)         # standard normal N(0,1)

# Categorical assignment without loops
F.when(F.rand() < 0.2, "AAPL").when(F.rand() < 0.5, "GOOG").otherwise("MSFT")

# Expr for SQL-style expressions
F.expr("round(900 + randn() * 4.5, 4)")

# Date/time manipulation
F.date_add("col", 5)
F.to_timestamp("col", "yyyy-MM-dd HH:mm:ss")
F.unix_timestamp("col")
F.current_timestamp()
F.date_format("col", "yyyy-MM-dd")
F.months_between("end_date", "start_date")

# Sequences (use spark.range instead of Python range)
spark.range(100_000)     # distributed; never use Python range() + collect

# String functions
F.concat_ws(" ", "first", "last")
F.regexp_replace("col", "pattern", "replacement")
F.substring("col", 1, 5)

# Array and struct operations
F.array("col1", "col2", "col3")
F.struct("col1", "col2")
F.explode("array_col")
F.size("array_col")

# Window functions
window = Window.partitionBy("group").orderBy("date")
F.row_number().over(window)
F.lag("col", 1).over(window)
F.sum("amount").over(window)

# Type conversions
F.col("str_col").cast("int")
F.col("epoch").cast("timestamp")

# Geospatial (DBR 17.1+ serverless)
F.expr("ST_POINT(lon, lat)")
F.expr("H3_LONGLATASH3STRING(lon, lat, 9)")
F.expr("ST_DISTANCESPHERE(ST_POINT(a.lon, a.lat), ST_POINT(b.lon, b.lat))")
```

---

## Pipelines — No Cluster Configuration

Declarative pipelines (DLT/SDP) on free tier run on **serverless compute only**. Never add cluster configuration to pipeline resources or YAML.

### Banned pipeline config

```yaml
# Do NOT include any of these in pipeline.yml
clusters:
  - label: default
    node_type_id: i3.xlarge
    num_workers: 2
    spark_version: 15.4.x-scala2.12

cluster:
  spark_version: ...
  node_type_id: ...

configuration:
  spark.databricks.cluster.profile: serverless   # also banned -- implicit
```

### Correct serverless pipeline resource

```yaml
# Serverless — no cluster block needed
resources:
  pipelines:
    my_pipeline:
      name: "[${bundle.target}] my pipeline"
      catalog: ${var.catalog}
      target: ${var.schema}
      serverless: true       # explicit; omitting also defaults to serverless on free tier
      channel: PREVIEW
      libraries:
        - file:
            path: ../../src/pipelines/pipeline.py
```

**Rule:** If you see `clusters:`, `cluster:`, `node_type_id`, `spark_version`, or `num_workers` in a pipeline resource, remove them entirely and set `serverless: true`.

---

## Jobs — Serverless Only

Jobs on free tier must use serverless compute. Do not specify cluster config.

### Banned job config

```yaml
# Do NOT include these
resources:
  jobs:
    my_job:
      tasks:
        - task_key: etl
          new_cluster:                    # BANNED
            spark_version: 15.4.x
            node_type_id: i3.xlarge
            num_workers: 2
          existing_cluster_id: "abc-123"  # BANNED
```

### Correct serverless job resource

```yaml
# Serverless job — no cluster config
resources:
  jobs:
    my_job:
      name: "[${bundle.target}] my job"
      tasks:
        - task_key: etl
          notebook_task:
            notebook_path: ../src/notebooks/etl.py
          # No new_cluster, no existing_cluster_id
          # Serverless is used automatically
```

---

## Notebooks — Best Practices

### File paths

```python
# Good: Volume paths (always accessible)
df = spark.read.parquet("/Volumes/my_catalog/my_schema/my_volume/data.parquet")
df.write.parquet("/Volumes/my_catalog/my_schema/my_volume/output")

# Good: Unity Catalog tables
df = spark.table("my_catalog.my_schema.my_table")
df.write.saveAsTable("my_catalog.my_schema.output_table")

# Bad: Local filesystem (not reliable on serverless)
df = spark.read.parquet("/tmp/data.parquet")          # may not exist
df = spark.read.parquet("dbfs:/mnt/data.parquet")     # mounts not available

# Bad: DBFS mounts (not available on serverless)
dbutils.fs.mount(...)                                  # BANNED
spark.read.parquet("/mnt/my_mount/data")              # BANNED
```

### Notebook magics

```python
# Safe magics
%sql SELECT * FROM my_table LIMIT 10
%python print("hello")
%md ## My Markdown Header
%pip install faker

# Risky/banned magics
%sh ls /tmp          # limited shell access
%scala val x = 1     # Scala not supported on serverless
%r library(ggplot2)  # R not supported on serverless
```

---

## SQL Warehouse — Default Name

On the free tier, the only available SQL warehouse is:

```
Serverless Starter Warehouse
```

Use this exact name in all DAB `warehouse_id` lookups and dashboard resources:

```yaml
# Correct — free tier warehouse name
variables:
  warehouse_id:
    lookup:
      warehouse: Serverless Starter Warehouse
```

```yaml
# Do NOT use these — they do not exist on free tier
warehouse: Shared SQL Warehouse
warehouse: Starter Warehouse
warehouse: Pro Warehouse
warehouse: My Warehouse
```

---

## Unity Catalog — Free Tier Constraints

```python
# Default catalog only (usually "main" or "hive_metastore" depending on setup)
# You CAN create catalogs, schemas, and volumes within your permissions

spark.sql("CREATE CATALOG IF NOT EXISTS my_catalog")     # may work
spark.sql("CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema")
spark.sql("CREATE VOLUME IF NOT EXISTS my_catalog.my_schema.my_volume")

# External locations and storage credentials
# may be limited — use managed tables and volumes when possible
```

---

## MLflow — Free Tier Support

```python
import mlflow

# These work on free tier
mlflow.set_experiment("/Users/me/my_experiment")
mlflow.start_run()
mlflow.log_param("param", "value")
mlflow.log_metric("metric", 0.95)
mlflow.log_artifact("path/to/file")
mlflow.end_run()

# Model registry works
mlflow.register_model("runs:/run_id/model", "my_model")

# Tracking server is managed — no setup needed
# Unity Catalog model registry is available
```

---

## Geospatial on Free Tier

Spatial SQL functions work on serverless compute (DBR 17.1+):

```python
# ST_ functions — work on serverless
spark.sql("SELECT ST_POINT(-122.4, 37.8)")
spark.sql("SELECT ST_DISTANCESPHERE(ST_POINT(-122.4, 37.8), ST_POINT(-73.9, 40.7))")

# H3 functions — work on serverless
spark.sql("SELECT H3_LONGLATASH3STRING(-122.4, 37.8, 9)")

# Reading Overture Maps from S3 — works (public bucket)
df = spark.read.parquet("s3a://overturemaps-us-west-2/release/2025-01-22.0/theme=places/type=place")

# GeoPandas — requires pip install, may work for small datasets
%pip install geopandas
import geopandas as gpd  # CPU only, no system-level GDAL

# ArcGIS GeoAnalytics Engine — requires license + pip install
# May not work on free tier due to licensing and system dependencies
```

---

## Common Patterns to Fix

### Fix: replace `.cache()` with a Delta temp table

```python
# Not supported on serverless
df_customers = customers_spec.build().cache()

# Write to a temp Delta table, then read back
df_customers = customers_spec.build()
df_customers.write.mode("overwrite").saveAsTable("_tmp_customers")
df_customers = spark.table("_tmp_customers")
```

### Fix: replace sparkContext broadcast with DataFrame hint

```python
# SparkContext = RDD API
bc = spark.sparkContext.broadcast(lookup_dict)

# Broadcast hint on small DataFrame
small_df = spark.createDataFrame(lookup_list)
df.join(F.broadcast(small_df), on="key")
```

### Fix: remove setLogLevel

```python
# SparkContext access
spark.sparkContext.setLogLevel("WARN")

# Just remove it — log level is managed via workspace UI on serverless
```

### Fix: row generation without loops

```python
# Python loop + collect pattern (slow + breaks on serverless)
rows = [Row(id=i) for i in range(100_000)]
df = spark.createDataFrame(rows)

# Fully distributed
df = spark.range(100_000)
```

### Fix: replace dbutils.fs with Volume paths

```python
# dbutils.fs (limited on serverless)
dbutils.fs.ls("/mnt/data/")
dbutils.fs.cp("/mnt/data/file.csv", "/tmp/file.csv")

# Volume paths (always work)
files = spark.sql("LIST '/Volumes/catalog/schema/volume/'")
df = spark.read.csv("/Volumes/catalog/schema/volume/file.csv")
```

### Fix: replace RDD operations with DataFrame API

```python
# RDD operations (not supported)
rdd = df.rdd.map(lambda row: (row.key, row.value * 2))
result = rdd.reduceByKey(lambda a, b: a + b)

# DataFrame API (always works)
result = df.groupBy("key").agg(F.sum(F.col("value") * 2).alias("total"))
```

### Fix: replace .collect() on large data

```python
# Dangerous: pulls entire DataFrame to driver memory
all_rows = df.collect()
for row in all_rows:
    process(row)

# Safe: aggregate first, collect small result
summary = df.groupBy("category").agg(
    F.count("*").alias("count"),
    F.sum("amount").alias("total")
).collect()

# Safe: use .limit() before .collect()
sample = df.limit(100).collect()

# Safe: use .toPandas() on small aggregated results
pdf = df.groupBy("category").count().toPandas()
```

### Fix: replace createDataFrame(rdd) with list of Rows

```python
# Not supported: RDD input
rdd = sc.parallelize([(1, "a"), (2, "b")])
df = spark.createDataFrame(rdd, ["id", "name"])

# Supported: list of tuples or Rows
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

# Supported: pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
df = spark.createDataFrame(pdf)
```

---

## Full Checklist — Run Before Finalizing Any Artifact

Go line-by-line and confirm:

### PySpark Code
- [ ] No `spark.sparkContext` or `sc.` anywhere
- [ ] No `.cache()` or `.persist()` calls
- [ ] No `.rdd.` chained operations
- [ ] No `setLogLevel`
- [ ] No `defaultParallelism`
- [ ] No `sparkContext.broadcast()` (use `F.broadcast()` hint)
- [ ] No `sparkContext.addFile()`, `accumulator()`, `textFile()`
- [ ] No `.foreach()` or `.foreachPartition()`
- [ ] No `.toLocalIterator()`
- [ ] No Python `for` / `while` loops generating rows (use `spark.range()`)
- [ ] No `.collect()` on large DataFrames (only acceptable for single-value scalars)
- [ ] No `pandas` for row generation (use DataFrame API)
- [ ] No `dbutils.fs.mount()` or `/mnt/` paths
- [ ] No custom `SparkConf` or `SparkSession.builder.config()`
- [ ] All `%pip install` packages flagged and reviewed

### File Paths
- [ ] Write targets are Volumes (`/Volumes/...`) or Delta tables — not local filesystem paths
- [ ] No `/mnt/` mount paths (not available on serverless)
- [ ] No `/dbfs/` paths for critical data (use Volumes instead)
- [ ] No `/tmp/` paths for persistent data (ephemeral on serverless)

### DataGenerator (dbldatagen)
- [ ] `partitions=` set explicitly (avoids `defaultParallelism` call)
- [ ] Using `dbldatagen>=0.4.0` (older versions may use `sparkContext`)
- [ ] No `.cache()` on built DataFrames (use temp table pattern)

### Pipeline Resources (YAML)
- [ ] No `clusters:` / `cluster:` block in any pipeline resource
- [ ] No `node_type_id`, `num_workers`, or `spark_version` in pipeline YAML
- [ ] Pipeline resources have `serverless: true`

### Job Resources (YAML)
- [ ] No `new_cluster:` block in job tasks
- [ ] No `existing_cluster_id:` in job tasks
- [ ] No `spark_version` or `node_type_id` in job config

### SQL Warehouse
- [ ] Any `warehouse_id` lookup uses name `Serverless Starter Warehouse`
- [ ] No references to custom warehouses, Pro warehouses, or Classic warehouses

### Notebooks
- [ ] No `%sh` with system-level operations
- [ ] No `%scala` cells (not supported on serverless)
- [ ] No `%r` cells (not supported on serverless)

---

## Quick Reference: Safe vs. Banned

```
┌──────────────────────────────────────────────────────────────────┐
│  SAFE (Spark Connect)              │  BANNED (Classic only)     │
│────────────────────────────────────│────────────────────────────│
│  spark.sql("...")                  │  spark.sparkContext        │
│  spark.read.parquet/csv/json       │  sc.*                     │
│  spark.table("catalog.schema.t")  │  df.rdd.*                 │
│  spark.range(N)                   │  df.cache() / .persist()   │
│  spark.createDataFrame(list)      │  df.foreach()             │
│  df.filter / .select / .join      │  sparkContext.broadcast()  │
│  df.groupBy / .agg / .orderBy     │  sparkContext.textFile()   │
│  df.write.saveAsTable / .parquet  │  setLogLevel()             │
│  F.broadcast(df) hint             │  dbutils.fs.mount()        │
│  F.expr("SQL expression")        │  /mnt/ paths               │
│  F.rand() / F.randn()            │  SparkConf()               │
│  Window functions                 │  createDataFrame(rdd)      │
│  ST_* / H3_* geospatial          │  %sh system commands       │
│  /Volumes/... paths               │  %scala / %r               │
│  %pip install <package>           │  new_cluster: in YAML      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Related Skills

- **dbldatagen** — use for large-scale generation; always set `partitions=` explicitly and pin `dbldatagen>=0.4.0`
- **databricks-synthetic-data-generation** — Faker-based; flag all pip installs before use on free tier
- **databricks-spark-declarative-pipelines** — pipelines run on serverless by default; same restrictions apply
- **databricks-geospatial** — ST_ and H3 functions work on serverless; flag geopandas/arcgis pip installs
- **databricks-jobs** — job tasks must omit cluster config on free tier
- **databricks-bundles** — pipeline/job resources must use serverless patterns
