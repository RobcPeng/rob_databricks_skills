# Resource Configuration — Free Tier

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

## AI Functions — Free Tier Support

AI Functions that call foundation models via pay-per-token are confirmed working on the Serverless Starter Warehouse (verified 2026-03-27):

```sql
-- ai_classify — works on free tier
SELECT ai_classify("This product is amazing!", ARRAY("positive", "negative", "neutral")) AS sentiment;

-- ai_forecast — works on free tier
-- horizon is an INTEGER (number of periods), NOT a date
SELECT * FROM ai_forecast(
  TABLE(my_catalog.my_schema.time_series_table),
  horizon => 10,
  time_col => 'ds',
  value_col => 'y'
);
```

> **Note:** `ai_forecast` requires the time column to be `TIMESTAMP` type and `horizon` must be an integer (number of future periods to predict), not a timestamp.

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

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-banned-apis.md](1-banned-apis.md) — Banned APIs and detection regex
- [2-library-restrictions.md](2-library-restrictions.md) — Library restrictions and native PySpark SQL
- [4-common-fixes.md](4-common-fixes.md) — Common patterns to fix
- [5-full-checklist.md](5-full-checklist.md) — Full checklist
