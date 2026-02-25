---
name: databricks-free-tier-guardrails
description: Use when generating any Databricks artifact (notebooks, scripts, pipelines, jobs) to verify it is compatible with Databricks free tier and serverless compute. Triggers on "free tier", "community edition", "serverless", "no cluster", "free Databricks", or any code generation task where cluster type is unspecified. Apply AFTER other generation skills as a compatibility filter.
---

# Databricks Free Tier Guardrails

## Overview

Databricks free tier runs exclusively on **Serverless compute**, which uses **Spark Connect** instead of the classic Spark driver/executor model. This restricts a significant subset of PySpark APIs. Use this skill as a final compatibility pass on any generated artifact.

## What Free Tier Provides

- Serverless compute (Spark Connect-based)
- Unity Catalog (limited to default catalog)
- Databricks SQL (serverless SQL warehouse)
- Notebooks (Python, SQL, Markdown)
- Basic MLflow tracking
- Volumes (for file storage)

**Does NOT include:** classic clusters, GPU clusters, Databricks Runtime customisation, or arbitrary `%pip install` of heavy packages.

---

## Banned APIs (will fail silently or raise errors)

| Banned Pattern                          | Why                                       | Safe Alternative                          |
| --------------------------------------- | ----------------------------------------- | ----------------------------------------- |
| `spark.sparkContext`                    | SparkContext = RDD API, not Spark Connect | Remove or rewrite without it              |
| `sc.*`                                  | Same as above                             | Remove                                    |
| `.rdd.*`                                | RDD operations not supported              | Use DataFrame API                         |
| `df.cache()` / `df.persist()`           | Cache APIs not supported on serverless    | Remove; recompute or use a temp table     |
| `spark.sparkContext.setLogLevel(...)`   | SparkContext access                       | Remove entirely                           |
| `spark.sparkContext.defaultParallelism` | SparkContext access                       | Use a hardcoded `partitions=` value       |
| `spark.sparkContext.broadcast(...)`     | SparkContext access                       | Use `F.broadcast(df)` hint instead        |
| Scala / JVM UDFs                        | JVM not accessible via Spark Connect      | Rewrite as Python UDFs or SQL expressions |
| `%sh` magic with system-level ops       | Restricted shell access                   | Use Volume paths + `spark.sql`            |

---

## Library Restrictions

Free tier supports a **fixed set of pre-installed libraries**. Treat every `%pip install` as a risk.

| Library                          | Available? | Notes                                                                                         |
| -------------------------------- | ---------- | --------------------------------------------------------------------------------------------- |
| `pyspark`                        | ✅         | Core, always available                                                                        |
| `pandas`                         | ✅         | Available, but avoid for large data                                                           |
| `numpy`                          | ✅         | Pre-installed                                                                                 |
| `mlflow`                         | ✅         | Pre-installed                                                                                 |
| `faker`                          | ✅         | Requires `%pip install faker`; works fine on serverless                                       |
| `dbldatagen`                     | ⚠️         | Requires `%pip install`; internally may call `sparkContext` in older versions — pin `>=0.4.0` |
| `holidays`                       | ❌         | Must be pip-installed                                                                         |
| `scipy`                          | ⚠️         | Sometimes available; don't rely on it                                                         |
| Custom wheels / private packages | ❌         | Not supported without classic cluster                                                         |

**Rule:** If a library requires `%pip install`, flag it. Prefer native PySpark SQL functions (`rand()`, `randn()`, `expr()`, `sequence()`, `date_add()`, etc.) for data generation tasks.

---

## Native PySpark SQL — Always Safe

These require zero external dependencies and work under Spark Connect:

```python
from pyspark.sql import functions as F

# Random numbers
F.rand(seed=42)          # uniform [0, 1)
F.randn(seed=42)         # standard normal N(0,1)

# Timestamp generation from a numeric epoch
F.col("epoch_col").cast("timestamp")

# Categorical assignment without loops
F.when(F.rand() < 0.2, "AAPL").when(...).otherwise("AMD")

# Expr for SQL-style expressions
F.expr("round(900 + randn() * 4.5, 4)")

# Date/time manipulation
F.date_add("col", 5)
F.to_timestamp("col", "yyyy-MM-dd HH:mm:ss")
F.unix_timestamp("col")

# Sequences (use spark.range instead of Python range)
spark.range(100_000)     # distributed; never use Python range() + collect
```

---

## Pipelines — No Cluster Configuration

Declarative pipelines (DLT/SDP) on free tier run on **serverless compute only**. Never add cluster configuration to pipeline resources or YAML.

### Banned pipeline config

```yaml
# ❌ Do NOT include any of these in pipeline.yml
clusters:
  - label: default
    node_type_id: i3.xlarge
    num_workers: 2
    spark_version: 15.4.x-scala2.12

cluster:
  spark_version: ...
  node_type_id: ...
```

### Correct serverless pipeline resource

```yaml
# ✅ Serverless — no cluster block needed
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

Add to checklist:
- [ ] No `clusters:` / `cluster:` block in any pipeline resource
- [ ] No `node_type_id`, `num_workers`, or `spark_version` in pipeline YAML
- [ ] `serverless: true` present in pipeline resource

---

## Checklist — Run Before Finalising Any Artifact

Go line-by-line and confirm:

- [ ] No `spark.sparkContext` or `sc.` anywhere
- [ ] No `.cache()` or `.persist()` calls
- [ ] No `.rdd.` chained operations
- [ ] No `setLogLevel`
- [ ] No Python `for` / `while` loops generating rows (use `spark.range()`)
- [ ] No `.collect()` on large DataFrames (only acceptable for single-value scalars)
- [ ] No `pandas` for row generation (use DataFrame API)
- [ ] All `%pip install` packages flagged and reviewed
- [ ] `partitions=` set explicitly in any `DataGenerator` (avoids `defaultParallelism` call)
- [ ] Write targets are Volumes (`/Volumes/...`) or Delta tables — not local filesystem paths
- [ ] No `clusters:` / `cluster:` block in any pipeline resource YAML
- [ ] No `node_type_id`, `num_workers`, or `spark_version` in pipeline YAML
- [ ] Pipeline resources have `serverless: true`

---

## Common Patterns to Fix

### Fix: replace `.cache()` with a Delta temp table

```python
# ❌ Not supported on serverless
df_customers = customers_spec.build().cache()

# ✅ Write to a temp Delta table, then read back
df_customers.write.mode("overwrite").saveAsTable("_tmp_customers")
df_customers = spark.table("_tmp_customers")
```

### Fix: replace sparkContext broadcast with DataFrame hint

```python
# ❌ SparkContext = RDD API
bc = spark.sparkContext.broadcast(lookup_dict)

# ✅ Broadcast hint on small DataFrame
small_df = spark.createDataFrame(lookup_list)
df.join(F.broadcast(small_df), on="key")
```

### Fix: remove setLogLevel

```python
# ❌ SparkContext access
spark.sparkContext.setLogLevel("WARN")

# ✅ Just remove it — log level is managed via workspace UI on serverless
```

### Fix: row generation without loops

```python
# ❌ Python loop + collect pattern
rows = [Row(id=i) for i in range(100_000)]
df = spark.createDataFrame(rows)

# ✅ Fully distributed
df = spark.range(100_000)
```

---

## SQL Warehouse — Default Name

On the free tier, the only available SQL warehouse is:

```
Serverless Starter Warehouse
```

Use this exact name in all DAB `warehouse_id` lookups and dashboard resources:

```yaml
# ✅ Correct — free tier warehouse name
variables:
  warehouse_id:
    lookup:
      warehouse: Serverless Starter Warehouse
```

```yaml
# ❌ Do NOT use these — they do not exist on free tier
warehouse: Shared SQL Warehouse
warehouse: Starter Warehouse
warehouse: Pro Warehouse
```

Add to checklist:
- [ ] Any `warehouse_id` lookup uses name `Serverless Starter Warehouse`

---

## Related Skills

- **dbldatagen** — use for large-scale generation; always set `partitions=` explicitly and pin `dbldatagen>=0.4.0`
- **databricks-synthetic-data-generation** — Faker-based; flag all pip installs before use on free tier
- **databricks-spark-declarative-pipelines** — pipelines run on serverless by default; same restrictions apply
