---
name: databricks-free-tier-guardrails
description: "Use when generating Databricks artifacts to verify free tier and serverless compatibility. Apply as a filter after other skills. Checks for banned APIs, cluster config, and Spark Connect restrictions."
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

## Quick Reference: Safe vs. Banned

```
+------------------------------------------------------------------+
|  SAFE (Spark Connect)              |  BANNED (Classic only)     |
|------------------------------------+----------------------------|
|  spark.sql("...")                  |  spark.sparkContext        |
|  spark.read.parquet/csv/json       |  sc.*                     |
|  spark.table("catalog.schema.t")  |  df.rdd.*                 |
|  spark.range(N)                   |  df.cache() / .persist()   |
|  spark.createDataFrame(list)      |  df.foreach()             |
|  df.filter / .select / .join      |  sparkContext.broadcast()  |
|  df.groupBy / .agg / .orderBy     |  sparkContext.textFile()   |
|  df.write.saveAsTable / .parquet  |  setLogLevel()             |
|  F.broadcast(df) hint             |  dbutils.fs.mount()        |
|  F.expr("SQL expression")        |  /mnt/ paths               |
|  F.rand() / F.randn()            |  SparkConf()               |
|  Window functions                 |  createDataFrame(rdd)      |
|  ST_* / H3_* geospatial          |  %sh system commands       |
|  /Volumes/... paths               |  %scala / %r               |
|  %pip install <package>           |  new_cluster: in YAML      |
+------------------------------------------------------------------+
```

---

## Supporting Files

| File | Content |
|------|---------|
| [1-banned-apis.md](1-banned-apis.md) | Banned APIs table and detection regex patterns |
| [2-library-restrictions.md](2-library-restrictions.md) | Library availability, safe install patterns, native PySpark SQL reference |
| [3-resource-config.md](3-resource-config.md) | Pipelines, Jobs, Notebooks, SQL Warehouse, AI Functions, Unity Catalog, MLflow, Geospatial |
| [4-common-fixes.md](4-common-fixes.md) | Common patterns to fix with before/after code examples |
| [5-full-checklist.md](5-full-checklist.md) | Full compatibility checklist to run before finalizing any artifact |

---

## Related Skills

- **dbldatagen** — use for large-scale generation; always set `partitions=` explicitly and pin `dbldatagen>=0.4.0`
- **databricks-synthetic-data-generation** — Faker-based; flag all pip installs before use on free tier
- **databricks-spark-declarative-pipelines** — pipelines run on serverless by default; same restrictions apply
- **databricks-geospatial** — ST_ and H3 functions work on serverless; flag geopandas/arcgis pip installs
- **databricks-jobs** — job tasks must omit cluster config on free tier
- **databricks-bundles** — pipeline/job resources must use serverless patterns
