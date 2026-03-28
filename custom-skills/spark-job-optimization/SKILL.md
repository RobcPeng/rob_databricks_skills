---
name: spark-job-optimization
description: "Use when optimizing Spark performance, diagnosing slow jobs, reading query plans, interpreting Spark UI, or debugging OOM/skew/shuffle issues on Databricks."
---

# Spark Job Optimization

A comprehensive guide to understanding, diagnosing, and optimizing Apache Spark jobs on Databricks. Covers foundational concepts for learners and practical optimization patterns for experienced engineers.

## How to Use This Skill

**Learning path (new to Spark optimization):** Start at file 1 and work through sequentially. Each file builds on the previous.

**Quick reference (experienced users):** Jump directly to the topic you need using the routing table below.

**Troubleshooting a problem right now:** Go straight to [9-symptom-troubleshooting.md](9-symptom-troubleshooting.md).

## Routing Table

| File | Topic | When to Use |
|------|-------|-------------|
| [1-spark-internals-foundation.md](1-spark-internals-foundation.md) | Spark Internals | Understanding Catalyst, Tungsten, memory model, shuffle architecture |
| [2-reading-query-plans.md](2-reading-query-plans.md) | Query Plans | Reading `explain()` output, understanding logical/physical plans, plan nodes |
| [3-spark-ui-guide.md](3-spark-ui-guide.md) | Spark UI | Navigating Jobs/Stages/Tasks/SQL tabs, DAG interpretation, timeline views |
| [4-databricks-diagnostics.md](4-databricks-diagnostics.md) | Databricks Diagnostics | Ganglia, cluster metrics, event logs, Photon-specific behavior |
| [5-join-optimization.md](5-join-optimization.md) | Join Optimization | Choosing join strategies, broadcast vs sort-merge, handling skewed joins |
| [6-shuffle-and-partitioning.md](6-shuffle-and-partitioning.md) | Shuffle & Partitioning | Partition sizing, AQE, coalesce vs repartition, skew mitigation |
| [7-memory-and-spill.md](7-memory-and-spill.md) | Memory & Spill | Executor memory tuning, GC, spill detection, OOM diagnosis |
| [8-io-and-storage.md](8-io-and-storage.md) | I/O & Storage | File formats, predicate pushdown, small files, Delta optimization |
| [9-symptom-troubleshooting.md](9-symptom-troubleshooting.md) | Troubleshooting | Symptom-first diagnosis: slow jobs, OOM, skew, spill, failures |
| [10-hands-on-labs.md](10-hands-on-labs.md) | Hands-On Labs | Runnable exercises to practice diagnosing and fixing performance issues |

## Quick Start: The 5-Minute Optimization Checklist

Before diving deep, check these common wins first:

```python
# 1. Check your query plan for red flags
df.explain("formatted")  # Look for: CartesianProduct, BroadcastNestedLoopJoin, no pushdown

# 2. Enable AQE (on by default in Databricks, but verify)
spark.conf.get("spark.sql.adaptive.enabled")  # Should be "true"

# 3. Check partition count after shuffles
df.rdd.getNumPartitions()  # Rule of thumb: 128MB per partition

# 4. Look for data skew
df.groupBy("join_key").count().orderBy(F.desc("count")).show(20)

# 5. Check file sizes in your Delta tables
spark.sql("DESCRIBE DETAIL my_table").select("numFiles", "sizeInBytes").show()
```

### Top 10 Quick Wins

| # | Check | Fix |
|---|-------|-----|
| 1 | Missing predicate pushdown | Add filters early, use partitioned columns |
| 2 | Wrong join type (CartesianProduct) | Add join conditions, use broadcast hints |
| 3 | Too many/few shuffle partitions | Set `spark.sql.shuffle.partitions` or use AQE |
| 4 | Data skew on join keys | Salt keys, use skew join hints, or pre-aggregate |
| 5 | Small files (< 32MB each) | Run `OPTIMIZE`, use Auto Compaction |
| 6 | No column pruning | Select only needed columns early |
| 7 | Repeated computation | Cache/persist intermediate DataFrames |
| 8 | UDFs blocking Catalyst | Replace with built-in functions or Pandas UDFs |
| 9 | Over-provisioned cluster | Right-size: check executor utilization in Spark UI |
| 10 | Missing Z-ORDER / Liquid Clustering | Add clustering on high-cardinality filter columns |

## Key Spark Configurations Reference

```python
# === Shuffle & Partitioning ===
spark.conf.set("spark.sql.shuffle.partitions", "auto")     # AQE handles this
spark.conf.set("spark.sql.adaptive.enabled", "true")        # Enable AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# === Join Optimization ===
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")  # 10MB default
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# === Memory ===
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.memory.fraction", "0.6")              # Unified memory fraction
spark.conf.set("spark.memory.storageFraction", "0.5")        # Initial storage share

# === I/O ===
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")  # Max partition size for reads
spark.conf.set("spark.sql.files.openCostInBytes", "4m")      # Cost to open a file
spark.conf.set("spark.sql.parquet.filterPushdown", "true")

# === Delta Lake ===
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

## Photon Callouts

> **Photon Note:** Throughout this skill, look for "Photon:" callouts that highlight where Photon-enabled clusters behave differently from standard JVM Spark. Key differences include vectorized execution, native C++ shuffle, and different memory pressure characteristics. See [4-databricks-diagnostics.md](4-databricks-diagnostics.md) for details.

## Related Skills

- [spark-structured-streaming](../spark-structured-streaming/SKILL.md) — Streaming-specific optimization (triggers, state management, watermarks)
- [spark-declarative-pipelines](../spark-declarative-pipelines/SKILL.md) — Lakeflow/DLT pipeline optimization (Liquid Clustering, pipeline tuning)
- [databricks-dbsql](../databricks-dbsql/SKILL.md) — SQL warehouse optimization and advanced SQL features
