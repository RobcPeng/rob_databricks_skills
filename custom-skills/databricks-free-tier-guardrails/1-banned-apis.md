# Banned APIs (will fail silently or raise errors)

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

## Banned API Detection Regex

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

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [2-library-restrictions.md](2-library-restrictions.md) — Library restrictions and native PySpark SQL
- [3-resource-config.md](3-resource-config.md) — Resource configuration (Pipelines, Jobs, Notebooks, etc.)
- [4-common-fixes.md](4-common-fixes.md) — Common patterns to fix
- [5-full-checklist.md](5-full-checklist.md) — Full checklist
