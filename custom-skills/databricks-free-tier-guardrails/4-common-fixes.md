# Common Patterns to Fix

## Fix: replace `.cache()` with a Delta temp table

```python
# Not supported on serverless
df_customers = customers_spec.build().cache()

# Write to a temp Delta table, then read back
df_customers = customers_spec.build()
df_customers.write.mode("overwrite").saveAsTable("_tmp_customers")
df_customers = spark.table("_tmp_customers")
```

## Fix: replace sparkContext broadcast with DataFrame hint

```python
# SparkContext = RDD API
bc = spark.sparkContext.broadcast(lookup_dict)

# Broadcast hint on small DataFrame
small_df = spark.createDataFrame(lookup_list)
df.join(F.broadcast(small_df), on="key")
```

## Fix: remove setLogLevel

```python
# SparkContext access
spark.sparkContext.setLogLevel("WARN")

# Just remove it — log level is managed via workspace UI on serverless
```

## Fix: row generation without loops

```python
# Python loop + collect pattern (slow + breaks on serverless)
rows = [Row(id=i) for i in range(100_000)]
df = spark.createDataFrame(rows)

# Fully distributed
df = spark.range(100_000)
```

## Fix: replace dbutils.fs with Volume paths

```python
# dbutils.fs (limited on serverless)
dbutils.fs.ls("/mnt/data/")
dbutils.fs.cp("/mnt/data/file.csv", "/tmp/file.csv")

# Volume paths (always work)
files = spark.sql("LIST '/Volumes/catalog/schema/volume/'")
df = spark.read.csv("/Volumes/catalog/schema/volume/file.csv")
```

## Fix: replace RDD operations with DataFrame API

```python
# RDD operations (not supported)
rdd = df.rdd.map(lambda row: (row.key, row.value * 2))
result = rdd.reduceByKey(lambda a, b: a + b)

# DataFrame API (always works)
result = df.groupBy("key").agg(F.sum(F.col("value") * 2).alias("total"))
```

## Fix: replace .collect() on large data

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

## Fix: replace createDataFrame(rdd) with list of Rows

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

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-banned-apis.md](1-banned-apis.md) — Banned APIs and detection regex
- [2-library-restrictions.md](2-library-restrictions.md) — Library restrictions and native PySpark SQL
- [3-resource-config.md](3-resource-config.md) — Resource configuration (Pipelines, Jobs, Notebooks, etc.)
- [5-full-checklist.md](5-full-checklist.md) — Full checklist
