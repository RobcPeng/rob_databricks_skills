# Full Checklist — Run Before Finalizing Any Artifact

Go line-by-line and confirm:

## PySpark Code
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

## File Paths
- [ ] Write targets are Volumes (`/Volumes/...`) or Delta tables — not local filesystem paths
- [ ] No `/mnt/` mount paths (not available on serverless)
- [ ] No `/dbfs/` paths for critical data (use Volumes instead)
- [ ] No `/tmp/` paths for persistent data (ephemeral on serverless)

## DataGenerator (dbldatagen)
- [ ] `partitions=` set explicitly (avoids `defaultParallelism` call)
- [ ] Using `dbldatagen>=0.4.0` (older versions may use `sparkContext`)
- [ ] No `.cache()` on built DataFrames (use temp table pattern)

## Pipeline Resources (YAML)
- [ ] No `clusters:` / `cluster:` block in any pipeline resource
- [ ] No `node_type_id`, `num_workers`, or `spark_version` in pipeline YAML
- [ ] Pipeline resources have `serverless: true`

## Job Resources (YAML)
- [ ] No `new_cluster:` block in job tasks
- [ ] No `existing_cluster_id:` in job tasks
- [ ] No `spark_version` or `node_type_id` in job config

## SQL Warehouse
- [ ] Any `warehouse_id` lookup uses name `Serverless Starter Warehouse`
- [ ] No references to custom warehouses, Pro warehouses, or Classic warehouses

## Notebooks
- [ ] No `%sh` with system-level operations
- [ ] No `%scala` cells (not supported on serverless)
- [ ] No `%r` cells (not supported on serverless)

---

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-banned-apis.md](1-banned-apis.md) — Banned APIs and detection regex
- [2-library-restrictions.md](2-library-restrictions.md) — Library restrictions and native PySpark SQL
- [3-resource-config.md](3-resource-config.md) — Resource configuration (Pipelines, Jobs, Notebooks, etc.)
- [4-common-fixes.md](4-common-fixes.md) — Common patterns to fix
