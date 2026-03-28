---
name: databricks-pipeline-guardrails
description: >
  Quality guardrails for Databricks pipeline code — fully qualified table names, table existence
  verification, broadcast hints for non-equi joins, schema contracts for multi-layer pipelines,
  and union alignment patterns. Use when generating pipeline notebooks, SDP definitions, or
  multi-table ETL code. Triggers on: 'pipeline', 'bronze/silver/gold', 'medallion', 'ETL',
  'streaming table', 'materialized view', 'SDP', or when dispatching subagents to build pipeline layers.
---

# Databricks Pipeline Guardrails

Quality checks and patterns that prevent common pipeline failures. Apply these guardrails to all generated pipeline code — especially when subagents build layers in parallel.

## 1. Always Use Fully Qualified Table Names

Every table reference in pipeline code must use three-part names: `catalog.schema.table`.

**Why:** Unqualified names resolve to the pipeline's configured `catalog` + `target` schema. In medallion architectures where bronze/silver/gold live in different schemas, unqualified names silently write to the wrong schema — then downstream layers fail with `TABLE_OR_VIEW_NOT_FOUND`.

```python
# BAD — resolves to pipeline target schema (e.g., 01_bronze)
@dp.materialized_view(name="enriched_orders")

# GOOD — explicit destination
@dp.materialized_view(name="my_catalog.02_silver.enriched_orders")
```

```sql
-- BAD
SELECT * FROM enriched_orders;

-- GOOD
SELECT * FROM my_catalog.02_silver.enriched_orders;
```

**Rule:** The only tables that may use unqualified names are those in the pipeline's default target schema (typically bronze). Silver, gold, and any cross-schema references must be fully qualified.

---

## 2. Verify Table Existence Before Referencing

Before any `SELECT`, `JOIN`, or `spark.read.table()` that references another table, verify the table exists. This catches schema drift, typos, and missing dependencies before they cause runtime failures.

**In notebooks / setup code:**
```python
# Check if source table exists before reading
assert spark.catalog.tableExists("my_catalog.01_bronze.raw_orders"), \
    "Source table my_catalog.01_bronze.raw_orders does not exist"

df = spark.read.table("my_catalog.01_bronze.raw_orders")
```

**In SQL:**
```sql
-- List tables in the target schema to verify
SHOW TABLES IN my_catalog.02_silver;
```

**In SDP pipelines:** Table existence within the same pipeline is guaranteed by the dependency graph — `spark.readStream.table("raw_orders")` will wait for the bronze table to be created. Cross-pipeline or external table references should be verified in a setup step.

**When dispatching subagents:** Include the exact output table names from upstream layers in each agent's prompt. Don't let agents invent table names independently.

---

## 3. Schema Contracts for Multi-Layer Pipelines

When building bronze -> silver -> gold (or any multi-table pipeline), define an explicit column contract before writing any code. This is critical when multiple agents or notebooks build different layers.

**Write this contract first:**
```
Bronze output: my_catalog.01_bronze.raw_orders
  - order_id: STRING
  - customer_id: STRING
  - amount: DECIMAL(10,2)
  - order_date: TIMESTAMP
  - status: STRING
  - _ingest_timestamp: TIMESTAMP

Silver output: my_catalog.02_silver.enriched_orders
  - order_id: STRING
  - customer_id: STRING
  - customer_name: STRING  (joined from customers)
  - amount: DECIMAL(10,2)
  - order_date: TIMESTAMP
  - status: STRING
  - is_high_value: BOOLEAN  (derived: amount > 1000)

Gold output: my_catalog.03_gold.daily_order_summary
  - order_date: DATE
  - total_orders: BIGINT
  - total_revenue: DECIMAL(12,2)
  - high_value_count: BIGINT
```

**Why:** Without this contract, parallel agents make independent naming decisions — `department` vs `department_name`, `centroid_lat` vs `neighborhood_centroid_lat`, `incident_count` vs `request_count`. The 5 minutes writing the contract saves 30 minutes of deploy-fail-fix cycles.

**Rules:**
- Every column must have a name and type
- Derived columns must note how they're computed
- Joined columns must note the source table
- Include this contract in every subagent's prompt

---

## 4. Broadcast Hints for Non-Equi Joins

Spatial joins (`ST_CONTAINS`, `ST_INTERSECTS`, `ST_DWITHIN`), range joins, and any join without an equality predicate use nested loop join — effectively a cross product. Without an explicit broadcast, Spark may shuffle both sides.

```python
from pyspark.sql import functions as F

# ALWAYS broadcast the small side of non-equi joins
result = large_table.join(
    F.broadcast(small_reference_table),
    F.expr("ST_CONTAINS(ref_geometry, point_geometry)")
)
```

```sql
-- SQL broadcast hint
SELECT /*+ BROADCAST(ref) */ *
FROM large_table t
JOIN small_reference ref
  ON ST_CONTAINS(ref.geometry, t.point);
```

**When to broadcast:**
- Boundary/neighborhood polygons (typically < 1K rows)
- Lookup/dimension tables (< 10K rows)
- Any table used as the "container" side of a spatial containment check

**Why serverless matters:** On serverless compute, you cannot tune `spark.sql.autoBroadcastJoinThreshold`. Explicit `F.broadcast()` or `/*+ BROADCAST */` hints are the only way to force broadcast behavior.

---

## 5. Union Schema Alignment

When combining streaming + batch sources (or any sources with different schemas), align schemas explicitly before `union()` or `unionByName()`.

**Common mismatches:**
- FK integers vs resolved name strings (`department_id` vs `department`)
- Missing columns in one source (`citizen_id` exists in Kafka but not batch parquet)
- Different column names for the same concept (`event_time` vs `timestamp`)

**Pattern:**
```python
# 1. Resolve FKs on the batch side FIRST
batch_resolved = batch_df.join(
    F.broadcast(departments),
    batch_df.department_id == departments.id
).select(
    departments.name.alias("department"),  # align to streaming column name
    # ... other columns
)

# 2. Add null placeholders for source-specific columns
batch_aligned = batch_resolved.select(
    F.col("department"),
    F.lit(None).cast("string").alias("citizen_id"),  # streaming-only field
    F.col("amount"),
    F.lit("batch").alias("_source")
)

stream_aligned = stream_df.select(
    F.col("department"),
    F.col("citizen_id"),
    F.col("amount"),
    F.lit("kafka").alias("_source")
)

# 3. Union with matching schemas
combined = stream_aligned.unionByName(batch_aligned)
```

**Rule:** Never `unionByName(allowMissingColumns=True)` without understanding what's missing. Explicit alignment makes the schema contract visible and debuggable.

---

## Pre-Flight Checklist

Before deploying any pipeline code, verify:

- [ ] All table references use fully qualified three-part names (except default target schema)
- [ ] Cross-schema and external table references have existence checks
- [ ] Column names match exactly between upstream output and downstream input
- [ ] Non-equi joins (spatial, range) use `F.broadcast()` or `/*+ BROADCAST */`
- [ ] Union operations have explicit schema alignment (no silent `allowMissingColumns`)
- [ ] SDP `%pip install` cells are the first cell in each notebook
- [ ] No DDL (`CREATE SCHEMA`, `CREATE VOLUME`) inside SDP pipeline notebooks
- [ ] Import uses `from pyspark import pipelines as dp` (not `import dlt`)
