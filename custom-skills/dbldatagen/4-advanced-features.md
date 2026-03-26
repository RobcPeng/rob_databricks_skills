# Advanced Features

Complex types, constraints, streaming data generation, CDC simulation, and schema inference.

## Complex Types (Structs, Arrays, JSON)

### Struct Columns

```python
# Method 1: withStructColumn helper (simplest)
.withColumn("event_type", "string", values=["click", "view", "purchase"], random=True)
.withColumn("event_ts", "timestamp",
            data_range=dg.DateRange(START, END, "seconds=1"), random=True)
.withStructColumn("event_info", fields=['event_type', 'event_ts'])

# Method 2: withStructColumn with field mapping (rename fields)
.withStructColumn("event_info", fields={
    'type': 'event_type',
    'timestamp': 'event_ts'
})

# Method 3: expr with named_struct (full control)
.withColumn("address", dg.INFER_DATATYPE,
            expr="named_struct('street', street, 'city', city, 'state', state)",
            baseColumn=['street', 'city', 'state'])
```

### Array Columns

```python
# Variable-length array of emails (1 to 6 items)
.withColumn("emails", "string", template=r'\w.\w@\w.com',
            numFeatures=(1, 6), structType="array")

# Array from helper columns using expr
.withColumn("r_0", "float", minValue=0, maxValue=100, random=True, omit=True)
.withColumn("r_1", "float", minValue=0, maxValue=100, random=True, omit=True)
.withColumn("r_2", "float", minValue=0, maxValue=100, random=True, omit=True)
.withColumn("observations", "array<float>",
            expr="slice(array(r_0, r_1, r_2), 1, abs(hash(id)) % 3 + 1)",
            baseColumn=["r_0", "r_1", "r_2"])
```

### JSON-Valued Fields

```python
# Convert a struct to a JSON string column
.withStructColumn("payload", fields=['event_type', 'event_ts'], asJson=True)
```

### Type Inference

Use `dg.INFER_DATATYPE` when `expr` determines the type:

```python
.withColumn("full_name", dg.INFER_DATATYPE,
            expr="concat(first_name, ' ', last_name)",
            baseColumn=["first_name", "last_name"])

.withColumn("ingest_ts", dg.INFER_DATATYPE, expr="current_timestamp()")
```

---

## Constraints

Apply constraints to enforce business rules on generated data:

```python
# SQL expression constraint — filters rows that violate the condition
# NOTE: rows parameter is BEFORE constraints. If you need 10K rows and
# the constraint filters ~20%, generate 12.5K rows.
spec = (
    dg.DataGenerator(spark, rows=12500, partitions=4,
                     randomSeedMethod="hash_fieldname")
    .withColumn("order_ts", "timestamp",
                data_range=dg.DateRange(START, END, "hours=1"), random=True)
    .withColumn("shipping_ts", "timestamp",
                data_range=dg.DateRange(START, END, "hours=1"), random=True,
                percentNulls=0.3)
    .withSqlConstraint("shipping_ts IS NULL OR shipping_ts > order_ts")
    .build()
)
```

### Constraint Types

| Constraint | Type | Description |
|-----------|------|-------------|
| `withSqlConstraint(expr)` | Filter | Rows not matching the SQL expression are removed |
| `UniqueCombinations(cols)` | Transform | Ensures unique combinations of specified columns (not supported on streaming) |

**Important:** Constraints modify the spec in-place. If reusing a spec with different constraints, clone it first:

```python
spec_clone = spec.clone()
spec_clone = spec_clone.withSqlConstraint("amount > 100")
```

**Row count warning:** The `rows` parameter determines rows generated **before** constraints filter. If a constraint filters ~20% of rows, set `rows` to ~125% of your target to compensate.

---

## Streaming Data Generation

Generate continuous streaming data for testing Structured Streaming or SDP pipelines:

```python
# Build with streaming enabled — rows parameter is ignored
streaming_df = (
    dg.DataGenerator(spark, name="sensor_stream", rows=0, partitions=4,
                     randomSeedMethod="hash_fieldname")
    .withColumn("device_id", "string", template=r'DEV-dddddd',
                uniqueValues=100)
    .withColumn("temperature", "double", minValue=15.0, maxValue=45.0,
                distribution=dist.Normal(mean=22.0, stddev=3.0), random=True)
    .withColumn("humidity", "double", minValue=20.0, maxValue=95.0,
                distribution=dist.Normal(mean=55.0, stddev=10.0), random=True)
    .withColumn("event_ts", dg.INFER_DATATYPE, expr="current_timestamp()")
    .build(withStreaming=True, options={"rowsPerSecond": 500})
)

# Write to Delta table as continuous stream
(streaming_df.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/sensor_stream")
 .table(f"{CATALOG}.{SCHEMA}.sensor_events"))
```

**Use with SDP / Delta Live Tables:**

```python
import dlt
import dbldatagen as dg

# Batch DLT table
@dlt.table
def raw_events():
    return (
        dg.DataGenerator(spark, name="events", rows=100_000, partitions=4,
                         randomSeedMethod="hash_fieldname")
        .withColumn("event_id", "string", template=r'EVT-dddddddd')
        .withColumn("event_type", "string",
                    values=["click", "view", "purchase"], weights=[50, 40, 10])
        .build()
    )

# Batch DLT table with partitioning and Delta properties
@dlt.table(
    name="device_data_source",
    partition_cols=["site", "event_date"],
    table_properties={"delta.autoOptimize.optimizeWrite": "true"}
)
def device_data():
    return (
        dg.DataGenerator(spark, rows=1_000_000, randomSeedMethod="hash_fieldname")
        .withColumn("site", "string", values=["site_a", "site_b", "site_c"])
        .withColumn("event_date", "date",
                    data_range=dg.DateRange("2024-01-01", "2024-12-31", "days=1"), random=True)
        .withColumn("value", "double", minValue=0, maxValue=100, random=True)
        .build()
    )

# Streaming DLT table
@dlt.table
def streaming_sensor_data():
    return (
        dg.DataGenerator(spark, rows=0, randomSeedMethod="hash_fieldname")
        .withColumn("device_id", "string", template=r'DEV-dddd', uniqueValues=50)
        .withColumn("temp", "double", minValue=15, maxValue=45,
                    distribution=dist.Normal(22, 3), random=True)
        .build(withStreaming=True, options={"rowsPerSecond": 1000})
    )
```

**Note:** `%pip install dbldatagen` must be the first cell in DLT notebooks, before any `%md` cells.

---

## Change Data Capture (CDC) Simulation

Generate an initial dataset, then simulate inserts and updates:

```python
# 1. Generate base dataset
base_spec = (
    dg.DataGenerator(spark, rows=10000, partitions=4,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("customer_id", "long", uniqueValues=10000)
    .withColumn("name", "string", template=r'\w \w', percentNulls=0.01)
    .withColumn("balance", "decimal(10,2)", minValue=0, maxValue=50000, random=True)
    .withColumn("created_ts", dg.INFER_DATATYPE, expr="current_timestamp()")
    .withColumn("memo", dg.INFER_DATATYPE, expr="'original'")
)
df_base = base_spec.build()
df_base.write.format("delta").mode("overwrite").save(f"{VOLUME_PATH}/customers_cdc")

# 2. Generate updates (sample existing + modify)
max_id = df_base.selectExpr("max(customer_id)").first()[0]

# New inserts (IDs above max)
insert_spec = (
    dg.DataGenerator(spark, rows=500, partitions=4,
                     randomSeedMethod="hash_fieldname", randomSeed=99)
    .withColumn("customer_id", "long", minValue=max_id + 1, uniqueValues=500)
    .withColumn("name", "string", template=r'\w \w')
    .withColumn("balance", "decimal(10,2)", minValue=0, maxValue=50000, random=True)
    .withColumn("created_ts", dg.INFER_DATATYPE, expr="current_timestamp()")
    .withColumn("memo", dg.INFER_DATATYPE, expr="'new insert'")
)

# Updates (sample from existing IDs)
df_updates = (
    df_base.sample(0.05)  # 5% of rows get updated
    .withColumn("balance", F.expr("round(rand() * 50000, 2)"))
    .withColumn("memo", F.lit("updated"))
    .withColumn("modified_ts", F.current_timestamp())
)

# 3. Merge
df_changes = insert_spec.build().union(df_updates.select(insert_spec.build().columns))
```

### scriptMerge — Auto-Generate MERGE SQL

```python
merge_sql = base_spec.scriptMerge(
    tgtName="customers",
    srcName="customers_changes",
    joinExpr="src.customer_id = tgt.customer_id",
    updateColumns=["name", "balance", "memo"],
    updateColumnExprs=[("memo", "'merged update'")]
)
spark.sql(merge_sql)
```

---

## Generating from Existing Schemas (DataAnalyzer)

Reverse-engineer a generation spec from an existing table or DataFrame:

```python
# Analyze an existing dataset
df_source = spark.table("production.sales.orders")
analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source)

# View statistical summary
display(analyzer.summarizeToDF())

# Generate code from data (analyzes distributions and ranges)
generated_code = analyzer.scriptDataGeneratorFromData()
print(generated_code)
# Produces a ready-to-use DataGenerator spec matching the source data's patterns

# Generate code from schema only (no data analysis, just types and names)
schema_code = analyzer.scriptDataGeneratorFromSchema()
print(schema_code)
```

**Suppress output** (get code as string only):

```python
code_string = analyzer.scriptDataGeneratorFromData(suppressOutput=True)
# code_string contains the generated spec -- not printed to notebook output
```

**Use case:** You have a production table and want to create synthetic test data with similar shape. DataAnalyzer gives you a starting spec that you can then customize.

**Limitation:** This feature is experimental. The generated code is a starting point -- you'll need to refine distributions, add correlated columns, and tune parameters.
