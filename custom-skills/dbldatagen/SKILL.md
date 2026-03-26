---
name: dbldatagen
description: >
  Generate large-scale synthetic data using Databricks Labs dbldatagen (Spark-native, declarative).
  Use when creating test data, demo datasets, or synthetic tables at scale using dbldatagen instead
  of Faker. Triggers on: 'dbldatagen', 'synthetic data', 'generate data', 'test data', 'fake data
  at scale', 'data generator', 'DataGenerator', 'withColumn spec', 'mock data', 'sample dataset'.
---

# Synthetic Data Generation with dbldatagen

Generate synthetic data for Databricks using **dbldatagen** — a Spark-native, declarative library from Databricks Labs designed for generating large volumes of data efficiently. It generates data directly as PySpark DataFrames without pandas, making it ideal for large datasets (millions to billions of rows).

## Installation

dbldatagen is NOT pre-installed on Databricks. Install it using `execute_databricks_command` tool:
- `code`: "%pip install dbldatagen"

Save the returned `cluster_id` and `context_id` for subsequent calls.

## Workflow

1. **Write Python code to a local file** (e.g., `scripts/generate_data.py`)
2. **Execute on Databricks** using the `run_python_file_on_databricks` MCP tool
3. **If execution fails**: Edit the local file to fix the error, then re-execute
4. **Reuse the context** by passing the returned `cluster_id` and `context_id`

**Always work with local files first, then execute.**

### Context Reuse Pattern

**First execution**:
- `file_path`: "scripts/generate_data.py"

Returns: `{ success, output, error, cluster_id, context_id, ... }`

**If execution fails:**
1. Edit the local Python file to fix the issue
2. Re-execute: `file_path`, `cluster_id`, `context_id`

**Follow-up executions** reuse context (faster, keeps installed libraries).

## Core API

### DataGenerator

```python
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import IntegerType, StringType, LongType

gen = (
    dg.DataGenerator(spark, name="my_dataset", rows=100_000,
                     partitions=4, randomSeedMethod="hash_fieldname")
    .withIdOutput()                          # adds auto-increment 'id' column
    .withColumn("col_name", "string", ...)  # define each column
    .build()                                 # returns a Spark DataFrame
)
```

**Key `DataGenerator` parameters:**
- `rows`: total rows to generate
- `partitions`: Spark partitions (tune for parallelism)
- `randomSeedMethod="hash_fieldname"`: recommended — gives each column its own random sequence, maintains reproducibility across runs
- `randomSeed=42`: fixed seed for full reproducibility

### withColumn Reference

| Option | Purpose | Example |
|--------|---------|---------|
| `values=[...]` | Discrete choices | `values=['A','B','C']` |
| `weights=[...]` | Probability weights (match `values`) | `weights=[60, 30, 10]` |
| `minValue/maxValue` | Numeric range | `minValue=1, maxValue=1000` |
| `random=True` | Random within range/values | `random=True` |
| `uniqueValues=N` | Generate N distinct values | `uniqueValues=5000` |
| `prefix="X-"` | Prefix for generated strings | `prefix="CUST-"` |
| `template=r'\w'` | Regex-like text pattern | `template=r'\w.\w@\w.com'` |
| `expr="..."` | Spark SQL expression | `expr="concat(first, ' ', last)"` |
| `baseColumn="col"` | Derive from another column | `baseColumn="tier"` |
| `baseColumnType="hash"` | Hash the base column value | `baseColumnType="hash"` |
| `distribution=dist.X` | Statistical distribution | `distribution=dist.Gamma(1.0,2.0)` |
| `data_range=dg.DateRange(...)` | Date range | see DateRange section |
| `percentNulls=0.05` | Fraction of nulls | `percentNulls=0.05` |
| `omit=True` | Exclude from output (helper col) | `omit=True` |
| `numFeatures=(min,max)` | Variable-length array generation | `numFeatures=(1, 6)` |
| `structType="array"` | Output as array type | `structType="array"` |
| `format="..."` | String formatting | `format="%05d"` |

### withColumnSpec — Apply to Existing Schema

Use `withColumnSpec` when you have an existing schema and want to define generation rules for specific columns **without renaming or retyping**:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

existing_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("balance", DoubleType()),
])

gen = (
    dg.DataGenerator(spark, name="from_schema", rows=10_000,
                     randomSeedMethod="hash_fieldname")
    .withSchema(existing_schema)
    .withColumnSpec("customer_id", minValue=10000, maxValue=99999)
    .withColumnSpec("name", template=r'\w \w')
    .withColumnSpec("balance", minValue=0, maxValue=50000,
                    distribution=dist.Gamma(2.0, 5000.0), random=True)
    .build()
)
```

### withColumnSpecs — Bulk Pattern Matching

Apply generation rules to multiple columns at once using pattern matching:

```python
gen = (
    dg.DataGenerator(spark, name="bulk", rows=50_000,
                     randomSeedMethod="hash_fieldname")
    .withSchema(large_schema)
    # Apply rules to all string columns matching a pattern
    .withColumnSpecs(matchTypes=[StringType()], template=r'\w \w')
    # Apply rules to all integer columns
    .withColumnSpecs(matchTypes=[IntegerType()], minValue=0, maxValue=1000, random=True)
    .build()
)
```

## Non-Linear Distributions

**Never use uniform** — real data is rarely uniform. Use these distributions:

```python
import dbldatagen.distributions as dist

# Gamma — good for positive-skewed values (order amounts, response times)
.withColumn("amount", "double", minValue=0, maxValue=10000,
            distribution=dist.Gamma(shape=1.5, scale=200.0), random=True)

# Normal / Gaussian — good for measurements around a mean
.withColumn("latency_ms", "double", minValue=0, maxValue=2000,
            distribution=dist.Normal(mean=120, stddev=40), random=True)

# Exponential — good for inter-arrival times, resolution durations
# ⚠️ Exponential accepts `rate` (NOT `scale`). scale = 1/rate.
# rate=1/24 ≈ 0.0417 → mean ~24 hours
.withColumn("resolution_hours", "double", minValue=0, maxValue=500,
            distribution=dist.Exponential(rate=1/24), random=True)

# Beta — good for rates, probabilities (0-1 range)
.withColumn("error_rate", "double", minValue=0.0, maxValue=1.0,
            distribution=dist.Beta(alpha=2.0, beta=5.0), random=True)

# Weighted categorical (power-law-like effect)
.withColumn("tier", "string",
            values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
            random=True)
```

### Distribution Selection Guide

| Distribution | Shape | Use Case | Parameters |
|-------------|-------|----------|------------|
| `Gamma(shape, scale)` | Right-skewed | Revenue, order amounts, wait times | shape: peak sharpness, scale: spread |
| `Normal(mean, stddev)` | Bell curve | Measurements, scores, physical values | mean: center, stddev: spread |
| `Exponential(rate)` | Steep decay | Inter-arrival times, durations, TTL | rate: 1/mean_value |
| `Beta(alpha, beta)` | Flexible 0-1 | Rates, probabilities, percentages | alpha>beta: right-skew, alpha<beta: left-skew |
| Weighted `values` | Categorical | Status, tier, category, region | weights: relative frequencies |

## Text Templates

Template characters: `d`=digit, `a`=lowercase alpha, `A`=uppercase, `\w`=random word, `\n`=number 0-255, `|`=OR alternative:

```python
.withColumn("email",    "string", template=r'\w.\w@\w.com|\w@\w.co.uk')
.withColumn("phone",    "string", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd')
.withColumn("ip_addr",  "string", template=r'\n.\n.\n.\n')
.withColumn("order_id", "string", template=r'ORD-dddddd')
.withColumn("cust_id",  "string", template=r'CUST-ddddd')
.withColumn("sku",      "string", template=r'SKU-AAA-dddd')
.withColumn("mac_addr", "string", template=r'AA:AA:AA:AA:AA:AA')
.withColumn("uuid",     "string", template=r'AAAAdddd-dddd-dddd-dddd-AAAAdddddddd')
```

Custom word list for realistic domain-specific names:

```python
products = ['widget', 'gadget', 'module', 'device', 'sensor']
.withColumn("product_name", "string",
            text=dg.TemplateGenerator(r'\w Pro|\w Plus|\w Enterprise',
                                      extendedWordList=products))
```

## Date and Time Ranges

Use `dg.DateRange` for controlled date generation:

```python
from datetime import datetime, timedelta

end_dt   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_dt = end_dt - timedelta(days=180)

START = start_dt.strftime("%Y-%m-%d 00:00:00")
END   = end_dt.strftime("%Y-%m-%d 23:59:59")

# Date column — random day within range
.withColumn("event_date", "date",
            data_range=dg.DateRange(START, END, "days=1"),
            random=True)

# Timestamp column — random minute within range
.withColumn("event_ts", "timestamp",
            data_range=dg.DateRange(START, END, "minutes=1"),
            random=True)

# Derived date (e.g., resolved_at = created_at + delay)
.withColumn("delay_days", "int", minValue=1, maxValue=30, random=True, omit=True)
.withColumn("resolved_at", "date",
            expr="date_add(event_date, delay_days)",
            baseColumn=["event_date", "delay_days"])
```

## Row Coherence (Correlated Columns)

Use `baseColumn` + `expr` with CASE WHEN to make attributes correlate logically:

```python
# Step 1: generate the driver column
.withColumn("tier", "string",
            values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
            random=True)

# Step 2: helper uniform random for stochasticity
.withColumn("_r", "double", minValue=0.0, maxValue=1.0, random=True, omit=True)

# Step 3: amount driven by tier (approximate log-normal effect via exp)
.withColumn("amount", "decimal(10,2)",
            expr="""round(case
                when tier = 'Enterprise' then exp(7 + _r * 2.0)
                when tier = 'Pro'        then exp(5 + _r * 1.5)
                else                          exp(3 + _r * 1.2)
            end, 2)""",
            baseColumn=["tier", "_r"])

# Step 4: priority driven by tier
.withColumn("priority", "string",
            expr="""case
                when tier = 'Enterprise' then
                    case when _r < 0.30 then 'Critical'
                         when _r < 0.80 then 'High' else 'Medium' end
                else
                    case when _r < 0.05 then 'Critical'
                         when _r < 0.25 then 'High'
                         when _r < 0.70 then 'Medium' else 'Low' end
            end""",
            baseColumn=["tier", "_r"])

# Step 5: CSAT driven by resolution time
.withColumn("_res", "double",
            minValue=0, maxValue=500,
            distribution=dist.Exponential(rate=1/24), random=True, omit=True)
.withColumn("csat_score", "int",
            expr="""case
                when _res <  4 then 5
                when _res < 12 then 4
                when _res < 36 then 3
                when _res < 72 then 2
                else 1
            end""",
            baseColumn="_res")
```

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

## Multi-Table Generation with Referential Integrity

dbldatagen uses **hash-based foreign keys** for referential integrity without joins:

```python
UNIQUE_CUSTOMERS = 2500

# 1. Parent table — customer_id is the primary key
customers_spec = (
    dg.DataGenerator(spark, rows=UNIQUE_CUSTOMERS, partitions=4,
                     randomSeedMethod="hash_fieldname")
    .withColumn("customer_id", "long",
                minValue=100000, uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
                baseColumn="customer_id")   # deterministic per customer_id
)
df_customers = customers_spec.build().cache()

# 2. Child table — reference customer_id via hash
orders_spec = (
    dg.DataGenerator(spark, rows=25000, partitions=4,
                     randomSeedMethod="hash_fieldname")
    .withColumn("_cust_base", "long",
                minValue=100000, uniqueValues=UNIQUE_CUSTOMERS,
                random=True, omit=True)
    # hash maps _cust_base to the same customer_id space
    .withColumn("customer_id", "long",
                minValue=100000, baseColumn="_cust_base",
                baseColumnType="hash", uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("order_id", "string", template=r'ORD-dddddd')
)
df_orders = orders_spec.build()

# 3. Join to carry parent attributes into child (for correlated fields)
df_orders_enriched = df_orders.join(
    df_customers.select("customer_id", "tier"),
    on="customer_id", how="left"
)
```

### Event Spikes (Two-Spec Pattern)

For incident spikes or seasonal effects, generate two separate specs and union:

```python
from datetime import datetime, timedelta

end_dt      = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_dt    = end_dt - timedelta(days=180)
inc_end_dt  = end_dt - timedelta(days=21)
inc_start_dt= inc_end_dt - timedelta(days=10)

NORMAL_START = start_dt.strftime("%Y-%m-%d 00:00:00")
NORMAL_END   = inc_start_dt.strftime("%Y-%m-%d 23:59:59")
INC_START    = inc_start_dt.strftime("%Y-%m-%d 00:00:00")
INC_END      = inc_end_dt.strftime("%Y-%m-%d 23:59:59")

N_NORMAL   = 7000
N_INCIDENT = 1000   # ~3x normal rate for 10-day window

# Normal-period tickets
normal_spec = (
    dg.DataGenerator(spark, rows=N_NORMAL, partitions=4,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("category", "string",
                values=["Auth", "Network", "Billing", "Account"],
                weights=[25, 30, 25, 20], random=True)
    .withColumn("created_at", "date",
                data_range=dg.DateRange(NORMAL_START, NORMAL_END, "days=1"),
                random=True)
)

# Incident-period tickets — Auth dominates
incident_spec = (
    dg.DataGenerator(spark, rows=N_INCIDENT, partitions=4,
                     randomSeedMethod="hash_fieldname", randomSeed=99)
    .withColumn("category", "string",
                values=["Auth", "Network", "Billing", "Account"],
                weights=[65, 15, 10, 10], random=True)
    .withColumn("created_at", "date",
                data_range=dg.DateRange(INC_START, INC_END, "days=1"),
                random=True)
)

df_tickets = normal_spec.build().union(incident_spec.build())
```

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

**Important:** Constraints modify the spec in-place. If reusing a spec with different constraints, clone it first:

```python
spec_clone = spec.clone()
spec_clone.withSqlConstraint("amount > 100")
```

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
```

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

**Use case:** You have a production table and want to create synthetic test data with similar shape. DataAnalyzer gives you a starting spec that you can then customize.

## Storage Destination

### Ask for Schema Name

Default catalog is `ai_dev_kit`. Ask the user:

> "I'll save the data to `ai_dev_kit.<schema>`. What schema name would you like to use? (You can also specify a different catalog.)"

### Create Infrastructure in the Script

```python
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
```

### Save to Volume as Parquet (Never Tables)

Always save raw data to a Volume — it feeds downstream Spark Declarative Pipelines:

```python
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

df_customers.write.mode("overwrite").parquet(f"{VOLUME_PATH}/customers")
df_orders.write.mode("overwrite").parquet(f"{VOLUME_PATH}/orders")
df_tickets.write.mode("overwrite").parquet(f"{VOLUME_PATH}/tickets")
```

## Raw Data Only

**Generate raw, transactional data — no pre-aggregated fields** unless explicitly requested. No `total_orders`, `sum_revenue`, `avg_csat`. The downstream SDP pipeline computes aggregations.

## Data Volume for Aggregation

Generate enough rows so patterns survive downstream GROUP BY:

| Grain | Minimum Rows |
|-------|-------------|
| Daily time series | 50–100/day |
| Per category | 500+ per category |
| Per customer | 5–20 events/customer |
| Total | 10K–50K minimum |

## Domain-Specific Recipe Templates

### E-Commerce (Customers + Orders + Line Items)

```python
# Customers
.withColumn("customer_id", LongType(), minValue=10000, uniqueValues=N_CUSTOMERS)
.withColumn("signup_date", "date", data_range=dg.DateRange("2020-01-01", END, "days=1"), random=True)
.withColumn("segment", "string", values=["Consumer", "Business", "Enterprise"], weights=[70, 20, 10])
.withColumn("country", "string", values=["US", "UK", "DE", "FR", "JP"], weights=[50, 15, 12, 10, 8])

# Orders
.withColumn("order_id", "string", template=r'ORD-dddddddd', uniqueValues=N_ORDERS)
.withColumn("order_date", "date", data_range=dg.DateRange(START, END, "days=1"), random=True)
.withColumn("status", "string", values=["completed", "pending", "cancelled", "refunded"], weights=[80, 10, 5, 5])
.withColumn("channel", "string", values=["web", "mobile", "in-store", "phone"], weights=[45, 35, 15, 5])

# Line Items
.withColumn("line_id", "string", template=r'LI-dddddddd')
.withColumn("quantity", "int", minValue=1, maxValue=20, distribution=dist.Exponential(rate=0.5), random=True)
.withColumn("unit_price", "decimal(10,2)", minValue=0.99, maxValue=999.99, distribution=dist.Gamma(2.0, 50.0), random=True)
```

### IoT / Sensor Data

```python
.withColumn("device_id", "string", template=r'SENSOR-dddd', uniqueValues=200)
.withColumn("location_id", "string", values=[f"LOC-{i:03d}" for i in range(50)], baseColumn="device_id")
.withColumn("reading_ts", "timestamp", data_range=dg.DateRange(START, END, "seconds=30"), random=True)
.withColumn("temperature_c", "double", minValue=-10, maxValue=50, distribution=dist.Normal(mean=22, stddev=5), random=True)
.withColumn("humidity_pct", "double", minValue=10, maxValue=100, distribution=dist.Normal(mean=55, stddev=12), random=True)
.withColumn("battery_pct", "double", minValue=0, maxValue=100, distribution=dist.Beta(5, 2), random=True)
.withColumn("anomaly_flag", "boolean", expr="temperature_c > 40 OR humidity_pct > 90", baseColumn=["temperature_c", "humidity_pct"])
```

### Healthcare / Clinical

```python
.withColumn("patient_id", "string", template=r'PAT-dddddddd', uniqueValues=5000)
.withColumn("encounter_date", "date", data_range=dg.DateRange(START, END, "days=1"), random=True)
.withColumn("encounter_type", "string", values=["outpatient", "inpatient", "emergency", "telehealth"], weights=[50, 15, 10, 25])
.withColumn("diagnosis_code", "string", template=r'Add.d', values=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"])
.withColumn("length_of_stay", "int", minValue=0, maxValue=30, distribution=dist.Exponential(rate=0.3), random=True)
.withColumn("total_charge", "decimal(10,2)", minValue=100, maxValue=100000, distribution=dist.Gamma(2.0, 5000.0), random=True)
```

### Financial Transactions

```python
.withColumn("txn_id", "string", template=r'TXN-dddddddddd', uniqueValues=N_TXNS)
.withColumn("account_id", "string", template=r'ACCT-dddddddd', uniqueValues=N_ACCOUNTS)
.withColumn("txn_type", "string", values=["debit", "credit", "transfer", "fee"], weights=[40, 35, 20, 5])
.withColumn("amount", "decimal(12,2)", minValue=0.01, maxValue=50000, distribution=dist.Gamma(1.5, 200.0), random=True)
.withColumn("currency", "string", values=["USD", "EUR", "GBP", "JPY"], weights=[60, 20, 10, 10])
.withColumn("is_fraud", "boolean", expr="case when rand() < 0.02 then true else false end")
.withColumn("merchant_category", "string", values=["retail", "food", "travel", "services", "online"], weights=[25, 25, 15, 15, 20])
```

## Script Structure

```python
"""Generate synthetic [use case] data using dbldatagen."""
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import IntegerType, StringType, LongType
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "ai_dev_kit"
SCHEMA  = "my_schema"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

N_CUSTOMERS = 2500
N_ORDERS    = 25000
N_TICKETS   = 8000

# Last 6 months from today
END_DATE   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

START = START_DATE.strftime("%Y-%m-%d 00:00:00")
END   = END_DATE.strftime("%Y-%m-%d 23:59:59")

SEED = 42

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()

# ... generation and save code below
```

## Complete Example

Save as `scripts/generate_data.py`:

```python
"""Generate synthetic customer, order, and ticket data using dbldatagen."""
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import LongType
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "ai_dev_kit"
SCHEMA  = "my_schema"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

N_CUSTOMERS = 2500
N_ORDERS    = 25000
N_TICKETS   = 8000

END_DATE   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)
INC_END    = END_DATE - timedelta(days=21)
INC_START  = INC_END - timedelta(days=10)

START     = START_DATE.strftime("%Y-%m-%d 00:00:00")
END       = END_DATE.strftime("%Y-%m-%d 23:59:59")
INC_S     = INC_START.strftime("%Y-%m-%d 00:00:00")
INC_E     = INC_END.strftime("%Y-%m-%d 23:59:59")
PRE_INC_E = (INC_START - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")

SEED = 42

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")
print(f"Infrastructure ready: {CATALOG}.{SCHEMA}")

# =============================================================================
# 1. CUSTOMERS (master table)
# =============================================================================
print(f"Generating {N_CUSTOMERS:,} customers...")

customers_spec = (
    dg.DataGenerator(spark, name="customers", rows=N_CUSTOMERS,
                     partitions=4, randomSeedMethod="hash_fieldname",
                     randomSeed=SEED)
    .withColumn("customer_id", LongType(),
                minValue=100000, uniqueValues=N_CUSTOMERS)
    .withColumn("company_name", "string",
                template=r'\w \w Inc|\w \w Ltd|\w \w Corp|\w Technologies',
                baseColumn="customer_id")
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
                baseColumn="customer_id")
    .withColumn("region", "string",
                values=["North", "South", "East", "West"],
                weights=[40, 25, 20, 15],
                baseColumn="customer_id")
    .withColumn("_r", "double", minValue=0.0, maxValue=1.0,
                random=True, omit=True)
    .withColumn("arr", "decimal(12,2)",
                expr="""round(case
                    when tier = 'Enterprise' then exp(10.5 + _r * 1.0)
                    when tier = 'Pro'        then exp(8.0  + _r * 0.8)
                    else 0
                end, 2)""",
                baseColumn=["tier", "_r"])
)
df_customers = customers_spec.build().cache()
print(f"  {df_customers.count():,} customers")

# =============================================================================
# 2. ORDERS (references customers)
# =============================================================================
print(f"Generating {N_ORDERS:,} orders...")

orders_spec = (
    dg.DataGenerator(spark, name="orders", rows=N_ORDERS,
                     partitions=4, randomSeedMethod="hash_fieldname",
                     randomSeed=SEED)
    .withColumn("_cust_base", LongType(),
                minValue=100000, uniqueValues=N_CUSTOMERS,
                random=True, omit=True)
    .withColumn("customer_id", LongType(),
                minValue=100000, baseColumn="_cust_base",
                baseColumnType="hash", uniqueValues=N_CUSTOMERS)
    .withColumn("order_id", "string", template=r'ORD-dddddd')
    .withColumn("_r", "double", minValue=0.0, maxValue=1.0,
                random=True, omit=True)
    .withColumn("order_date", "date",
                data_range=dg.DateRange(START, END, "days=1"), random=True)
    .withColumn("status", "string",
                values=["completed", "pending", "cancelled"],
                weights=[85, 10, 5], random=True)
)
df_orders_raw = orders_spec.build()

# Join to get tier for correlated amounts
df_orders = df_orders_raw.join(
    df_customers.select("customer_id", "tier"), on="customer_id", how="left"
).withColumn("amount",
    __import__("pyspark.sql.functions", fromlist=["expr"]).expr(
        """round(case
            when tier = 'Enterprise' then exp(7 + rand() * 2.0)
            when tier = 'Pro'        then exp(5 + rand() * 1.5)
            else                          exp(3 + rand() * 1.2)
        end, 2)"""
    )
).drop("tier")
print(f"  {df_orders.count():,} orders")

# =============================================================================
# 3. TICKETS (references customers; incident spike via union)
# =============================================================================
print(f"Generating {N_TICKETS:,} tickets...")

N_INCIDENT = int(N_TICKETS * 0.12)   # ~12% in 10-day spike window
N_NORMAL   = N_TICKETS - N_INCIDENT

def ticket_spec(rows, date_start, date_end, cat_weights, seed_val):
    return (
        dg.DataGenerator(spark, rows=rows, partitions=4,
                         randomSeedMethod="hash_fieldname",
                         randomSeed=seed_val)
        .withColumn("_cust_base", LongType(),
                    minValue=100000, uniqueValues=N_CUSTOMERS,
                    random=True, omit=True)
        .withColumn("customer_id", LongType(),
                    minValue=100000, baseColumn="_cust_base",
                    baseColumnType="hash", uniqueValues=N_CUSTOMERS)
        .withColumn("ticket_id", "string", template=r'TKT-dddddd')
        .withColumn("category", "string",
                    values=["Auth", "Network", "Billing", "Account"],
                    weights=cat_weights, random=True)
        .withColumn("created_at", "date",
                    data_range=dg.DateRange(date_start, date_end, "days=1"),
                    random=True)
        .withColumn("_res", "double", minValue=0, maxValue=500,
                    distribution=dist.Exponential(rate=1/24),
                    random=True, omit=True)
        .withColumn("resolution_hours", "double",
                    expr="round(_res, 1)", baseColumn="_res")
        .withColumn("priority", "string",
                    expr="""case
                        when _res < 4  then 'Critical'
                        when _res < 12 then 'High'
                        when _res < 48 then 'Medium'
                        else                'Low'
                    end""",
                    baseColumn="_res")
        .withColumn("csat_score", "int",
                    expr="""case
                        when _res <  4 then 5
                        when _res < 12 then 4
                        when _res < 36 then 3
                        when _res < 72 then 2
                        else 1
                    end""",
                    baseColumn="_res")
    )

df_normal   = ticket_spec(N_NORMAL,   START,  PRE_INC_E, [25, 30, 25, 20], SEED).build()
df_incident = ticket_spec(N_INCIDENT, INC_S,  INC_E,     [65, 15, 10, 10], SEED + 1).build()
df_tickets  = df_normal.union(df_incident)
print(f"  {df_tickets.count():,} tickets ({N_INCIDENT:,} during incident)")

# =============================================================================
# 4. SAVE TO VOLUME
# =============================================================================
print(f"\nSaving to {VOLUME_PATH}...")
df_customers.write.mode("overwrite").parquet(f"{VOLUME_PATH}/customers")
df_orders.write.mode("overwrite").parquet(f"{VOLUME_PATH}/orders")
df_tickets.write.mode("overwrite").parquet(f"{VOLUME_PATH}/tickets")
print("Done!")

# =============================================================================
# 5. VALIDATION
# =============================================================================
print("\n=== VALIDATION ===")
print("Tier distribution:")
df_customers.groupBy("tier").count().orderBy("tier").show()

print("Avg order amount by tier (joined):")
df_orders.join(df_customers.select("customer_id","tier"), on="customer_id") \
         .groupBy("tier").avg("amount").orderBy("tier").show()

incident_ct = df_tickets.filter(
    (df_tickets.created_at >= INC_S[:10]) &
    (df_tickets.created_at <= INC_E[:10])
).count()
print(f"Incident period tickets: {incident_ct:,} ({incident_ct/N_TICKETS*100:.1f}%)")

print("Category mix during incident:")
df_tickets.filter(
    (df_tickets.created_at >= INC_S[:10]) &
    (df_tickets.created_at <= INC_E[:10])
).groupBy("category").count().orderBy("count", ascending=False).show()
```

Execute using `run_python_file_on_databricks` tool:
- `file_path`: "scripts/generate_data.py"

If it fails, edit the file and re-run with the same `cluster_id` and `context_id`.

### Validate Generated Data

After execution, use `get_volume_folder_details` tool to verify:
- `volume_path`: "ai_dev_kit/my_schema/raw_data/customers"
- `format`: "parquet"
- `table_stat_level`: "SIMPLE"

## When to Use dbldatagen vs Faker

| Use Case | dbldatagen | Faker |
|----------|------------|-------|
| Millions/billions of rows | Best choice | Too slow |
| Purely Spark-native pipeline | Best choice | Needs pandas |
| Realistic text (names, addresses) | Template-based | Best choice |
| Complex multi-step business logic | expr can be verbose | Python loops |
| Referential integrity at scale | hash-based FKs | Memory-limited |
| Reproducibility by default | randomSeed | Requires seeding |
| Delta Live Tables / SDP support | Native | Needs workarounds |
| Streaming data generation | Native (withStreaming) | Not supported |
| Schema inference from existing data | DataAnalyzer | Not supported |
| CDC simulation | scriptMerge | Manual |
| Complex types (struct, array, JSON) | withStructColumn | Manual |
| Constraints / business rules | withSqlConstraint | Manual |

## Best Practices

1. **Install first**: `%pip install dbldatagen` before any imports
2. **`randomSeedMethod="hash_fieldname"`**: Use on every DataGenerator for reproducibility
3. **Cache parent tables**: `.cache()` after building — referenced multiple times for joins
4. **`omit=True` helper columns**: Use freely for intermediate values; they don't appear in output
5. **`baseColumn` for determinism**: Drive child columns from parent with `baseColumn="col"` (not `random=True`) when you want the same input to always produce the same output
6. **`baseColumnType="hash"` for FK**: Hash the base value to generate unique-but-bounded foreign keys
7. **`baseColumn` on a string column — avoid; use numeric IDs instead**: `baseColumnType="hash"` works but causes uneven distribution due to hash collisions + modulo mapping. The correct approach is to generate IDs as numeric (`LongType`, `minValue/maxValue`) so dbldatagen can map linearly into the range:
   ```python
   # Bad: String ID + hash — uneven distribution
   .withColumn("customer_id", "string", template=r'CUST-ddddd', random=True)
   .withColumn("_street_num", "int", minValue=100, maxValue=9999,
               baseColumn="customer_id", baseColumnType="hash", omit=True)

   # Good: Numeric ID — linear mapping, uniform distribution
   .withColumn("customer_id", LongType(), minValue=10000, maxValue=99999, random=True)
   .withColumn("_street_num", "int", minValue=100, maxValue=9999,
               baseColumn="customer_id", omit=True)
   ```
8. **`unique=True` is NOT valid** — use `uniqueValues=N` instead. `unique=True` raises `DataGenError: invalid column option unique`. For primary keys, use `uniqueValues=N_ROWS` with `random=True`:
   ```python
   .withColumn("order_id", "string", template=r'ORD-dddddddd', uniqueValues=N_ORDERS, random=True)
   ```
9. **Two-spec union for spikes**: Generate separate specs for normal vs. anomaly periods, then union
10. **Join for correlated cross-table fields**: After building, join parent's attribute onto child if needed
11. **Raw data only**: No `total_x`, `sum_x`, `avg_x` — SDP pipeline computes those
12. **Save to Volume**: Write parquet to `/Volumes/{catalog}/{schema}/raw_data/<name>`
13. **Dynamic dates**: Always use `datetime.now() - timedelta(days=180)` for last 6 months
14. **Validate after generation**: Print `.count()` and distribution checks before saving
15. **Use DataAnalyzer for existing tables**: When mimicking production data, start with `scriptDataGeneratorFromData()` then customize
16. **Constraints need headroom**: Generate ~25% more rows than needed if constraints will filter

## Related Skills

- **[spark-declarative-pipelines](../spark-declarative-pipelines/SKILL.md)** — build bronze/silver/gold pipelines on top of generated data
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** — visualize generated data
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — manage catalogs, schemas, and volumes
- **[databricks-free-tier-guardrails](../databricks-free-tier-guardrails/SKILL.md)** — compatibility check for serverless compute
- **[databricks-geospatial](../databricks-geospatial/SKILL.md)** — generate synthetic geo data with lat/lon columns
