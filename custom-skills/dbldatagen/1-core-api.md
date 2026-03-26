# Core API Reference

## DataGenerator

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
- `randomSeedMethod="fixed"`: all columns share the same seed (less random but simpler)
- `randomSeed=42`: fixed seed for full reproducibility
- `randomSeed=-1`: non-repeatable random (different output every run)
- `seedColumnName="_id"`: rename the internal seed column (default is `id` -- rename if your data needs an `id` column)
- `verbose=True`: print detailed build information
- `debug=True`: enable debug-level logging

### Avoiding the `id` Column Conflict

The generator reserves `id` as its internal seed column. If you need a column named `id`, rename the seed:

```python
gen = dg.DataGenerator(spark, rows=10_000, seedColumnName="_seed_id",
                       randomSeedMethod="hash_fieldname")
# Now you can safely use "id" as a column name
gen = gen.withColumn("id", "string", template=r'USR-dddddddd')
```

### Debugging a Spec

```python
# After building, inspect the generation plan
spec = dg.DataGenerator(spark, rows=1000, verbose=True, debug=True,
                        randomSeedMethod="hash_fieldname")
spec = spec.withColumn("name", "string", template=r'\w \w')
df = spec.build()

# View the generation synopsis
spec.explain()
```

### Cloning a Spec

```python
# Create a modified copy without affecting the original
spec_v2 = spec.clone()
spec_v2 = spec_v2.withColumn("extra_col", "int", minValue=0, maxValue=100)
df_v2 = spec_v2.build()
```

---

## withColumn Reference

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
| `step=N` | Increment between values | `step=10` |
| `begin="..."` | Date/timestamp range start (alt to DateRange) | `begin="2024-01-01"` |
| `end="..."` | Date/timestamp range end | `end="2024-12-31"` |
| `interval="..."` | Date/timestamp step interval | `interval="days=1"` |
| `text=dg.ILText(...)` | Lorem Ipsum text generator | see text section |
| `text=dg.TemplateGenerator(...)` | Advanced template with word lists | see text section |

---

## withColumnSpec — Apply to Existing Schema

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

---

## withColumnSpecs — Bulk Pattern Matching

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

---

## Standard Datasets (Pre-Built)

dbldatagen includes pre-built dataset definitions for common scenarios:

```python
# List available datasets
print(dg.Datasets.list())

# List datasets that support streaming
print(dg.Datasets.list(pattern="basic.*", supportsStreaming=True))

# Get details about a dataset
dg.Datasets.describe("basic/user")

# Generate from a standard dataset
df = dg.Datasets(spark, "basic/user").get(rows=10_000, partitions=4).build()

# Standard dataset with extra options
df = dg.Datasets(spark, "basic/user").get(rows=10_000, dummyValues=10).build()
```

### Multi-Table Standard Datasets

```python
# Telephony dataset: plans, customers, and device events
ds = dg.Datasets(spark, "multi_table/telephony")
options = {"numPlans": 50, "numCustomers": 1000}

df_plans     = ds.get(table="plans", **options).build()
df_customers = ds.get(table="customers", **options).build()
df_events    = ds.get(table="deviceEvents", **options).build()
```

### Associated/Enriched Datasets

```python
# Get pre-joined or summary versions
ds = dg.Datasets(spark, "basic/user")
df_enriched = ds.getEnrichedDataset()
df_summary  = ds.getSummaryDataset()
```
