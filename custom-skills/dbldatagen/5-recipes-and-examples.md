# Domain Recipes, Complete Example, and Best Practices

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

---

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

---

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

---

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

---

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
17. **Rename seed column if you need `id`**: Use `seedColumnName="_seed_id"` in DataGenerator constructor
18. **Use `dropDuplicates`** after building tables with hash-based FKs to handle rare hash collisions
19. **Use `explain()`** after `build()` to inspect the generation plan for debugging
20. **Declare `baseColumn` for `expr` dependencies**: SQL expressions don't auto-detect column references -- always list referenced columns in `baseColumn` to ensure correct build order
21. **Don't modify specs after `build()`**: Use `clone()` to create derivative specs instead
22. **Faker text is NOT repeatable**: `fakerText()` columns produce different values each run -- use native templates when reproducibility matters
23. **`%pip install` must be first cell in DLT notebooks**: Before any `%md` or other cells

---

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `DataGenError: invalid column option unique` | Used `unique=True` | Use `uniqueValues=N` instead |
| `AnalysisException: cannot resolve 'col'` | Column referenced in `expr` before it's generated | Add `baseColumn=["col"]` to declare dependency |
| `Column 'id' already exists` | Conflict with internal seed column | Use `seedColumnName="_seed_id"` |
| `TypeError: unhashable type: 'list'` | Passed a list where a string is expected | Check `baseColumn` -- use string for single col, list for multiple |
| Empty DataFrame after build | `withSqlConstraint` filtered all rows | Increase `rows` or loosen the constraint |
| Duplicate FKs in child table | Hash collisions on `baseColumnType="hash"` | Add `.dropDuplicates(["fk_column"])` after build |
| `Py4JJavaError` on build | Spark SQL syntax error in `expr` | Test the expression with `spark.sql("SELECT " + expr)` first |
