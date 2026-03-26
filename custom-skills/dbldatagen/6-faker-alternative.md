# Using Faker with dbldatagen

The best approach: use Faker to generate a **pool of realistic unique values** on the driver, then feed them into dbldatagen's `values=[]` parameter. This gives you Faker's realism with dbldatagen's speed, reproducibility, distributions, and column correlation.

```
┌──────────────────────────────────────────────────────────────────┐
│  Recommended Pattern                                             │
│                                                                  │
│  1. Faker (driver)  ──▶  Generate 500 unique names              │
│  2. dbldatagen      ──▶  values=faker_names, weights=[...]      │
│                          baseColumn, expr, distributions...      │
│                                                                  │
│  Result: Realistic text + fast Spark generation + reproducible   │
│                                                                  │
│  Anti-Pattern (slow):                                            │
│  Faker via Pandas UDF ──▶ Python per-row on every executor       │
│  Only use when you need unique values per row (e.g., emails)     │
└──────────────────────────────────────────────────────────────────┘
```

## When to Use Which Approach

| Need | Best Approach |
|------|--------------|
| Realistic names for 1M rows | Faker pool → `values=[]` in dbldatagen |
| Realistic company names | Faker pool → `values=[]` |
| Realistic addresses | Faker pool → `values=[]` |
| Job titles | Faker pool → `values=[]` |
| Realistic cities / states | Faker pool → `values=[]` with `weights=[]` |
| Every row needs a unique email | Faker via Pandas UDF (only option) |
| Every row needs a unique SSN | Faker via Pandas UDF (only option) |
| Pattern-based IDs (`ORD-123456`) | dbldatagen `template=` (no Faker needed) |
| IP addresses | dbldatagen `template=r'\n.\n.\n.\n'` (no Faker needed) |
| Correlated columns | dbldatagen `baseColumn` + `expr` |
| Non-linear distributions | dbldatagen `distribution=dist.Gamma(...)` |

---

## Installation

```python
# Via MCP tool: execute_databricks_command
# code: "%pip install faker"

# In notebooks:
%pip install faker
```

---

## Pattern 1: Faker Pool → dbldatagen `values=[]` (Preferred)

Generate a small set of unique values with Faker on the driver, then use them as the value pool in dbldatagen. Fast, reproducible, and fully Spark-native at generation time.

### Basic Example

```python
from faker import Faker
import dbldatagen as dg
import dbldatagen.distributions as dist

fake = Faker()
Faker.seed(42)  # reproducible Faker output

# Generate pools of realistic values (runs on driver -- fast for hundreds)
FAKE_NAMES     = [fake.name() for _ in range(500)]
FAKE_COMPANIES = [fake.company() for _ in range(300)]
FAKE_ADDRESSES = [fake.address().replace('\n', ', ') for _ in range(400)]
FAKE_CITIES    = [fake.city() for _ in range(200)]
FAKE_EMAILS    = [fake.email() for _ in range(1000)]
FAKE_PHONES    = [fake.phone_number() for _ in range(500)]
FAKE_JOBS      = [fake.job() for _ in range(150)]

# Use in dbldatagen -- pure Spark, no Python UDF overhead
customers_spec = (
    dg.DataGenerator(spark, rows=100_000, partitions=8,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("customer_id", "long", minValue=10000, uniqueValues=100_000)
    .withColumn("name", "string", values=FAKE_NAMES, random=True)
    .withColumn("company", "string", values=FAKE_COMPANIES, random=True)
    .withColumn("email", "string", values=FAKE_EMAILS, random=True)
    .withColumn("phone", "string", values=FAKE_PHONES, random=True)
    .withColumn("address", "string", values=FAKE_ADDRESSES, random=True)
    .withColumn("city", "string", values=FAKE_CITIES, random=True)
    .withColumn("job_title", "string", values=FAKE_JOBS, random=True)
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10], random=True)
    .build()
)

customers_spec.show(5, truncate=False)
```

### With Weighted Distributions

Weight the Faker pool to reflect real-world frequency:

```python
# Top 20 US cities with approximate population weights
FAKE_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "Indianapolis", "San Francisco", "Seattle", "Denver", "Washington"
]
CITY_WEIGHTS = [18, 13, 9, 8, 6, 5, 5, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2]

.withColumn("city", "string", values=FAKE_CITIES, weights=CITY_WEIGHTS, random=True)
```

### Locale-Specific Pools

```python
fake_jp = Faker('ja_JP')
fake_de = Faker('de_DE')
fake_fr = Faker('fr_FR')

JAPANESE_NAMES = [fake_jp.name() for _ in range(200)]
GERMAN_COMPANIES = [fake_de.company() for _ in range(200)]
FRENCH_ADDRESSES = [fake_fr.address().replace('\n', ', ') for _ in range(200)]

# Use in dbldatagen based on country column
.withColumn("country", "string",
            values=["US", "JP", "DE", "FR"], weights=[50, 20, 15, 15], random=True)
# Then use expr to pick from locale-appropriate pool
# (or generate separate specs per country and union)
```

---

## Pattern 2: Faker Pool + Correlated Columns

Combine Faker pools with dbldatagen's `baseColumn` and `expr` for correlated realistic data:

```python
fake = Faker()
Faker.seed(42)

FAKE_NAMES = [fake.name() for _ in range(500)]
FAKE_COMPANIES = [fake.company() for _ in range(300)]

spec = (
    dg.DataGenerator(spark, rows=50_000, partitions=8,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("customer_id", "long", minValue=10000, uniqueValues=50_000)

    # Faker pool for realistic text
    .withColumn("name", "string", values=FAKE_NAMES, random=True)
    .withColumn("company", "string", values=FAKE_COMPANIES, random=True)

    # dbldatagen for structure and distributions
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10], random=True)
    .withColumn("region", "string",
                values=["North", "South", "East", "West"], weights=[40, 25, 20, 15], random=True)

    # Correlated columns (pure dbldatagen -- no Faker needed here)
    .withColumn("_r", "double", minValue=0.0, maxValue=1.0, random=True, omit=True)
    .withColumn("arr", "decimal(12,2)",
                expr="""round(case
                    when tier = 'Enterprise' then exp(10.5 + _r * 1.0)
                    when tier = 'Pro'        then exp(8.0 + _r * 0.8)
                    else 0
                end, 2)""",
                baseColumn=["tier", "_r"])

    # Dates and timestamps (pure dbldatagen)
    .withColumn("signup_date", "date",
                data_range=dg.DateRange("2020-01-01", "2024-12-31", "days=1"), random=True)

    .build()
)
```

---

## Pattern 3: Faker Pool for Multi-Table Referential Integrity

```python
fake = Faker()
Faker.seed(42)

UNIQUE_CUSTOMERS = 5000
FAKE_NAMES = [fake.name() for _ in range(UNIQUE_CUSTOMERS)]
FAKE_COMPANIES = [fake.company() for _ in range(500)]

# 1. Parent table
customers_spec = (
    dg.DataGenerator(spark, rows=UNIQUE_CUSTOMERS, partitions=4,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("customer_id", "long", minValue=100000, uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("name", "string", values=FAKE_NAMES, baseColumn="customer_id")
    .withColumn("company", "string", values=FAKE_COMPANIES, random=True)
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
                baseColumn="customer_id")
)
df_customers = customers_spec.build()

# Write to Delta (serverless-safe, no .cache())
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}._tmp_customers")
customer_lookup = spark.table(f"{CATALOG}.{SCHEMA}._tmp_customers").select(
    "customer_id", "tier"
)

# 2. Child table with FK
orders_spec = (
    dg.DataGenerator(spark, rows=50_000, partitions=8,
                     randomSeedMethod="hash_fieldname", randomSeed=42)
    .withColumn("_cust_base", "long",
                minValue=100000, uniqueValues=UNIQUE_CUSTOMERS, random=True, omit=True)
    .withColumn("customer_id", "long",
                minValue=100000, baseColumn="_cust_base",
                baseColumnType="hash", uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("order_id", "string", template=r'ORD-dddddddd')
    .withColumn("order_date", "date",
                data_range=dg.DateRange("2024-01-01", "2024-12-31", "days=1"), random=True)
    .withColumn("status", "string",
                values=["completed", "pending", "cancelled"], weights=[85, 10, 5], random=True)
)
df_orders = orders_spec.build().dropDuplicates(["order_id"])

# Join for correlated amounts
df_orders_enriched = df_orders.join(customer_lookup, on="customer_id", how="left")
```

---

## Pattern 4: Pandas UDFs (When Every Row Needs Unique Values)

Only use Pandas UDFs when you truly need a **unique value per row** (e.g., unique emails, unique SSNs). This is slower because Python runs on every executor.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from faker import Faker
import pandas as pd

@F.pandas_udf(StringType())
def fake_unique_email(ids: pd.Series) -> pd.Series:
    fake = Faker()
    return pd.Series([fake.unique.email() for _ in range(len(ids))])

@F.pandas_udf(StringType())
def fake_unique_ssn(ids: pd.Series) -> pd.Series:
    fake = Faker('en_US')
    return pd.Series([fake.ssn() for _ in range(len(ids))])

# Use sparingly -- only for columns that must be unique per row
df = (
    spark.range(0, 10_000, numPartitions=8)
    .withColumn("email", fake_unique_email(F.col("id")))
    .withColumn("ssn", fake_unique_ssn(F.col("id")))
)
```

### Performance Comparison

| Approach | 100K rows | 1M rows | 10M rows |
|----------|----------|---------|----------|
| Faker pool → `values=[]` | ~5 sec | ~15 sec | ~60 sec |
| Faker Pandas UDF | ~30 sec | ~5 min | ~50 min |
| dbldatagen native template | ~3 sec | ~10 sec | ~40 sec |

**Rule:** Use Faker pools for 95% of cases. Reserve Pandas UDFs for the rare case where every row must have a globally unique text value.

---

## Pool Sizing Guide

| Pool Size | Good For |
|-----------|----------|
| 50-100 values | Cities, states, departments, categories |
| 200-500 values | Names, companies, job titles |
| 500-1000 values | Addresses, emails (with some repeats) |
| 1000-5000 values | When you need low repeat rate across 100K+ rows |
| = N rows | When every row needs a unique value (consider Pandas UDF instead) |

```python
# How many unique names for your dataset?
# Rule of thumb: pool size = sqrt(N_rows) gives ~50% repeat rate
# pool size = N_rows / 10 gives ~10% repeat rate
import math

N_ROWS = 100_000
pool_low_repeat  = N_ROWS // 10    # 10,000 names -- ~10% repeat rate
pool_med_repeat  = int(math.sqrt(N_ROWS))  # 316 names -- ~50% repeat rate
pool_high_repeat = 100             # 100 names -- very common repeats
```

---

## Common Faker Providers Quick Reference

| Category | Method | Output Example |
|----------|--------|---------------|
| **Person** | `fake.name()` | `John Smith` |
| | `fake.first_name()` | `John` |
| | `fake.last_name()` | `Smith` |
| | `fake.prefix()` | `Dr.` |
| **Company** | `fake.company()` | `Acme Corp` |
| | `fake.catch_phrase()` | `Innovative solutions` |
| | `fake.bs()` | `leverage synergies` |
| **Address** | `fake.address()` | `123 Main St, City, ST 12345` |
| | `fake.street_address()` | `123 Main St` |
| | `fake.city()` | `Springfield` |
| | `fake.state_abbr()` | `CA` |
| | `fake.zipcode()` | `90210` |
| | `fake.country()` | `United States` |
| | `fake.latitude()` / `fake.longitude()` | `40.7128` / `-74.0060` |
| **Contact** | `fake.email()` | `john@example.com` |
| | `fake.phone_number()` | `(555) 123-4567` |
| | `fake.url()` | `https://www.example.com` |
| | `fake.user_name()` | `jsmith42` |
| | `fake.ipv4()` | `192.168.1.42` |
| **Financial** | `fake.credit_card_number()` | `4111111111111111` |
| | `fake.iban()` | `GB82 WEST 1234 5698 7654 32` |
| | `fake.currency_code()` | `USD` |
| **Identity** | `fake.ssn()` | `123-45-6789` |
| | `fake.job()` | `Software Engineer` |
| | `fake.date_of_birth()` | `1990-03-15` |
| **Text** | `fake.sentence()` | Single sentence |
| | `fake.paragraph()` | Multi-sentence paragraph |
| | `fake.text(max_nb_chars=200)` | Bounded text block |
| **Misc** | `fake.uuid4()` | `a1b2c3d4-...` |
| | `fake.color_name()` | `Blue` |
| | `fake.license_plate()` | `ABC-1234` |
| | `fake.file_name()` | `report.pdf` |

### Common Locales

| Locale | Language/Country |
|--------|-----------------|
| `en_US` | English (US) -- default |
| `en_GB` | English (UK) |
| `fr_FR` | French |
| `de_DE` | German |
| `es_ES` | Spanish |
| `it_IT` | Italian |
| `ja_JP` | Japanese |
| `zh_CN` | Chinese (Simplified) |
| `ko_KR` | Korean |
| `pt_BR` | Portuguese (Brazil) |
| `hi_IN` | Hindi |

---

## Complete Example: Faker Pool + dbldatagen

Save as `scripts/generate_with_faker_pool.py`:

```python
"""Generate synthetic data using Faker pools + dbldatagen."""
from faker import Faker
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import LongType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "ai_dev_kit"
SCHEMA = "my_schema"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
SEED = 42

N_CUSTOMERS = 10_000
N_ORDERS = 100_000

END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)
START = START_DATE.strftime("%Y-%m-%d 00:00:00")
END = END_DATE.strftime("%Y-%m-%d 23:59:59")

# =============================================================================
# FAKER POOLS (runs on driver -- fast)
# =============================================================================
fake = Faker()
Faker.seed(SEED)

FAKE_NAMES     = list(set(fake.name() for _ in range(2000)))[:1000]
FAKE_COMPANIES = list(set(fake.company() for _ in range(600)))[:500]
FAKE_ADDRESSES = list(set(fake.address().replace('\n', ', ') for _ in range(800)))[:500]
FAKE_EMAILS    = list(set(fake.email() for _ in range(2000)))[:1500]
FAKE_PHONES    = list(set(fake.phone_number() for _ in range(1000)))[:500]
FAKE_JOBS      = list(set(fake.job() for _ in range(300)))[:200]

print(f"Faker pools: {len(FAKE_NAMES)} names, {len(FAKE_COMPANIES)} companies, "
      f"{len(FAKE_EMAILS)} emails, {len(FAKE_JOBS)} jobs")

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")

# =============================================================================
# 1. CUSTOMERS
# =============================================================================
print(f"Generating {N_CUSTOMERS:,} customers...")

customers_spec = (
    dg.DataGenerator(spark, name="customers", rows=N_CUSTOMERS,
                     partitions=8, randomSeedMethod="hash_fieldname",
                     randomSeed=SEED, seedColumnName="_seed")
    .withColumn("customer_id", LongType(), minValue=100000, uniqueValues=N_CUSTOMERS)
    .withColumn("name", "string", values=FAKE_NAMES, random=True)
    .withColumn("company", "string", values=FAKE_COMPANIES, random=True)
    .withColumn("email", "string", values=FAKE_EMAILS, random=True)
    .withColumn("phone", "string", values=FAKE_PHONES, random=True)
    .withColumn("address", "string", values=FAKE_ADDRESSES, random=True)
    .withColumn("job_title", "string", values=FAKE_JOBS, random=True)
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
                baseColumn="customer_id")
    .withColumn("region", "string",
                values=["North", "South", "East", "West"], weights=[40, 25, 20, 15],
                baseColumn="customer_id")
    .withColumn("_r", "double", minValue=0.0, maxValue=1.0, random=True, omit=True)
    .withColumn("arr", "decimal(12,2)",
                expr="""round(case
                    when tier = 'Enterprise' then exp(10.5 + _r * 1.0)
                    when tier = 'Pro'        then exp(8.0 + _r * 0.8)
                    else 0
                end, 2)""",
                baseColumn=["tier", "_r"])
    .withColumn("signup_date", "date",
                data_range=dg.DateRange("2020-01-01", END, "days=1"), random=True)
)

df_customers = customers_spec.build()
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}._tmp_customers")
customer_lookup = spark.table(f"{CATALOG}.{SCHEMA}._tmp_customers").select(
    "customer_id", "tier"
)
print(f"  {spark.table(f'{CATALOG}.{SCHEMA}._tmp_customers').count():,} customers")

# =============================================================================
# 2. ORDERS (FK to customers)
# =============================================================================
print(f"Generating {N_ORDERS:,} orders...")

orders_spec = (
    dg.DataGenerator(spark, name="orders", rows=N_ORDERS,
                     partitions=16, randomSeedMethod="hash_fieldname",
                     randomSeed=SEED)
    .withColumn("_cust_base", LongType(),
                minValue=100000, uniqueValues=N_CUSTOMERS, random=True, omit=True)
    .withColumn("customer_id", LongType(),
                minValue=100000, baseColumn="_cust_base",
                baseColumnType="hash", uniqueValues=N_CUSTOMERS)
    .withColumn("order_id", "string", template=r'ORD-dddddddd')
    .withColumn("order_date", "date",
                data_range=dg.DateRange(START, END, "days=1"), random=True)
    .withColumn("status", "string",
                values=["completed", "pending", "cancelled"], weights=[85, 10, 5], random=True)
    .withColumn("channel", "string",
                values=["web", "mobile", "in-store", "phone"], weights=[45, 35, 15, 5], random=True)
)

df_orders = orders_spec.build().dropDuplicates(["order_id"])

# Join for correlated amounts
import pyspark.sql.functions as F
df_orders = (df_orders
    .join(customer_lookup, on="customer_id", how="left")
    .withColumn("amount",
        F.expr("""round(case
            when tier = 'Enterprise' then exp(7 + rand() * 2.0)
            when tier = 'Pro'        then exp(5 + rand() * 1.5)
            else                          exp(3 + rand() * 1.2)
        end, 2)"""))
    .drop("tier")
)
print(f"  {df_orders.count():,} orders")

# =============================================================================
# 3. SAVE
# =============================================================================
print(f"\nSaving to {VOLUME_PATH}...")
spark.table(f"{CATALOG}.{SCHEMA}._tmp_customers") \
    .write.mode("overwrite").parquet(f"{VOLUME_PATH}/customers")
df_orders.write.mode("overwrite").parquet(f"{VOLUME_PATH}/orders")

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}._tmp_customers")
print("Done!")

# =============================================================================
# 4. VALIDATION
# =============================================================================
print("\n=== VALIDATION ===")
cust = spark.read.parquet(f"{VOLUME_PATH}/customers")
print(f"Customers: {cust.count():,}")
print(f"Unique names: {cust.select('name').distinct().count():,}")
cust.groupBy("tier").count().orderBy("tier").show()
cust.select("name", "company", "email", "job_title", "tier", "region").show(5, truncate=False)
```

Execute:
1. `execute_databricks_command` with `code: "%pip install faker dbldatagen"`
2. `run_python_file_on_databricks` with `file_path: "scripts/generate_with_faker_pool.py"`
