# Row Coherence, Multi-Table Relationships, and Event Spikes

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

---

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

# 3. Handle potential hash collisions (rare but possible)
df_orders = df_orders.dropDuplicates(["order_id"])

# 4. Join to carry parent attributes into child (for correlated fields)
df_orders_enriched = df_orders.join(
    df_customers.select("customer_id", "tier"),
    on="customer_id", how="left"
)
```

### Hash Collision Handling

Hash-based foreign keys can rarely produce duplicates. Always `dropDuplicates` on unique columns:

```python
df_orders = orders_spec.build().dropDuplicates(["order_id"])
df_customers = customers_spec.build().dropDuplicates(["customer_id", "phone_number"])
```

### Master-Detail Row Calculation

For event-style child tables where each parent has multiple children:

```python
# Calculate total rows from the relationship
AVG_EVENTS_PER_CUSTOMER = 10
NUM_DAYS = 31
UNIQUE_CUSTOMERS = 5000

TOTAL_EVENTS = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS
# = 1,550,000 events

events_spec = dg.DataGenerator(spark, rows=TOTAL_EVENTS, partitions=8,
                               randomSeedMethod="hash_fieldname")
```

---

## Event Spikes (Two-Spec Pattern)

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
