# Exercise Bank

A curated set of exercises organized by topic and difficulty. Each exercise includes a problem statement, optional starter code, hints, and a reference solution.

---

## Table of Contents
1. [DataFrames & Transformations](#dataframes--transformations)
2. [Spark SQL](#spark-sql)
3. [Joins & Data Combination](#joins--data-combination)
4. [Window Functions](#window-functions)
5. [Delta Lake Core](#delta-lake-core)
6. [Delta Lake Optimization](#delta-lake-optimization)
7. [Structured Streaming](#structured-streaming)
8. [Spark Declarative Pipelines (SDP)](#spark-declarative-pipelines)
9. [Unity Catalog & Governance](#unity-catalog--governance)
10. [Performance Tuning](#performance-tuning)
11. [Fix It Exercises](#fix-it-exercises)
12. [Review It Exercises](#review-it-exercises)
13. [Certification-Style Questions](#certification-style-questions)

---

## DataFrames & Transformations

### DF-1: Customer Data Cleaner ⭐⭐
**Type:** Build It

**Problem:** You have a messy CSV of customer data. Clean it by: normalizing email to lowercase, extracting domain from email, trimming whitespace from names, removing rows with null customer_id, adding a `full_name` column.

**Starter data setup:**
```python
data = [
    (1, "  John ", " Doe  ", "John.Doe@GMAIL.COM", 35),
    (2, "Jane", "Smith", "jane@yahoo.com", None),
    (None, "Bob", "Jones", "BOB@outlook.com", 42),
    (4, "Alice ", None, "alice@company.io", 28),
    (5, "  Charlie", "Brown  ", None, 31),
]
schema = "customer_id INT, first_name STRING, last_name STRING, email STRING, age INT"
df = spark.createDataFrame(data, schema)
```

**Hints:**
1. Use `trim()` for whitespace, `lower()` for case
2. Use `split()` and element access for domain extraction
3. Use `concat_ws()` for full_name with trimmed parts
4. Chain `.filter()` for null removal

**Reference solution:**
```python
from pyspark.sql.functions import trim, lower, split, concat_ws, col

cleaned = (df
    .filter(col("customer_id").isNotNull())
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("email_domain", split(col("email"), "@")[1])
    .withColumn("full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))
)
```

**Teaching points:**
- `trim()` removes leading/trailing whitespace; there's also `ltrim()` and `rtrim()`
- `split()` returns an array; use `[1]` or `getItem(1)` to access elements
- Always filter nulls on key columns early in the pipeline
- `concat_ws` handles nulls gracefully (skips them) vs `concat` which propagates nulls

---

### DF-2: Multi-Aggregation Report ⭐⭐
**Type:** Build It

**Problem:** From an orders table, produce a summary showing per customer: total orders, total spend, average order value, first and last order dates, and the number of distinct products ordered.

**Starter data setup:**
```python
from datetime import date
orders = [
    (1, 101, date(2024,1,15), 250.00, "laptop"),
    (2, 101, date(2024,2,20), 45.00, "mouse"),
    (3, 101, date(2024,2,20), 120.00, "keyboard"),
    (4, 102, date(2024,1,10), 800.00, "laptop"),
    (5, 102, date(2024,3,05), 25.00, "cable"),
    (6, 103, date(2024,1,20), 500.00, "monitor"),
]
schema = "order_id INT, customer_id INT, order_date DATE, amount DOUBLE, product STRING"
df = spark.createDataFrame(orders, schema)
```

**Reference solution:**
```python
from pyspark.sql.functions import count, sum, avg, min, max, countDistinct, round

summary = (df
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("amount"), 2).alias("total_spend"),
        round(avg("amount"), 2).alias("avg_order_value"),
        min("order_date").alias("first_order"),
        max("order_date").alias("last_order"),
        countDistinct("product").alias("distinct_products")
    )
    .orderBy("customer_id")
)
```

---

### DF-3: Conditional Column Logic ⭐⭐
**Type:** Build It

**Problem:** Add a `customer_tier` column based on total spend: 'Platinum' (>$1000), 'Gold' ($500-$1000), 'Silver' ($100-$499), 'Bronze' (<$100). Also add `is_high_value` boolean (Platinum or Gold).

**Reference solution:**
```python
from pyspark.sql.functions import when, col

tiered = (summary
    .withColumn("customer_tier",
        when(col("total_spend") > 1000, "Platinum")
        .when(col("total_spend") >= 500, "Gold")
        .when(col("total_spend") >= 100, "Silver")
        .otherwise("Bronze")
    )
    .withColumn("is_high_value",
        col("customer_tier").isin("Platinum", "Gold")
    )
)
```

**Teaching points:**
- `when/otherwise` is the PySpark equivalent of SQL CASE WHEN
- Order matters — first matching condition wins
- `.isin()` is cleaner than chaining OR conditions

---

## Joins & Data Combination

### JOIN-1: The Silent Data Loss ⭐⭐⭐
**Type:** Fix It

**Problem:** A junior engineer wrote this code to create a customer order report. The business says some customers are missing from the report. Find and fix the bug.

**Broken code:**
```python
# Customer list (10 customers)
customers = spark.table("practice.data.customers")  # 10 rows
orders = spark.table("practice.data.orders")          # 25 rows

report = (customers
    .join(orders, "customer_id")  # BUG: what type of join is this?
    .groupBy("customer_id", "customer_name")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_spend")
    )
)
# Expected: 10 customers (some with 0 orders)
# Actual: only 7 customers appear
```

**The bug:** Default join in PySpark is INNER join. Customers with no orders are silently dropped.

**Fix:**
```python
report = (customers
    .join(orders, "customer_id", "left")  # LEFT join keeps all customers
    .groupBy("customer_id", "customer_name")
    .agg(
        count("order_id").alias("order_count"),  # Will be 0 for no-order customers
        coalesce(sum("amount"), lit(0)).alias("total_spend")
    )
)
```

**Teaching points:**
- PySpark default join is INNER — always specify the join type explicitly
- LEFT joins preserve all rows from the left DataFrame
- After a LEFT join, aggregation columns from the right side may be NULL — handle with coalesce

---

### JOIN-2: Ambiguous Column Explosion ⭐⭐
**Type:** Fix It

**Problem:** This join fails with "ambiguous column reference". Fix it.

**Broken code:**
```python
orders = spark.table("orders").select("customer_id", "amount", "status")
refunds = spark.table("refunds").select("customer_id", "amount", "reason")

result = orders.join(refunds, "customer_id", "left")
result.select("customer_id", "amount")  # ERROR: ambiguous reference to 'amount'
```

**Fix:**
```python
result = (orders.alias("o")
    .join(refunds.alias("r"), col("o.customer_id") == col("r.customer_id"), "left")
    .select(
        col("o.customer_id"),
        col("o.amount").alias("order_amount"),
        col("r.amount").alias("refund_amount"),
        col("r.reason")
    )
)
```

---

## Window Functions

### WIN-1: Top N Per Group ⭐⭐⭐
**Type:** Build It

**Problem:** Find the top 3 products by revenue within each category.

**Reference solution:**
```python
from pyspark.sql.functions import row_number, sum as _sum, col
from pyspark.sql.window import Window

product_revenue = (df
    .groupBy("category", "product_name")
    .agg(_sum("revenue").alias("total_revenue"))
)

w = Window.partitionBy("category").orderBy(col("total_revenue").desc())

top3 = (product_revenue
    .withColumn("rank", row_number().over(w))
    .filter(col("rank") <= 3)
)
```

---

### WIN-2: Running Total with Reset ⭐⭐⭐
**Type:** Build It

**Problem:** Calculate a running total of daily sales per store, resetting each month.

**Reference solution:**
```python
from pyspark.sql.functions import sum, year, month, col
from pyspark.sql.window import Window

w = (Window
    .partitionBy("store_id", year("sale_date"), month("sale_date"))
    .orderBy("sale_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result = df.withColumn("monthly_running_total", sum("daily_sales").over(w))
```

**Teaching points:**
- `rowsBetween(unboundedPreceding, currentRow)` creates a cumulative window
- Partition by the reset boundaries (store + year + month)
- This is different from `rangeBetween` which works on value ranges

---

### WIN-3: Gap Detection (Session Identification) ⭐⭐⭐⭐
**Type:** Build It

**Problem:** Given clickstream events (user_id, timestamp), identify user sessions where a session ends after 30 minutes of inactivity. Add a `session_id` column.

**Reference solution:**
```python
from pyspark.sql.functions import lag, col, unix_timestamp, sum as _sum, concat
from pyspark.sql.window import Window

w_user = Window.partitionBy("user_id").orderBy("event_timestamp")

sessions = (df
    .withColumn("prev_ts", lag("event_timestamp").over(w_user))
    .withColumn("gap_seconds",
        unix_timestamp("event_timestamp") - unix_timestamp("prev_ts"))
    .withColumn("new_session",
        when((col("gap_seconds") > 1800) | col("prev_ts").isNull(), 1).otherwise(0))
    .withColumn("session_num", _sum("new_session").over(w_user))
    .withColumn("session_id", concat(col("user_id"), lit("_"), col("session_num")))
)
```

**Teaching points:**
- `lag()` accesses the previous row's value
- The cumulative sum of `new_session` flags creates incrementing session numbers
- This pattern is a classic interview question and a real production pattern

---

## Delta Lake Core

### DELTA-1: MERGE Upsert ⭐⭐
**Type:** Build It

**Problem:** Write a MERGE that upserts new customer data into an existing customers table. Update existing customers (matched by customer_id) with new values. Insert new customers. Delete customers marked as `is_deleted = true` in the source.

**Reference solution (SQL):**
```sql
MERGE INTO catalog.schema.customers AS target
USING staging.new_customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.email = source.email,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED AND source.is_deleted = false THEN INSERT (
    customer_id, name, email, created_at, updated_at
) VALUES (
    source.customer_id, source.name, source.email,
    current_timestamp(), current_timestamp()
)
```

---

### DELTA-2: Time Travel Investigation ⭐⭐
**Type:** Build It

**Problem:** A bad MERGE was run 3 versions ago that introduced duplicates. Use Time Travel to: (1) find when it happened, (2) query the table before the bad merge, (3) restore the table.

**Reference solution:**
```sql
-- 1. Find when it happened
DESCRIBE HISTORY catalog.schema.customers;
-- Look for the MERGE operation with unexpected rowsUpdated count

-- 2. Query the clean version
SELECT count(*), count(DISTINCT customer_id)
FROM catalog.schema.customers VERSION AS OF 5;
-- Compare with current to confirm duplicates

-- 3. Restore
RESTORE TABLE catalog.schema.customers TO VERSION AS OF 5;
```

---

## Spark Declarative Pipelines

### SDP-1: Fix the Pipeline ⭐⭐⭐
**Type:** Fix It

**Problem:** This SDP pipeline has 5 bugs. Find them all.

**Broken code:**
```sql
-- Bug 1: Missing keyword
CREATE STREAMING TABLE bronze_events
AS SELECT * FROM read_files('s3://raw/events/', format => 'json')

-- Bug 2: Wrong function
CREATE STREAMING TABLE silver_events
AS SELECT
    event_id, user_id, event_type,
    CAST(timestamp AS TIMESTAMP) AS event_ts
FROM LIVE.bronze_events
WHERE event_id IS NOT NULL

-- Bug 3: Missing quality gate
CREATE MATERIALIZED VIEW gold_daily_summary
AS SELECT
    DATE(event_ts) AS event_date,
    event_type,
    COUNT(*) AS event_count
FROM LIVE.silver_events
GROUP BY 1, 2
```

**All 5 bugs:**
1. Missing `OR REFRESH`: should be `CREATE OR REFRESH STREAMING TABLE`
2. Missing `STREAM` on read_files: should be `STREAM read_files(...)`
3. Missing `STREAM()` wrapper: should be `FROM STREAM(LIVE.bronze_events)`
4. No expectations on silver table — should add `CONSTRAINT valid_event EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW`
5. Missing `OR REFRESH` on materialized view: should be `CREATE OR REFRESH MATERIALIZED VIEW`

---

### SDP-2: Build a CDC Pipeline ⭐⭐⭐⭐
**Type:** Build It

**Problem:** Build a complete SDP pipeline that: ingests CDC events from a Kafka-like source, applies APPLY CHANGES to maintain a live customer table, and creates a gold summary.

**Reference solution:**
```sql
CREATE OR REFRESH STREAMING TABLE bronze_cdc_events
AS SELECT * FROM STREAM read_files('s3://cdc/customers/', format => 'json');

CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_cdc_events)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_summary
AS SELECT
    region,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_ltv
FROM LIVE.silver_customers
WHERE __END_AT IS NULL  -- Current records only for SCD Type 2
GROUP BY region;
```

---

## Performance Tuning

### PERF-1: Diagnose the Slow Query ⭐⭐⭐⭐
**Type:** Optimize It

**Problem:** This query takes 45 minutes on a 10-node cluster. It should take 5 minutes. Identify all performance problems and fix them.

**Slow code:**
```python
# Problem 1: Reading entire table
events = spark.table("prod.analytics.events")  # 500M rows, 2TB

# Problem 2: Python UDF
from pyspark.sql.functions import udf
@udf("string")
def categorize(event_type):
    if event_type in ["click", "view", "scroll"]:
        return "engagement"
    elif event_type in ["purchase", "add_to_cart"]:
        return "conversion"
    else:
        return "other"

# Problem 3: No filter pushdown
result = (events
    .withColumn("category", categorize(col("event_type")))
    .join(spark.table("prod.analytics.users").collect(),  # Problem 4
          "user_id")
    .groupBy("category", "country")
    .agg(count("*").alias("event_count"))
)
result.collect()  # Problem 5
```

**All problems and fixes:**
1. No predicate pushdown — add `.filter(col("event_date") >= "2024-01-01")` early
2. Python UDF for simple categorization — replace with `when/otherwise`
3. (see #2)
4. `.collect()` on users table in join — use `broadcast()` instead
5. `.collect()` on final result — use `.show()` or write to table

**Optimized code:**
```python
from pyspark.sql.functions import when, col, count, broadcast

events = (spark.table("prod.analytics.events")
    .filter(col("event_date") >= "2024-01-01")  # Push filter early
)

users = spark.table("prod.analytics.users")  # Don't collect!

result = (events
    .withColumn("category",
        when(col("event_type").isin("click", "view", "scroll"), "engagement")
        .when(col("event_type").isin("purchase", "add_to_cart"), "conversion")
        .otherwise("other")
    )
    .join(broadcast(users), "user_id")  # Broadcast small table
    .groupBy("category", "country")
    .agg(count("*").alias("event_count"))
)
result.write.format("delta").saveAsTable("prod.analytics.event_summary")
```

---

## Unity Catalog & Governance

### UC-1: Secure Table Setup ⭐⭐⭐
**Type:** Build It

**Problem:** Create a table with PII data (customer_name, email, ssn, phone). Set up governance so: the analytics team can see all columns EXCEPT ssn, the finance team can see everything, and the marketing team can only see customer_name and email domain (not full email).

**Reference solution:**
```sql
-- Create the table
CREATE TABLE prod.customers.profiles (
    customer_id BIGINT,
    customer_name STRING,
    email STRING,
    ssn STRING,
    phone STRING,
    region STRING
);

-- Analytics view: mask SSN
CREATE VIEW prod.customers.profiles_analytics AS
SELECT customer_id, customer_name, email, '***-**-****' AS ssn, phone, region
FROM prod.customers.profiles;

GRANT SELECT ON VIEW prod.customers.profiles_analytics TO `analytics-team`;

-- Marketing view: only name + email domain
CREATE VIEW prod.customers.profiles_marketing AS
SELECT customer_id, customer_name, split(email, '@')[1] AS email_domain, region
FROM prod.customers.profiles;

GRANT SELECT ON VIEW prod.customers.profiles_marketing TO `marketing-team`;

-- Finance: full access
GRANT SELECT ON TABLE prod.customers.profiles TO `finance-team`;
```

---

## Certification-Style Questions

### CERT-1 (DE Associate)
**Q:** What is the default join type in PySpark when you call `df1.join(df2, "key")`?
- A) Left join
- B) Right join
- C) Inner join ✓
- D) Full outer join

### CERT-2 (DE Associate)
**Q:** Which command removes old data files from a Delta table that are no longer referenced by the transaction log?
- A) OPTIMIZE
- B) VACUUM ✓
- C) PURGE
- D) DELETE FILES

### CERT-3 (DE Associate)
**Q:** In Spark Declarative Pipelines, what does `ON VIOLATION DROP ROW` do?
- A) Deletes the row from the source table
- B) Logs the row and continues processing
- C) Silently drops the row from the target streaming table ✓
- D) Fails the entire pipeline

### CERT-4 (Spark Developer)
**Q:** What determines the number of tasks in a Spark stage?
- A) The number of executors
- B) The number of cores
- C) The number of partitions in the RDD/DataFrame ✓
- D) The spark.sql.shuffle.partitions setting

### CERT-5 (DE Associate)
**Q:** A three-level namespace in Unity Catalog follows which pattern?
- A) workspace.schema.table
- B) catalog.schema.table ✓
- C) account.catalog.table
- D) schema.catalog.table

### CERT-6 (DE Professional)
**Q:** You have a streaming pipeline reading from Kafka. The checkpoint directory is accidentally deleted. What happens on restart?
- A) The pipeline resumes from the last committed offset
- B) The pipeline reprocesses ALL data from the beginning ✓ (or earliest offset)
- C) The pipeline fails with an error and cannot restart
- D) The pipeline starts from the latest offset, skipping unprocessed data

### CERT-7 (DE Associate)
**Q:** Which of these is NOT a valid output mode for Structured Streaming?
- A) append
- B) complete
- C) update
- D) overwrite ✓

### CERT-8 (DE Professional)
**Q:** A MERGE operation on a Delta table is running slowly. The Spark UI shows one task taking 95% of the total time. What is the most likely cause?
- A) Insufficient cluster memory
- B) Data skew in the join key ✓
- C) Too many shuffle partitions
- D) Network bandwidth limitation

### CERT-9 (DE Associate)
**Q:** What is the purpose of `DESCRIBE HISTORY` on a Delta table?
- A) Shows the table schema
- B) Shows the transaction log with all operations performed ✓
- C) Shows the storage location of data files
- D) Shows the access control permissions

### CERT-10 (Spark Developer)
**Q:** Which transformation causes a shuffle in Spark?
- A) `filter()`
- B) `select()`
- C) `groupBy()` ✓
- D) `withColumn()`
