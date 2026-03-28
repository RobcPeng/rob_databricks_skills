# 7. Saving as a Delta Table

```python
# Write the cleaned, transformed data as a managed Delta table
(
    df
    .withColumn("begin_dt", F.to_timestamp("BEGIN_DATE_TIME", "dd-MMM-yy HH:mm:ss"))
    .withColumn("end_dt", F.to_timestamp("END_DATE_TIME", "dd-MMM-yy HH:mm:ss"))
    .withColumn("property_damage_usd", parse_damage("DAMAGE_PROPERTY"))
    .withColumn("crop_damage_usd", parse_damage("DAMAGE_CROPS"))
    .withColumn("total_damage_usd", F.col("property_damage_usd") + F.col("crop_damage_usd"))
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("my_catalog.my_schema.noaa_storm_events")
)

# Optimize for common queries
spark.sql("OPTIMIZE my_catalog.my_schema.noaa_storm_events ZORDER BY (STATE, EVENT_TYPE, YEAR)")
```

## Partitioning Guidance

| Query Pattern | Recommended Partitioning | Why |
|--------------|-------------------------|-----|
| Filter by year range | `PARTITIONED BY (YEAR)` | Most queries filter by time period |
| Filter by state + year | `PARTITIONED BY (YEAR)` + ZORDER by `STATE` | Partition by year, ZORDER handles state |
| Full-table scans (ML/analytics) | No partitioning, ZORDER only | Avoid partition overhead on small data |

> **This dataset is ~2 GB total** — partitioning is optional. ZORDER alone is usually sufficient. Only partition if you're loading 50+ years and frequently filtering by year.

---

# 8. SDP Pipeline Pattern (Bronze/Silver/Gold)

For a production pipeline using Spark Declarative Pipelines:

```python
from pyspark import pipelines as dp

# Bronze — raw CSV ingestion
@dp.table(
    name="my_catalog.01_bronze.noaa_storm_events_raw",
    comment="Raw NOAA Storm Events from CSV files"
)
def bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/my_catalog/my_schema/noaa_storm_events/_schema")
        .load("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_details-*.csv.gz")
    )

# Silver — parsed, cleaned, typed
@dp.materialized_view(
    name="my_catalog.02_silver.noaa_storm_events",
    comment="Cleaned storm events with parsed damage and timestamps"
)
def silver():
    raw = spark.read.table("my_catalog.01_bronze.noaa_storm_events_raw")
    return (
        raw
        .withColumn("begin_dt", F.to_timestamp("BEGIN_DATE_TIME", "dd-MMM-yy HH:mm:ss"))
        .withColumn("end_dt", F.to_timestamp("END_DATE_TIME", "dd-MMM-yy HH:mm:ss"))
        .withColumn("property_damage_usd", parse_damage("DAMAGE_PROPERTY"))
        .withColumn("crop_damage_usd", parse_damage("DAMAGE_CROPS"))
        .withColumn("total_damage_usd", F.col("property_damage_usd") + F.col("crop_damage_usd"))
        .filter(F.col("EVENT_ID").isNotNull())
        .dropDuplicates(["EVENT_ID"])
    )

# Gold — aggregated for analytics/dashboard
@dp.materialized_view(
    name="my_catalog.03_gold.storm_events_state_yearly",
    comment="Yearly storm event summary by state and event type"
)
def gold():
    silver = spark.read.table("my_catalog.02_silver.noaa_storm_events")
    return (
        silver.groupBy("YEAR", "STATE", "EVENT_TYPE")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("total_damage_usd").alias("total_damage_usd"),
            F.sum("DEATHS_DIRECT").alias("deaths"),
            F.sum("INJURIES_DIRECT").alias("injuries"),
            F.avg("MAGNITUDE").alias("avg_magnitude")
        )
    )
```

> **Reminder:** Use fully qualified three-part table names for silver and gold. See `databricks-pipeline-guardrails`.

---

# 9. AI/BI Dashboard Queries

Pre-tested SQL queries for building a Lakeview dashboard (see `databricks-aibi-dashboards` skill):

```sql
-- KPI: Total events, deaths, and damage (current year)
SELECT
    COUNT(*) AS total_events,
    SUM(deaths_direct) AS total_deaths,
    SUM(total_damage_usd) / 1e9 AS total_damage_billions
FROM my_catalog.02_silver.noaa_storm_events
WHERE YEAR = YEAR(CURRENT_DATE());

-- Bar chart: Top 10 states by event count
SELECT STATE, COUNT(*) AS event_count
FROM my_catalog.02_silver.noaa_storm_events
WHERE YEAR >= YEAR(CURRENT_DATE()) - 5
GROUP BY STATE
ORDER BY event_count DESC
LIMIT 10;

-- Line chart: Monthly event trend
SELECT
    DATE_TRUNC('month', begin_dt) AS month,
    EVENT_TYPE,
    COUNT(*) AS events
FROM my_catalog.02_silver.noaa_storm_events
WHERE YEAR >= YEAR(CURRENT_DATE()) - 3
GROUP BY 1, 2
ORDER BY 1;

-- Map: Events by lat/lon (sample for performance)
SELECT
    BEGIN_LAT AS lat,
    BEGIN_LON AS lon,
    EVENT_TYPE,
    total_damage_usd
FROM my_catalog.02_silver.noaa_storm_events
WHERE BEGIN_LAT IS NOT NULL
  AND YEAR >= YEAR(CURRENT_DATE()) - 1
  AND total_damage_usd > 10000;

-- Heatmap: Events by state and month
SELECT
    STATE,
    MONTH_NAME,
    COUNT(*) AS events
FROM my_catalog.02_silver.noaa_storm_events
WHERE YEAR >= YEAR(CURRENT_DATE()) - 5
GROUP BY 1, 2;
```

---

# Common Mistakes

| Mistake | Fix |
|---------|-----|
| Negating `BEGIN_LON` (it's already negative for US) | Leave as-is — western hemisphere longitudes are negative |
| Ignoring null lat/lon (many records lack coordinates) | Filter `BEGIN_LAT IS NOT NULL AND BEGIN_LAT != 0` |
| Treating `DAMAGE_PROPERTY` as numeric | Parse the K/M/B suffix first (see section 3) |
| Using `MAGNITUDE` without checking `MAGNITUDE_TYPE` | Hail = inches, wind = knots; check type before comparing |
| Assuming one lat/lon per tornado | Use `locations` file for full tornado paths |
| Two-digit year parsing errors | Verify century for pre-2000 records after `to_timestamp` |
| Not broadcasting in spatial joins | Always `F.broadcast()` the boundary/reference table |
| Using `inferSchema` on multi-year loads | Slow — use explicit schema (section 1) for production |
| Not deduplicating by `EVENT_ID` | Some years have overlapping files; `dropDuplicates(["EVENT_ID"])` |
| Forgetting `YEAR` column exists in the CSV | No need to extract year from timestamp — it's already a column |
| Over-partitioning a 2 GB dataset | ZORDER is usually sufficient; partition only if filtering by year heavily |
| Using `ST_GEOGFROMWKB` with `ST_CONTAINS` | Use `ST_GEOMFROMWKB` — see `databricks-geospatial` skill for GEOGRAPHY vs GEOMETRY |

---

[Previous: 3-spatial-analysis.md](3-spatial-analysis.md) | [Back to SKILL.md](SKILL.md)
