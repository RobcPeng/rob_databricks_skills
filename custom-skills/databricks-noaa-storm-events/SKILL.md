---
name: databricks-noaa-storm-events
description: >
  Use when working with NOAA Storm Events data on Databricks — loading severe weather CSVs,
  parsing event types (tornado, hail, flood, wind), geocoding with lat/lon, damage estimation,
  and spatial analysis. Triggers on: 'NOAA', 'storm events', 'severe weather', 'tornado data',
  'hail data', 'flood data', 'weather history', 'storm damage', or when building weather
  analytics on Databricks.
---

# NOAA Storm Events on Databricks

Load, transform, and analyze NOAA Storm Events data — the authoritative US record of severe weather events including tornadoes, hail, floods, thunderstorm wind, winter storms, and more. Each record includes event type, location (lat/lon), damage estimates, injuries/deaths, and narrative descriptions.

## Data Source

| Field | Details |
|-------|---------|
| **Source** | NOAA National Centers for Environmental Information (NCEI) |
| **URL** | https://www.ncdc.noaa.gov/stormevents/ftp.jsp |
| **Bulk FTP** | https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/ |
| **Format** | Gzipped CSV files (`.csv.gz`), one per year per file type |
| **Coverage** | 1950–present (updated monthly) |
| **Size** | ~50-100 MB per year (details), ~2 GB total uncompressed |
| **License** | Public domain (US government data) |

### File Types

The bulk download has three file types per year:

| File Pattern | Contents | Key Fields |
|-------------|----------|------------|
| `StormEvents_details-ftp_v1.0_d{YYYY}_c*.csv.gz` | Event details | event_type, state, begin_lat/lon, damage, injuries, deaths, narrative |
| `StormEvents_locations-ftp_v1.0_d{YYYY}_c*.csv.gz` | Location details | lat/lon per event (multiple points for paths) |
| `StormEvents_fatalities-ftp_v1.0_d{YYYY}_c*.csv.gz` | Fatality records | age, sex, location of fatality per event |

**Start with `details`** — it has everything for most analyses. Use `locations` for path-based events (tornadoes) and `fatalities` for mortality studies.

---

## 1. Loading Data into Databricks

### Option A: Upload CSVs to a Volume (Recommended)

Download the bulk CSVs locally, then upload to a Unity Catalog volume:

```python
# Create the volume first
spark.sql("""
    CREATE VOLUME IF NOT EXISTS my_catalog.my_schema.noaa_storm_events
""")
```

Upload via MCP tool `upload_folder` or Databricks CLI:
```bash
# Upload a directory of downloaded .csv.gz files
databricks fs cp -r ./stormevents/ /Volumes/my_catalog/my_schema/noaa_storm_events/
```

Then read with Auto Loader or batch:
```python
# Batch read — all years at once
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_details-*.csv.gz")
)
```

### Option B: Direct Read from Staged Files

If you've uploaded individual year files:

```python
# Read a single year
df_2024 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_details-ftp_v1.0_d2024_*.csv.gz")
)
```

### Option C: Download in a Notebook

Download directly to a Volume from the NOAA FTP server:

```python
import subprocess

VOLUME_PATH = "/Volumes/my_catalog/my_schema/noaa_storm_events"
NOAA_BASE = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles"

# Download a specific year (filename includes a processing date suffix)
# Check the FTP listing for the exact filename first
year = 2024
filename = f"StormEvents_details-ftp_v1.0_d{year}_c20240617.csv.gz"  # adjust suffix

subprocess.run([
    "wget", "-q", "-P", VOLUME_PATH, f"{NOAA_BASE}/{filename}"
], check=True)

# Then read
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/{filename}")
)
```

> **Tip:** To find exact filenames, list the FTP directory:
> ```python
> import pandas as pd
> listing = pd.read_html(f"{NOAA_BASE}/")[0]
> details_files = listing[listing["Name"].str.contains("details", na=False)]
> ```

### Option D: Auto Loader for Incremental Updates

NOAA publishes new files monthly. Use Auto Loader to automatically pick up new files:

```python
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/my_catalog/my_schema/noaa_storm_events/_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_details-*.csv.gz")
)

# Write as a streaming table
(
    df_stream.writeStream
    .option("checkpointLocation", "/Volumes/my_catalog/my_schema/noaa_storm_events/_checkpoint")
    .trigger(availableNow=True)
    .toTable("my_catalog.my_schema.noaa_storm_events_raw")
)
```

### Performance: Schema Inference Warning

> **Warning:** `inferSchema=true` scans the entire CSV to determine types — slow on multi-year loads (2 GB+). For production pipelines, use an explicit schema:
> ```python
> from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
>
> storm_schema = (
>     StructType()
>     .add("BEGIN_YEARMONTH", StringType())
>     .add("BEGIN_DAY", StringType())
>     .add("BEGIN_TIME", StringType())
>     .add("END_YEARMONTH", StringType())
>     .add("END_DAY", StringType())
>     .add("END_TIME", StringType())
>     .add("EPISODE_ID", StringType())
>     .add("EVENT_ID", StringType())
>     .add("STATE", StringType())
>     .add("STATE_FIPS", StringType())
>     .add("YEAR", IntegerType())
>     .add("MONTH_NAME", StringType())
>     .add("EVENT_TYPE", StringType())
>     .add("CZ_TYPE", StringType())
>     .add("CZ_FIPS", StringType())
>     .add("CZ_NAME", StringType())
>     .add("WFO", StringType())
>     .add("BEGIN_DATE_TIME", StringType())
>     .add("CZ_TIMEZONE", StringType())
>     .add("END_DATE_TIME", StringType())
>     .add("INJURIES_DIRECT", IntegerType())
>     .add("INJURIES_INDIRECT", IntegerType())
>     .add("DEATHS_DIRECT", IntegerType())
>     .add("DEATHS_INDIRECT", IntegerType())
>     .add("DAMAGE_PROPERTY", StringType())
>     .add("DAMAGE_CROPS", StringType())
>     .add("SOURCE", StringType())
>     .add("MAGNITUDE", DoubleType())
>     .add("MAGNITUDE_TYPE", StringType())
>     .add("FLOOD_CAUSE", StringType())
>     .add("CATEGORY", StringType())
>     .add("TOR_F_SCALE", StringType())
>     .add("TOR_LENGTH", DoubleType())
>     .add("TOR_WIDTH", DoubleType())
>     .add("TOR_OTHER_WFO", StringType())
>     .add("TOR_OTHER_CZ_STATE", StringType())
>     .add("TOR_OTHER_CZ_FIPS", StringType())
>     .add("TOR_OTHER_CZ_NAME", StringType())
>     .add("BEGIN_RANGE", DoubleType())
>     .add("BEGIN_AZIMUTH", StringType())
>     .add("BEGIN_LOCATION", StringType())
>     .add("END_RANGE", DoubleType())
>     .add("END_AZIMUTH", StringType())
>     .add("END_LOCATION", StringType())
>     .add("BEGIN_LAT", DoubleType())
>     .add("BEGIN_LON", DoubleType())
>     .add("END_LAT", DoubleType())
>     .add("END_LON", DoubleType())
>     .add("EPISODE_NARRATIVE", StringType())
>     .add("EVENT_NARRATIVE", StringType())
>     .add("DATA_SOURCE", StringType())
> )
>
> df = spark.read.option("header", "true").schema(storm_schema).csv("/Volumes/.../StormEvents_details-*.csv.gz")
> ```

### Free Tier / Serverless Compatibility

All patterns in this skill work on serverless compute and the free tier. No cluster configuration needed. CSV reads, spatial functions (DBR 14.0+), H3 functions, and AI Functions all work on the Serverless Starter Warehouse. See `databricks-free-tier-guardrails` for additional checks.

---

## 2. Schema Reference — Details File

The details file has ~50 columns. These are the most useful:

| Column | Type | Description |
|--------|------|-------------|
| `EVENT_ID` | STRING | Unique event identifier |
| `EPISODE_ID` | STRING | Groups related events (e.g., one storm system) |
| `STATE` | STRING | State name (uppercase) |
| `STATE_FIPS` | STRING | State FIPS code |
| `CZ_NAME` | STRING | County/zone name |
| `CZ_FIPS` | STRING | County/zone FIPS code |
| `CZ_TYPE` | STRING | `C` = county, `Z` = zone, `M` = marine |
| `EVENT_TYPE` | STRING | Tornado, Hail, Thunderstorm Wind, Flash Flood, etc. |
| `BEGIN_DATE_TIME` | STRING | Event start (format: `DD-MON-YY HH:MM:SS`) |
| `END_DATE_TIME` | STRING | Event end |
| `BEGIN_LAT` | DOUBLE | Start latitude |
| `BEGIN_LON` | DOUBLE | Start longitude |
| `END_LAT` | DOUBLE | End latitude (for path events) |
| `END_LON` | DOUBLE | End longitude |
| `TOR_F_SCALE` | STRING | Fujita/EF scale for tornadoes (`EF0`–`EF5`) |
| `TOR_LENGTH` | DOUBLE | Tornado path length (miles) |
| `TOR_WIDTH` | DOUBLE | Tornado path width (yards) |
| `MAGNITUDE` | DOUBLE | Hail size (inches) or wind speed (knots) |
| `MAGNITUDE_TYPE` | STRING | `MG` = measured gust, `MS` = measured sustained, `EG` = estimated gust, `ES` = estimated sustained |
| `INJURIES_DIRECT` | INT | Direct injuries |
| `INJURIES_INDIRECT` | INT | Indirect injuries |
| `DEATHS_DIRECT` | INT | Direct deaths |
| `DEATHS_INDIRECT` | INT | Indirect deaths |
| `DAMAGE_PROPERTY` | STRING | Property damage (e.g., `25.00K`, `1.50M`, `0.00K`) |
| `DAMAGE_CROPS` | STRING | Crop damage (same format) |
| `SOURCE` | STRING | Data source (trained spotter, radar, law enforcement) |
| `EVENT_NARRATIVE` | STRING | Free-text description of the event |
| `EPISODE_NARRATIVE` | STRING | Free-text description of the episode |
| `YEAR` | INT | Event year |
| `MONTH_NAME` | STRING | Event month |

---

## 3. Essential Transformations

### Parse Timestamps

The date format is non-standard (`DD-MON-YY HH:MM:SS`):

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "begin_dt",
    F.to_timestamp("BEGIN_DATE_TIME", "dd-MMM-yy HH:mm:ss")
).withColumn(
    "end_dt",
    F.to_timestamp("END_DATE_TIME", "dd-MMM-yy HH:mm:ss")
)
```

> **Warning:** The `YY` year format is ambiguous — `50` could mean 1950 or 2050. Spark resolves two-digit years relative to a pivot. For data before 2000, verify parsed years are correct. If needed, apply a manual century fix:
> ```python
> df = df.withColumn(
>     "begin_dt",
>     F.when(F.year("begin_dt") > 2050, F.add_months("begin_dt", -1200))
>      .otherwise(F.col("begin_dt"))
> )
> ```

### Parse Damage Values

Damage columns use K/M/B suffixes:

```python
def parse_damage(col_name):
    """Convert '25.00K' → 25000.0, '1.50M' → 1500000.0, '0.00K' → 0.0"""
    col = F.upper(F.trim(F.col(col_name)))
    return (
        F.when(col.endswith("K"), F.regexp_extract(col, r"([\d.]+)", 1).cast("double") * 1e3)
        .when(col.endswith("M"), F.regexp_extract(col, r"([\d.]+)", 1).cast("double") * 1e6)
        .when(col.endswith("B"), F.regexp_extract(col, r"([\d.]+)", 1).cast("double") * 1e9)
        .otherwise(F.regexp_extract(col, r"([\d.]+)", 1).cast("double"))
    )

df = (
    df
    .withColumn("property_damage_usd", parse_damage("DAMAGE_PROPERTY"))
    .withColumn("crop_damage_usd", parse_damage("DAMAGE_CROPS"))
    .withColumn("total_damage_usd", F.col("property_damage_usd") + F.col("crop_damage_usd"))
)
```

### Clean Coordinates

Some records have null or zero lat/lon:

```python
df_geo = df.filter(
    (F.col("BEGIN_LAT").isNotNull()) &
    (F.col("BEGIN_LON").isNotNull()) &
    (F.col("BEGIN_LAT") != 0) &
    (F.col("BEGIN_LON") != 0)
)
```

> **Note:** `BEGIN_LON` is stored as **negative** for the western hemisphere (US data). Values like `-104.99` are correct for Colorado. Do not negate them.

### Data Quality Checks

```python
# Check for duplicate EVENT_IDs
dupes = df.groupBy("EVENT_ID").count().filter(F.col("count") > 1)
print(f"Duplicate EVENT_IDs: {dupes.count()}")

# Check coordinate coverage
total = df.count()
with_coords = df.filter(F.col("BEGIN_LAT").isNotNull() & (F.col("BEGIN_LAT") != 0)).count()
print(f"Records with coordinates: {with_coords}/{total} ({100*with_coords/total:.1f}%)")

# Check damage parsing coverage
with_damage = df.filter(F.col("DAMAGE_PROPERTY").isNotNull() & (F.col("DAMAGE_PROPERTY") != "")).count()
print(f"Records with damage data: {with_damage}/{total}")
```

> **Note:** Not all records have coordinates — especially older data (pre-2006) and zone-level events. Expect ~60-80% coordinate coverage depending on the year range.

### Loading Locations and Fatalities Files

```python
# Locations file — multiple lat/lon points per event (useful for tornado paths)
locations = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_locations-*.csv.gz")
)

# Join locations to details by EVENT_ID
events_with_paths = df.join(locations, on="EVENT_ID", how="left")

# Fatalities file — one row per fatality
fatalities = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/my_catalog/my_schema/noaa_storm_events/StormEvents_fatalities-*.csv.gz")
)
# Key columns: EVENT_ID, FATALITY_AGE, FATALITY_SEX, FATALITY_LOCATION, FATALITY_TYPE (D=direct, I=indirect)

# Fatality demographics per event type
fatality_demo = (
    fatalities.join(df.select("EVENT_ID", "EVENT_TYPE"), on="EVENT_ID")
    .groupBy("EVENT_TYPE")
    .agg(
        F.count("*").alias("total_fatalities"),
        F.avg("FATALITY_AGE").alias("avg_age"),
        F.sum(F.when(F.col("FATALITY_SEX") == "M", 1).otherwise(0)).alias("male"),
        F.sum(F.when(F.col("FATALITY_SEX") == "F", 1).otherwise(0)).alias("female")
    )
    .orderBy(F.desc("total_fatalities"))
)
```

---

## 4. Common Event Types

| EVENT_TYPE | What It Is | Key Columns |
|-----------|-----------|-------------|
| `Tornado` | Confirmed tornado | `TOR_F_SCALE`, `TOR_LENGTH`, `TOR_WIDTH` |
| `Hail` | Hail >= 1 inch | `MAGNITUDE` (inches) |
| `Thunderstorm Wind` | Wind >= 58 mph or damage | `MAGNITUDE` (knots) |
| `Flash Flood` | Rapid flooding | `DAMAGE_PROPERTY` |
| `Flood` | Non-flash flooding | `DAMAGE_PROPERTY` |
| `Winter Storm` | Heavy snow, ice, blizzard | |
| `Heavy Rain` | Excessive rainfall | |
| `High Wind` | Sustained wind >= 40 mph | `MAGNITUDE` (knots) |
| `Wildfire` | Wildland fire | `DAMAGE_PROPERTY` |
| `Drought` | Extended dry period | `DAMAGE_CROPS` |
| `Lightning` | Lightning strike | `INJURIES_DIRECT`, `DEATHS_DIRECT` |

Full list: ~48 event types. Filter with:
```python
df.select("EVENT_TYPE").distinct().orderBy("EVENT_TYPE").show(50, False)
```

---

## 5. Spatial Analysis Recipes

### Events Near a Point (e.g., Denver)

```python
DENVER_LAT, DENVER_LON = 39.7392, -104.9903
RADIUS_KM = 50

df_near_denver = df_geo.filter(
    F.expr(f"""
        ST_DISTANCE(
            ST_POINT(BEGIN_LON, BEGIN_LAT),
            ST_POINT({DENVER_LON}, {DENVER_LAT})
        ) < {RADIUS_KM * 1000}
    """)
)
```

### H3 Hexagonal Aggregation

```python
# Add H3 cell at resolution 5 (~250 km² hexagons)
df_h3 = df_geo.withColumn(
    "h3_res5",
    F.expr("H3_LONGLATASH3STRING(BEGIN_LON, BEGIN_LAT, 5)")
)

# Count events per hex
heatmap = (
    df_h3.groupBy("h3_res5")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("total_damage_usd").alias("total_damage"),
        F.countDistinct("EVENT_TYPE").alias("event_type_variety")
    )
    .withColumn("lat", F.expr("GET_JSON_OBJECT(H3_CENTERASGEOJSON(h3_res5), '$.coordinates[1]')").cast("double"))
    .withColumn("lon", F.expr("GET_JSON_OBJECT(H3_CENTERASGEOJSON(h3_res5), '$.coordinates[0]')").cast("double"))
)
```

### Tornado Path Lines

Tornadoes have begin AND end coordinates — you can create path geometries:

```python
tornado_paths = (
    df.filter(F.col("EVENT_TYPE") == "Tornado")
    .filter(F.col("END_LAT").isNotNull() & (F.col("END_LAT") != 0))
    .withColumn(
        "path_line",
        F.expr("ST_MAKELINE(ST_POINT(BEGIN_LON, BEGIN_LAT), ST_POINT(END_LON, END_LAT))")
    )
    .withColumn("path_length_km", F.col("TOR_LENGTH") * 1.60934)  # miles to km
)
```

### Join with Overture Maps Boundaries

Enrich storm events with Overture Maps division boundaries (counties, states):

```python
# Load Overture divisions (see databricks-geospatial skill)
OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2026-02-18.0"
counties = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division_area")

# Spatial join — broadcast the small side
events_with_county = df_geo.join(
    F.broadcast(counties.filter(F.col("subtype") == "county")),
    F.expr("ST_CONTAINS(ST_GEOMFROMWKB(geometry), ST_POINT(BEGIN_LON, BEGIN_LAT))")
)
```

> **Performance:** Always `F.broadcast()` the boundary table. See `databricks-pipeline-guardrails` for non-equi join best practices.

---

## 6. Analysis Patterns

### Yearly Trend by Event Type

```python
trend = (
    df.groupBy("YEAR", "EVENT_TYPE")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("total_damage_usd").alias("total_damage"),
        F.sum("DEATHS_DIRECT").alias("deaths")
    )
    .orderBy("YEAR", "EVENT_TYPE")
)
```

### Top Damage Events

```python
top_damage = (
    df.filter(F.col("total_damage_usd") > 0)
    .orderBy(F.desc("total_damage_usd"))
    .select(
        "EVENT_ID", "YEAR", "STATE", "EVENT_TYPE",
        "total_damage_usd", "DEATHS_DIRECT", "EVENT_NARRATIVE"
    )
    .limit(100)
)
```

### Tornado Intensity Distribution

```python
ef_distribution = (
    df.filter(F.col("EVENT_TYPE") == "Tornado")
    .groupBy("TOR_F_SCALE")
    .agg(
        F.count("*").alias("count"),
        F.avg("TOR_LENGTH").alias("avg_path_miles"),
        F.avg("TOR_WIDTH").alias("avg_width_yards"),
        F.sum("DEATHS_DIRECT").alias("total_deaths")
    )
    .orderBy("TOR_F_SCALE")
)
```

### Seasonal Patterns

```python
monthly = (
    df.groupBy("MONTH_NAME", "EVENT_TYPE")
    .agg(F.count("*").alias("events"))
    .orderBy(
        F.when(F.col("MONTH_NAME") == "January", 1)
        .when(F.col("MONTH_NAME") == "February", 2)
        .when(F.col("MONTH_NAME") == "March", 3)
        .when(F.col("MONTH_NAME") == "April", 4)
        .when(F.col("MONTH_NAME") == "May", 5)
        .when(F.col("MONTH_NAME") == "June", 6)
        .when(F.col("MONTH_NAME") == "July", 7)
        .when(F.col("MONTH_NAME") == "August", 8)
        .when(F.col("MONTH_NAME") == "September", 9)
        .when(F.col("MONTH_NAME") == "October", 10)
        .when(F.col("MONTH_NAME") == "November", 11)
        .when(F.col("MONTH_NAME") == "December", 12)
    )
)
```

### Year-over-Year Comparison (Window Functions)

```python
from pyspark.sql.window import Window

# YoY change in event count by state
yearly_state = (
    df.groupBy("YEAR", "STATE")
    .agg(F.count("*").alias("event_count"))
)

w = Window.partitionBy("STATE").orderBy("YEAR")
yoy = (
    yearly_state
    .withColumn("prev_year_count", F.lag("event_count").over(w))
    .withColumn("yoy_change_pct",
        F.round((F.col("event_count") - F.col("prev_year_count")) / F.col("prev_year_count") * 100, 1)
    )
    .filter(F.col("prev_year_count").isNotNull())
    .orderBy("STATE", "YEAR")
)
```

### Rolling Averages for Trend Smoothing

```python
# 5-year rolling average of tornado count
tornado_yearly = (
    df.filter(F.col("EVENT_TYPE") == "Tornado")
    .groupBy("YEAR")
    .agg(F.count("*").alias("tornado_count"))
)

w5 = Window.orderBy("YEAR").rowsBetween(-4, 0)
tornado_trend = tornado_yearly.withColumn(
    "rolling_5yr_avg", F.round(F.avg("tornado_count").over(w5), 1)
)
```

### AI Functions on Narratives

Use Databricks AI Functions to classify or extract from the free-text narratives:

```sql
-- Classify severity from narrative text
SELECT
    EVENT_ID,
    EVENT_TYPE,
    EVENT_NARRATIVE,
    ai_classify(EVENT_NARRATIVE, ARRAY('minor', 'moderate', 'severe', 'catastrophic')) AS severity
FROM my_catalog.my_schema.noaa_storm_events
WHERE EVENT_NARRATIVE IS NOT NULL
LIMIT 100;

-- Extract specific impacts
SELECT
    EVENT_ID,
    ai_extract(EVENT_NARRATIVE, ARRAY('structures_damaged', 'roads_closed', 'power_outages')) AS impacts
FROM my_catalog.my_schema.noaa_storm_events
WHERE EVENT_TYPE = 'Tornado'
LIMIT 50;
```

### ai_forecast — Predict Future Event Counts

```sql
-- Forecast monthly tornado count for next 12 months
WITH monthly_tornadoes AS (
    SELECT
        DATE_TRUNC('month', begin_dt) AS month_start,
        CAST(COUNT(*) AS DOUBLE) AS tornado_count
    FROM my_catalog.my_schema.noaa_storm_events
    WHERE EVENT_TYPE = 'Tornado' AND begin_dt IS NOT NULL
    GROUP BY 1
)
SELECT * FROM ai_forecast(
    TABLE(monthly_tornadoes),
    horizon => 12,
    time_col => 'month_start',
    value_col => 'tornado_count'
);
```

> **Note:** `ai_forecast` requires the time column to be `TIMESTAMP` type and `horizon` is an integer (number of periods), not a date. Works on the free tier Serverless Starter Warehouse.

---

## 7. Saving as a Delta Table

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

### Partitioning Guidance

| Query Pattern | Recommended Partitioning | Why |
|--------------|-------------------------|-----|
| Filter by year range | `PARTITIONED BY (YEAR)` | Most queries filter by time period |
| Filter by state + year | `PARTITIONED BY (YEAR)` + ZORDER by `STATE` | Partition by year, ZORDER handles state |
| Full-table scans (ML/analytics) | No partitioning, ZORDER only | Avoid partition overhead on small data |

> **This dataset is ~2 GB total** — partitioning is optional. ZORDER alone is usually sufficient. Only partition if you're loading 50+ years and frequently filtering by year.

---

## 8. SDP Pipeline Pattern (Bronze/Silver/Gold)

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

## 9. AI/BI Dashboard Queries

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

## Common Mistakes

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
