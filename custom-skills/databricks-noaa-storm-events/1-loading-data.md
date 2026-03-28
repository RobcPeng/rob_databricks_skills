# 1. Loading Data into Databricks

## Option A: Upload CSVs to a Volume (Recommended)

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

## Option B: Direct Read from Staged Files

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

## Option C: Download in a Notebook

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

## Option D: Auto Loader for Incremental Updates

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

## Performance: Schema Inference Warning

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

## Free Tier / Serverless Compatibility

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

[Next: 2-transformations.md](2-transformations.md) | [Back to SKILL.md](SKILL.md)
