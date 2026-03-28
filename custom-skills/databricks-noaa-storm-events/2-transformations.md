# 3. Essential Transformations

## Parse Timestamps

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

## Parse Damage Values

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

## Clean Coordinates

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

## Data Quality Checks

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

## Loading Locations and Fatalities Files

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

# 4. Common Event Types

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

[Previous: 1-loading-data.md](1-loading-data.md) | [Next: 3-spatial-analysis.md](3-spatial-analysis.md) | [Back to SKILL.md](SKILL.md)
