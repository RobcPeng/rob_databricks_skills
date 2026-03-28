# 5. Spatial Analysis Recipes

## Events Near a Point (e.g., Denver)

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

## H3 Hexagonal Aggregation

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

## Tornado Path Lines

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

## Join with Overture Maps Boundaries

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

# 6. Analysis Patterns

## Yearly Trend by Event Type

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

## Top Damage Events

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

## Tornado Intensity Distribution

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

## Seasonal Patterns

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

## Year-over-Year Comparison (Window Functions)

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

## Rolling Averages for Trend Smoothing

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

## AI Functions on Narratives

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

## ai_forecast — Predict Future Event Counts

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

[Previous: 2-transformations.md](2-transformations.md) | [Next: 4-pipeline-and-dashboards.md](4-pipeline-and-dashboards.md) | [Back to SKILL.md](SKILL.md)
