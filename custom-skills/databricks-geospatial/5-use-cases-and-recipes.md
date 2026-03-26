# Geospatial Use Cases and Recipes

Production-ready patterns for common geospatial analysis tasks on Databricks, with Overture Maps examples.

## Table of Contents

1. [Store/Site Location Analysis](#1-storesite-location-analysis)
2. [Catchment Area and Trade Area Analysis](#2-catchment-area-and-trade-area-analysis)
3. [Point-in-Polygon Assignment](#3-point-in-polygon-assignment)
4. [Nearest Neighbor Search](#4-nearest-neighbor-search)
5. [Route and Network Analysis](#5-route-and-network-analysis)
6. [Geocoding and Reverse Geocoding](#6-geocoding-and-reverse-geocoding)
7. [Heatmaps and Density Analysis](#7-heatmaps-and-density-analysis)
8. [Building Footprint Analysis](#8-building-footprint-analysis)
9. [Address Enrichment](#9-address-enrichment)
10. [Multi-Theme Spatial Joins](#10-multi-theme-spatial-joins)

---

## 1. Store/Site Location Analysis

**Goal:** Evaluate potential store locations using surrounding POI density, competition, and demographics.

```python
import pyspark.sql.functions as F

OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2025-01-22.0"

places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")

# Define candidate locations
candidates = spark.createDataFrame([
    ("Site A", -122.4194, 37.7749),
    ("Site B", -122.3894, 37.7849),
    ("Site C", -122.4094, 37.7649),
], ["site_name", "lon", "lat"])

# Count nearby POIs by category within 1km of each candidate
poi_analysis = (
    candidates.alias("c")
    .crossJoin(
        places
        .filter("bbox.xmin > -122.6 AND bbox.xmax < -122.2")
        .filter("bbox.ymin > 37.6 AND bbox.ymax < 37.9")
        .filter("confidence > 0.7")
        .alias("p")
    )
    .filter("""
        ST_DWITHIN(
            ST_POINT(c.lon, c.lat)::GEOGRAPHY,
            ST_GEOGFROMWKB(p.geometry),
            1000
        )
    """)
    .groupBy("c.site_name", "p.categories.primary")
    .agg(F.count("*").alias("poi_count"))
    .groupBy("site_name")
    .pivot("primary")
    .sum("poi_count")
)
poi_analysis.show()
```

```sql
-- Competition analysis: count competing stores near each candidate
WITH candidates AS (
  SELECT 'Site A' AS name, -122.4194 AS lon, 37.7749 AS lat
  UNION ALL
  SELECT 'Site B', -122.3894, 37.7849
),
nearby_competitors AS (
  SELECT c.name AS site,
         p.names.primary AS competitor_name,
         p.categories.primary AS category,
         ST_DISTANCESPHERE(ST_POINT(c.lon, c.lat), ST_GEOGFROMWKB(p.geometry)) AS dist_m
  FROM candidates c
  JOIN overture_places p
    ON ST_DWITHIN(
      ST_POINT(c.lon, c.lat)::GEOGRAPHY,
      ST_GEOGFROMWKB(p.geometry),
      2000
    )
  WHERE p.categories.primary = 'coffee_shop'
    AND p.bbox.xmin > -122.5 AND p.bbox.xmax < -122.2
    AND p.bbox.ymin > 37.7   AND p.bbox.ymax < 37.85
)
SELECT site, COUNT(*) AS competitor_count, MIN(dist_m) AS nearest_competitor_m
FROM nearby_competitors
GROUP BY site;
```

---

## 2. Catchment Area and Trade Area Analysis

**Goal:** Define the service area around a location and analyze what's inside it.

### Buffer-Based Catchment

```sql
-- Create 500m, 1km, 2km rings around a store
WITH store AS (
  SELECT ST_POINT(-122.4194, 37.7749)::GEOGRAPHY AS location
),
rings AS (
  SELECT '0-500m' AS ring, ST_BUFFER(location, 500) AS area FROM store
  UNION ALL
  SELECT '500m-1km', ST_DIFFERENCE(ST_BUFFER(location, 1000), ST_BUFFER(location, 500)) FROM store
  UNION ALL
  SELECT '1km-2km', ST_DIFFERENCE(ST_BUFFER(location, 2000), ST_BUFFER(location, 1000)) FROM store
)
SELECT r.ring,
       COUNT(p.id) AS place_count
FROM rings r
JOIN overture_places p
  ON ST_CONTAINS(r.area, ST_GEOGFROMWKB(p.geometry))
WHERE p.bbox.xmin > -122.5 AND p.bbox.xmax < -122.3
  AND p.bbox.ymin > 37.7   AND p.bbox.ymax < 37.85
GROUP BY r.ring
ORDER BY r.ring;
```

### H3-Based Catchment (Faster)

```sql
-- Use H3 k-ring for fast catchment analysis
WITH store_h3 AS (
  SELECT H3_LONGLATASH3STRING(-122.4194, 37.7749, 8) AS center_cell
),
catchment_cells AS (
  SELECT EXPLODE(H3_KRING(center_cell, 3)) AS h3_cell
  FROM store_h3
),
events_in_catchment AS (
  SELECT e.*
  FROM events e
  JOIN catchment_cells c
    ON H3_LONGLATASH3STRING(e.lon, e.lat, 8) = c.h3_cell
)
SELECT COUNT(*) AS event_count,
       SUM(amount) AS total_revenue
FROM events_in_catchment;
```

---

## 3. Point-in-Polygon Assignment

**Goal:** Assign points to regions (stores to territories, events to neighborhoods, etc.).

```python
# Assign Overture places to Overture division areas (neighborhoods)
places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")
neighborhoods = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division_area")

# Filter to area of interest
sf_places = places.filter(
    "bbox.xmin > -122.52 AND bbox.xmax < -122.35 AND bbox.ymin > 37.70 AND bbox.ymax < 37.82"
)
sf_hoods = neighborhoods.filter(
    "subtype = 'neighborhood' AND bbox.xmin > -122.52 AND bbox.xmax < -122.35 AND bbox.ymin > 37.70 AND bbox.ymax < 37.82"
)

# Spatial join
assigned = sf_places.alias("p").join(
    sf_hoods.alias("n"),
    F.expr("ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), ST_GEOGFROMWKB(p.geometry))")
).selectExpr(
    "p.names.primary AS place_name",
    "p.categories.primary AS category",
    "n.names.primary AS neighborhood"
)

assigned.show(20, truncate=False)
```

### Optimized with H3 Pre-Filter

```python
# Add H3 to both sides, then join on H3 first (reduces comparisons)
sf_places_h3 = sf_places.selectExpr(
    "*",
    "H3_POINTASH3STRING(ST_GEOGFROMWKB(geometry), 7) AS h3_7"
)

sf_hoods_h3 = sf_hoods.selectExpr(
    "*",
    "EXPLODE(H3_COVERASH3STRING(ST_GEOGFROMWKB(geometry), 7)) AS h3_7"
)

# H3 pre-filter + spatial refinement
assigned_fast = (
    sf_places_h3.alias("p")
    .join(sf_hoods_h3.alias("n"), "h3_7")
    .filter("ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), ST_GEOGFROMWKB(p.geometry))")
    .selectExpr(
        "p.names.primary AS place_name",
        "n.names.primary AS neighborhood"
    )
)
```

---

## 4. Nearest Neighbor Search

**Goal:** Find the closest feature of type B for each feature of type A.

### Brute Force (Small Datasets)

```sql
-- Nearest hospital to each school
SELECT school_name, hospital_name, distance_m
FROM (
  SELECT s.name AS school_name,
         h.name AS hospital_name,
         ST_DISTANCESPHERE(
           ST_GEOGFROMWKB(s.geometry),
           ST_GEOGFROMWKB(h.geometry)
         ) AS distance_m,
         ROW_NUMBER() OVER (
           PARTITION BY s.id
           ORDER BY ST_DISTANCESPHERE(ST_GEOGFROMWKB(s.geometry), ST_GEOGFROMWKB(h.geometry))
         ) AS rn
  FROM schools s
  CROSS JOIN hospitals h
)
WHERE rn = 1;
```

### H3-Accelerated (Large Datasets)

```python
# Step 1: Index both datasets at same resolution
schools_h3 = schools.selectExpr(
    "*",
    "H3_POINTASH3STRING(ST_GEOGFROMWKB(geometry), 8) AS h3_8"
)

hospitals_h3 = hospitals.selectExpr("*")

# Step 2: Expand hospital search to k-ring
from pyspark.sql.functions import explode, col

hospitals_expanded = hospitals_h3.selectExpr(
    "id AS hospital_id",
    "name AS hospital_name",
    "geometry AS hospital_geom",
    "EXPLODE(H3_KRING(H3_POINTASH3STRING(ST_GEOGFROMWKB(geometry), 8), 3)) AS h3_8"
)

# Step 3: Join on H3, then rank by actual distance
nearest = (
    schools_h3.alias("s")
    .join(hospitals_expanded.alias("h"), "h3_8")
    .selectExpr(
        "s.id AS school_id",
        "s.name AS school_name",
        "h.hospital_id",
        "h.hospital_name",
        "ST_DISTANCESPHERE(ST_GEOGFROMWKB(s.geometry), ST_GEOGFROMWKB(h.hospital_geom)) AS distance_m"
    )
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("school_id").orderBy("distance_m")
    ))
    .filter("rn = 1")
    .drop("rn")
)
```

---

## 5. Route and Network Analysis

**Goal:** Analyze road networks using Overture Transportation data.

```python
# Road network statistics for a city
segments = spark.read.parquet(f"{OVERTURE_BASE}/theme=transportation/type=segment")

sf_roads = (
    segments
    .filter("subtype = 'road'")
    .filter("bbox.xmin > -122.52 AND bbox.xmax < -122.35")
    .filter("bbox.ymin > 37.70 AND bbox.ymax < 37.82")
    .selectExpr(
        "id",
        "class",
        "subclass",
        "ST_LENGTH(ST_GEOGFROMWKB(geometry)) AS length_m",
        "SIZE(connectors) AS junction_count"
    )
)

# Road network summary
sf_roads.groupBy("class").agg(
    F.count("*").alias("segment_count"),
    F.sum("length_m").alias("total_length_m"),
    F.avg("length_m").alias("avg_segment_length_m")
).orderBy(F.desc("total_length_m")).show()
```

```sql
-- Walkability score: density of pedestrian infrastructure
SELECT H3_LONGLATASH3STRING(
         ST_X(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         ST_Y(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         8
       ) AS h3_cell,
       SUM(CASE WHEN class IN ('footway', 'pedestrian', 'cycleway') THEN ST_LENGTH(ST_GEOGFROMWKB(geometry)) ELSE 0 END) AS ped_length_m,
       SUM(ST_LENGTH(ST_GEOGFROMWKB(geometry))) AS total_length_m,
       ROUND(
         SUM(CASE WHEN class IN ('footway', 'pedestrian', 'cycleway') THEN ST_LENGTH(ST_GEOGFROMWKB(geometry)) ELSE 0 END) /
         NULLIF(SUM(ST_LENGTH(ST_GEOGFROMWKB(geometry))), 0) * 100,
         1
       ) AS walkability_pct
FROM overture_segments
WHERE subtype = 'road'
  AND bbox.xmin > -122.52 AND bbox.xmax < -122.35
  AND bbox.ymin > 37.70   AND bbox.ymax < 37.82
GROUP BY h3_cell
ORDER BY walkability_pct DESC;
```

---

## 6. Geocoding and Reverse Geocoding

### Forward Geocoding (Address to Coordinates)

```sql
-- Match addresses against Overture address data
SELECT a.number, a.street, a.postcode,
       ST_Y(ST_GEOGFROMWKB(a.geometry)) AS lat,
       ST_X(ST_GEOGFROMWKB(a.geometry)) AS lon
FROM overture_addresses a
WHERE a.country = 'US'
  AND a.street LIKE '%Market%'
  AND a.number = '100'
  AND a.postcode = '94105';
```

### Reverse Geocoding (Coordinates to Address)

```sql
-- Find the nearest address to a coordinate
SELECT number, street, postcode,
       ST_DISTANCESPHERE(
         ST_GEOGFROMWKB(geometry),
         ST_POINT(-122.4194, 37.7749)
       ) AS distance_m
FROM overture_addresses
WHERE country = 'US'
  AND bbox.xmin > -122.43 AND bbox.xmax < -122.41
  AND bbox.ymin > 37.77   AND bbox.ymax < 37.78
ORDER BY distance_m
LIMIT 1;
```

### Reverse Geocoding with Divisions (Coordinates to Admin Area)

```sql
-- What neighborhood/city/state contains this point?
SELECT subtype, names.primary AS area_name
FROM overture_division_areas
WHERE ST_CONTAINS(
  ST_GEOGFROMWKB(geometry),
  ST_POINT(-122.4194, 37.7749)
)
ORDER BY
  CASE subtype
    WHEN 'neighborhood' THEN 1
    WHEN 'locality' THEN 2
    WHEN 'county' THEN 3
    WHEN 'region' THEN 4
    WHEN 'country' THEN 5
    ELSE 6
  END;
```

---

## 7. Heatmaps and Density Analysis

### H3 Heatmap

```sql
-- Event density heatmap with GeoJSON output for visualization
SELECT h3_cell,
       event_count,
       H3_BOUNDARYASGEOJSON(h3_cell) AS geojson,
       H3_CENTERASWKT(h3_cell) AS center
FROM (
  SELECT H3_LONGLATASH3STRING(lon, lat, 8) AS h3_cell,
         COUNT(*) AS event_count
  FROM events
  GROUP BY h3_cell
)
WHERE event_count > 10
ORDER BY event_count DESC;
```

### Kernel Density Estimation (KDE Approximation)

```sql
-- Weighted density using H3 k-ring smoothing
WITH raw_counts AS (
  SELECT H3_LONGLATASH3STRING(lon, lat, 9) AS h3_cell,
         COUNT(*) AS cnt
  FROM events
  GROUP BY h3_cell
),
smoothed AS (
  SELECT r.h3_cell,
         r.cnt AS raw_count,
         (
           SELECT AVG(r2.cnt)
           FROM raw_counts r2
           WHERE r2.h3_cell IN (
             SELECT EXPLODE(H3_KRING(r.h3_cell, 1))
           )
         ) AS smoothed_count
  FROM raw_counts r
)
SELECT h3_cell, raw_count, ROUND(smoothed_count, 1) AS smoothed,
       H3_BOUNDARYASGEOJSON(h3_cell) AS geojson
FROM smoothed
ORDER BY smoothed_count DESC
LIMIT 200;
```

---

## 8. Building Footprint Analysis

### Urban Density Metrics

```python
# Calculate building coverage ratio per neighborhood
buildings = spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")
neighborhoods = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division_area")

sf_buildings = buildings.filter(
    "bbox.xmin > -122.52 AND bbox.xmax < -122.35 AND bbox.ymin > 37.70 AND bbox.ymax < 37.82"
)
sf_hoods = neighborhoods.filter(
    "subtype = 'neighborhood' AND bbox.xmin > -122.52 AND bbox.xmax < -122.35 AND bbox.ymin > 37.70 AND bbox.ymax < 37.82"
)

density = (
    sf_buildings.alias("b")
    .join(
        sf_hoods.alias("n"),
        F.expr("ST_CONTAINS(ST_GEOGFROMWKB(n.geometry), ST_CENTROID(ST_GEOGFROMWKB(b.geometry)))")
    )
    .groupBy("n.names.primary")
    .agg(
        F.count("*").alias("building_count"),
        F.sum(F.expr("ST_AREA(ST_GEOGFROMWKB(b.geometry))")).alias("total_footprint_sqm"),
        F.avg("b.height").alias("avg_height"),
        F.max("b.height").alias("max_height")
    )
    .withColumn("neighborhood", F.col("primary"))
    .drop("primary")
    .orderBy(F.desc("building_count"))
)
density.show()
```

### 3D Building Volume Estimation

```sql
-- Estimate total building volume per H3 cell
SELECT H3_LONGLATASH3STRING(
         ST_X(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         ST_Y(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         8
       ) AS h3_cell,
       COUNT(*) AS building_count,
       SUM(ST_AREA(ST_GEOGFROMWKB(geometry)) * COALESCE(height, 10)) AS estimated_volume_m3,
       AVG(num_floors) AS avg_floors
FROM overture_buildings
WHERE height IS NOT NULL
  AND bbox.xmin > -74.02 AND bbox.xmax < -73.93
  AND bbox.ymin > 40.70 AND bbox.ymax < 40.80
GROUP BY h3_cell
ORDER BY estimated_volume_m3 DESC
LIMIT 50;
```

---

## 9. Address Enrichment

**Goal:** Enrich business or customer data with geographic context from Overture Maps.

```python
# Enrich customer addresses with Overture place data
customers = spark.table("my_catalog.sales.customers")  # has lat, lon columns
places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")

# Find nearby amenities for each customer
enriched = (
    customers.alias("c")
    .crossJoin(
        places
        .filter("categories.primary IN ('grocery_store', 'pharmacy', 'hospital')")
        .filter("confidence > 0.7")
        .alias("p")
    )
    .filter("""
        ST_DWITHIN(
            ST_POINT(c.lon, c.lat)::GEOGRAPHY,
            ST_GEOGFROMWKB(p.geometry),
            2000
        )
        AND p.bbox.xmin > c.lon - 0.03
        AND p.bbox.xmax < c.lon + 0.03
        AND p.bbox.ymin > c.lat - 0.03
        AND p.bbox.ymax < c.lat + 0.03
    """)
    .groupBy("c.customer_id")
    .agg(
        F.sum(F.when(F.col("p.categories.primary") == "grocery_store", 1).otherwise(0)).alias("nearby_grocery"),
        F.sum(F.when(F.col("p.categories.primary") == "pharmacy", 1).otherwise(0)).alias("nearby_pharmacy"),
        F.sum(F.when(F.col("p.categories.primary") == "hospital", 1).otherwise(0)).alias("nearby_hospital"),
    )
)

# Join back to customers
customers_enriched = customers.join(enriched, "customer_id", "left")
```

---

## 10. Multi-Theme Spatial Joins

**Goal:** Combine multiple Overture themes for rich geographic analysis.

```python
# Combine places + buildings + transportation for a complete picture
places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")
buildings = spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")
segments = spark.read.parquet(f"{OVERTURE_BASE}/theme=transportation/type=segment")

# Bounding box: downtown San Francisco
bbox_filter = """
    bbox.xmin > -122.42 AND bbox.xmax < -122.39
    AND bbox.ymin > 37.78 AND bbox.ymax < 37.80
"""

sf_places = places.filter(bbox_filter).selectExpr(
    "id AS place_id", "names.primary AS place_name",
    "categories.primary AS category", "geometry AS place_geom"
)

sf_buildings = buildings.filter(bbox_filter).selectExpr(
    "id AS building_id", "geometry AS building_geom",
    "height", "num_floors", "class AS building_class"
)

# Match places to buildings (place point inside building polygon)
places_in_buildings = (
    sf_places.alias("p")
    .join(
        sf_buildings.alias("b"),
        F.expr("ST_CONTAINS(ST_GEOGFROMWKB(b.building_geom), ST_GEOGFROMWKB(p.place_geom))")
    )
    .selectExpr(
        "place_name", "category",
        "height AS building_height",
        "num_floors",
        "building_class"
    )
)

places_in_buildings.show(20, truncate=False)
```

```sql
-- Which roads pass through which neighborhoods?
SELECT n.names.primary AS neighborhood,
       s.class AS road_class,
       COUNT(*) AS segment_count,
       SUM(ST_LENGTH(ST_GEOGFROMWKB(s.geometry))) / 1000 AS total_km
FROM overture_division_areas n
JOIN overture_segments s
  ON ST_INTERSECTS(ST_GEOGFROMWKB(n.geometry), ST_GEOGFROMWKB(s.geometry))
WHERE n.subtype = 'neighborhood'
  AND s.subtype = 'road'
  AND n.bbox.xmin > -122.52 AND n.bbox.xmax < -122.35
  AND n.bbox.ymin > 37.70   AND n.bbox.ymax < 37.82
GROUP BY neighborhood, road_class
ORDER BY neighborhood, total_km DESC;
```
