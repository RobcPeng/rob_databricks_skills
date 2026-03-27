# Geospatial Foundations on Databricks

Core concepts, data types, coordinate systems, and spatial data formats for geographic analysis on Databricks.

## Table of Contents

1. [Spatial Data Types](#1-spatial-data-types)
2. [Coordinate Systems and SRIDs](#2-coordinate-systems-and-srids)
3. [Spatial Data Formats](#3-spatial-data-formats)
4. [Working with GeoParquet](#4-working-with-geoparquet)
5. [Creating Spatial Tables](#5-creating-spatial-tables)
6. [Converting Between Formats](#6-converting-between-formats)

---

## 1. Spatial Data Types

Databricks (DBR 17.1+) provides two native spatial types:

### GEOGRAPHY

Represents locations on a sphere (Earth). Coordinates are **longitude and latitude in degrees**. Distances are measured along the curved surface in **meters**.

```sql
-- Create a GEOGRAPHY from longitude, latitude
SELECT ST_GEOGFROMTEXT('POINT(-122.4194 37.7749)') AS san_francisco;

-- From a WKT polygon
SELECT ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') AS area;
```

**Use GEOGRAPHY when:**
- Working with global data spanning large areas
- Calculating real-world distances in meters
- Handling data crossing the antimeridian or poles

### GEOMETRY

Represents shapes on a flat 2D plane. Coordinates are in whatever unit the projection uses (often meters or feet). Distances are Euclidean (straight line).

```sql
-- Create a GEOMETRY with an SRID
SELECT ST_GEOMFROMTEXT('POINT(500000 4649776)', 32610) AS utm_point;

-- Area calculation in projected coordinates (result in square meters if SRID is metric)
SELECT ST_AREA(ST_GEOMFROMTEXT('POLYGON((0 0, 1000 0, 1000 1000, 0 1000, 0 0))', 32610)) AS area_sqm;
```

**Use GEOMETRY when:**
- Working with projected/local coordinate systems
- Performing precise area calculations
- Integrating with CAD or engineering data

### Casting Between Types

```sql
-- GEOGRAPHY to GEOMETRY (for local operations)
SELECT ST_POINT(-122.4, 37.8)::GEOMETRY AS geom_point;

-- GEOMETRY to GEOGRAPHY (for distance calculations)
SELECT some_geometry_col::GEOGRAPHY AS geog_col FROM my_table;
```

---

## 2. Coordinate Systems and SRIDs

An SRID (Spatial Reference Identifier) defines how coordinates map to locations on Earth.

| SRID | Name | Use Case | Units |
|------|------|----------|-------|
| 4326 | WGS 84 | GPS, web maps, most global data | degrees |
| 3857 | Web Mercator | Google/Bing/OSM tile maps | meters (distorted) |
| 32610-32660 | UTM Zones | Regional precision (N. hemisphere) | meters |
| 32710-32760 | UTM Zones | Regional precision (S. hemisphere) | meters |
| 2163 | US National Atlas | US-wide equal area | meters |

```sql
-- Check the SRID of a geometry
SELECT ST_SRID(ST_GEOMFROMTEXT('POINT(-122.4 37.8)', 4326));
-- Returns: 4326

-- Set/change SRID
SELECT ST_SETSRID(ST_POINT(-122.4, 37.8), 4326) AS point_with_srid;

-- Transform between coordinate systems
SELECT ST_TRANSFORM(
  ST_GEOMFROMTEXT('POINT(-122.4194 37.7749)', 4326),
  32610  -- UTM Zone 10N (San Francisco)
) AS utm_point;

-- Estimate best projected SRID for a location
SELECT ST_ESTIMATESRID(ST_GEOMFROMTEXT('POINT(-122.4 37.8)', 4326)) AS best_srid;
```

**Rule of thumb:** Store data in SRID 4326 (WGS 84). Transform to a local projection only when you need precise area/distance in projected units.

---

## 3. Spatial Data Formats

### WKT (Well-Known Text) -- Human-Readable

```
POINT(-122.4194 37.7749)
LINESTRING(-122.4 37.7, -122.5 37.8, -122.3 37.9)
POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))
MULTIPOINT((-122.4 37.7), (-122.5 37.8))
MULTIPOLYGON(((...)), ((...)))
GEOMETRYCOLLECTION(POINT(...), LINESTRING(...))
```

```sql
-- Parse WKT
SELECT ST_GEOGFROMTEXT('POINT(-122.4194 37.7749)') AS point;
SELECT ST_GEOGFROMWKT('POLYGON((...))') AS polygon;

-- Output as WKT
SELECT ST_ASWKT(geography_col) FROM spatial_table;
```

### WKB (Well-Known Binary) -- Compact Storage

```sql
-- Parse WKB
SELECT ST_GEOGFROMWKB(wkb_binary_col) AS geog;

-- Output as WKB
SELECT ST_ASWKB(geography_col) AS wkb FROM spatial_table;
```

### GeoJSON -- Web-Friendly

```sql
-- Parse GeoJSON
SELECT ST_GEOGFROMGEOJSON('{"type":"Point","coordinates":[-122.4194,37.7749]}') AS point;

-- Output as GeoJSON
SELECT ST_ASGEOJSON(ST_POINT(-122.4194, 37.7749)) AS geojson;
-- Returns: {"type":"Point","coordinates":[-122.4194,37.7749]}
```

### EWKT / EWKB -- WKT/WKB with Embedded SRID

```sql
-- EWKT includes SRID prefix
SELECT ST_GEOMFROMEWKT('SRID=4326;POINT(-122.4194 37.7749)') AS geom;

-- Output as EWKT
SELECT ST_ASEWKT(geometry_col) FROM spatial_table;
```

### Geohash -- Grid-Based Encoding

```sql
-- Convert point to geohash (precision 1-12, higher = more precise)
SELECT ST_GEOHASH(ST_POINT(-122.4194, 37.7749), 7) AS hash;
-- Returns: 9q8yyk9

-- Decode geohash to polygon
SELECT ST_GEOMFROMGEOHASH('9q8yyk9') AS cell_polygon;

-- Decode to center point
SELECT ST_POINTFROMGEOHASH('9q8yyk9') AS center;
```

---

## 4. Working with GeoParquet

GeoParquet stores geometry columns as WKB in Parquet files with spatial metadata. It is the standard format for large-scale spatial data (used by Overture Maps, etc.).

### Reading GeoParquet

```python
# Read GeoParquet files -- geometry is stored as binary (WKB)
df = spark.read.parquet("s3://overturemaps-us-west-2/release/2026-02-18.0/theme=places/type=place")

# The geometry column is binary WKB -- convert to GEOGRAPHY
df_geo = df.selectExpr(
    "id",
    "names",
    "ST_GEOGFROMWKB(geometry) AS location",
    "bbox"
)
```

```sql
-- SQL: Read and convert geometry
CREATE TABLE places
USING PARQUET
LOCATION 's3://overturemaps-us-west-2/release/2026-02-18.0/theme=places/type=place';

SELECT id,
       names.primary AS name,
       ST_GEOGFROMWKB(geometry) AS location
FROM places
WHERE bbox.xmin > -74.1 AND bbox.xmax < -73.9
  AND bbox.ymin > 40.6  AND bbox.ymax < 40.9;
```

### Writing GeoParquet

```python
# Convert GEOGRAPHY back to WKB for storage
df_output = df_geo.selectExpr(
    "id",
    "name",
    "ST_ASWKB(location) AS geometry"
)

df_output.write.mode("overwrite").parquet("/mnt/output/my_spatial_data")
```

### Bounding Box Filtering (Predicate Pushdown)

GeoParquet files from Overture Maps include a `bbox` struct column. Use it for efficient filtering **before** expensive spatial operations:

```sql
-- Fast: uses Parquet predicate pushdown on bbox columns
SELECT * FROM buildings
WHERE bbox.xmin > -122.5 AND bbox.xmax < -122.3
  AND bbox.ymin > 37.7   AND bbox.ymax < 37.8;

-- Slow: full spatial predicate without bbox pre-filter
SELECT * FROM buildings
WHERE ST_INTERSECTS(ST_GEOGFROMWKB(geometry), ST_GEOGFROMTEXT('POLYGON((...))'));

-- Best: combine bbox filter + spatial predicate
SELECT * FROM buildings
WHERE bbox.xmin > -122.5 AND bbox.xmax < -122.3
  AND bbox.ymin > 37.7   AND bbox.ymax < 37.8
  AND ST_INTERSECTS(
    ST_GEOGFROMWKB(geometry),
    ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.3 37.7, -122.3 37.8, -122.5 37.8, -122.5 37.7))')
  );
```

---

## 5. Creating Spatial Tables

### From Lat/Lon Columns

```sql
-- Existing table with lat/lon
CREATE TABLE locations AS
SELECT *,
       ST_POINT(longitude, latitude) AS geog_point
FROM raw_locations;
```

### From Address Geocoding (using AI Functions)

```sql
-- Use ai_query to geocode addresses (requires a serving endpoint)
SELECT address,
       ai_query(
         'databricks-meta-llama-3-3-70b-instruct',
         CONCAT('Return ONLY the latitude and longitude as JSON {"lat":X,"lon":Y} for: ', address)
       ) AS geocode_result
FROM addresses;
```

### From GeoJSON Files

```python
import json
from pyspark.sql.types import StructType, StructField, StringType

# Read GeoJSON file
geojson_text = spark.read.text("/mnt/data/regions.geojson").collect()[0][0]
geojson = json.loads(geojson_text)

# Convert features to rows
rows = []
for feature in geojson["features"]:
    rows.append((
        feature["properties"].get("name", ""),
        json.dumps(feature["geometry"])
    ))

df = spark.createDataFrame(rows, ["name", "geojson"])
df_geo = df.selectExpr("name", "ST_GEOGFROMGEOJSON(geojson) AS geometry")
```

---

## 6. Converting Between Formats

```sql
-- Conversion cheat sheet
SELECT
  -- Input: create from various formats
  ST_GEOGFROMTEXT('POINT(-122.4 37.8)')           AS from_wkt,
  ST_GEOGFROMGEOJSON('{"type":"Point","coordinates":[-122.4,37.8]}') AS from_geojson,
  ST_GEOGFROMWKB(binary_col)                       AS from_wkb,

  -- Output: export to various formats
  ST_ASWKT(geog_col)                               AS to_wkt,
  ST_ASGEOJSON(geog_col)                            AS to_geojson,
  ST_ASWKB(geog_col)                                AS to_wkb,
  ST_ASEWKT(geom_col)                               AS to_ewkt,

  -- Type casting
  geog_col::GEOMETRY                                AS to_geometry,
  geom_col::GEOGRAPHY                               AS to_geography
FROM my_table;
```

| From \ To | WKT | WKB | GeoJSON | GEOGRAPHY | GEOMETRY |
|-----------|-----|-----|---------|-----------|----------|
| **WKT** | -- | `ST_GEOGFROMTEXT` then `ST_ASWKB` | `ST_GEOGFROMTEXT` then `ST_ASGEOJSON` | `ST_GEOGFROMTEXT` | `ST_GEOMFROMTEXT` |
| **WKB** | `ST_GEOGFROMWKB` then `ST_ASWKT` | -- | `ST_GEOGFROMWKB` then `ST_ASGEOJSON` | `ST_GEOGFROMWKB` | `ST_GEOMFROMWKB` |
| **GeoJSON** | `ST_GEOGFROMGEOJSON` then `ST_ASWKT` | `ST_GEOGFROMGEOJSON` then `ST_ASWKB` | -- | `ST_GEOGFROMGEOJSON` | `ST_GEOMFROMGEOJSON` |
| **Lat/Lon** | -- | -- | -- | `ST_POINT(lon, lat)` | `ST_POINT(lon, lat)` |
