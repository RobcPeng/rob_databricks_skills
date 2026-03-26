# Overture Maps Data on Databricks

Access and analyze the world's largest open map dataset directly from Databricks. Overture Maps Foundation (a Linux Foundation project) provides free, open, interoperable map data in GeoParquet format across 6 themes.

## Table of Contents

1. [What is Overture Maps?](#1-what-is-overture-maps)
2. [Data Access](#2-data-access)
3. [Theme: Places](#3-theme-places)
4. [Theme: Buildings](#4-theme-buildings)
5. [Theme: Transportation](#5-theme-transportation)
6. [Theme: Addresses](#6-theme-addresses)
7. [Theme: Divisions](#7-theme-divisions)
8. [Theme: Base](#8-theme-base)
9. [Loading into Delta Tables](#9-loading-into-delta-tables)
10. [Performance Tips](#10-performance-tips)

---

## 1. What is Overture Maps?

Overture Maps Foundation merges data from OpenStreetMap, Microsoft, Meta, Esri, and 175+ other organizations into a single, unified dataset with:

- **Stable IDs (GERS):** Consistent entity identifiers across releases
- **Modular schemas:** Pydantic-validated, typed schemas for each theme
- **GeoParquet format:** Cloud-native, column-oriented, with spatial metadata
- **Regular releases:** Updated approximately monthly
- **Free and open:** ODbL or CDLA-Permissive-2.0 licensed

### Data Scale

| Theme | Features | Description |
|-------|----------|-------------|
| Places | 64M+ | Businesses, landmarks, POIs |
| Buildings | 2.5B+ | Footprints from OSM, Microsoft, Google |
| Transportation | 222M+ road segments | Road/rail/water networks with connectors |
| Addresses | 446M+ | Geocoded address points in 75 countries |
| Divisions | 5.5M+ | Administrative boundaries (country to neighborhood) |
| Base | Varies | Land, water, land cover, land use, infrastructure |

---

## 2. Data Access

### Cloud Storage Paths

Data is stored as GeoParquet files partitioned by `theme` and `type`:

```
s3://overturemaps-us-west-2/release/{VERSION}/theme={THEME}/type={TYPE}/*
```

**AWS S3 (us-west-2):**
```
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=places/type=place/*
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=buildings/type=building/*
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=transportation/type=segment/*
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=addresses/type=address/*
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=divisions/type=division/*
s3://overturemaps-us-west-2/release/2025-01-22.0/theme=base/type=land/*
```

**Azure Blob Storage:**
```
wasbs://release@overturemapswestus2.blob.core.windows.net/2025-01-22.0/theme=places/type=place/*
```

> **Note:** Replace `2025-01-22.0` with the latest release version. Check https://docs.overturemaps.org/ for the current release.

### Reading Directly from S3 in Databricks

Overture Maps S3 buckets are public -- no credentials or IAM config needed. Databricks clusters can read directly using `s3a://` URIs.

**Via MCP (AI Dev Kit) -- PySpark:**
Write the following to a local file (e.g., `scripts/load_overture.py`), then execute with the `run_python_file_on_databricks` MCP tool:

```python
OVERTURE_RELEASE = "2025-01-22.0"
OVERTURE_BASE = f"s3a://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"

# Read a theme
places_df = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")
buildings_df = spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")
transport_df = spark.read.parquet(f"{OVERTURE_BASE}/theme=transportation/type=segment")
addresses_df = spark.read.parquet(f"{OVERTURE_BASE}/theme=addresses/type=address")
divisions_df = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division")
```

**Via MCP (AI Dev Kit) -- SQL:**
Use the `execute_sql` MCP tool to register as an external table:

```sql
-- Register as external table (run via execute_sql tool)
CREATE TABLE IF NOT EXISTS overture_places
USING PARQUET
LOCATION 's3a://overturemaps-us-west-2/release/2025-01-22.0/theme=places/type=place';
```

**Context reuse pattern:** After the first `run_python_file_on_databricks` call returns `cluster_id` and `context_id`, pass them to subsequent calls. This keeps the cluster warm and avoids re-reading Overture data from S3.

### Common Columns Across All Themes

Every Overture feature includes:

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | Stable GERS identifier |
| `geometry` | BINARY | WKB-encoded geometry |
| `bbox` | STRUCT | `{xmin, xmax, ymin, ymax}` bounding box |
| `version` | INT | Incremented on change |
| `sources` | ARRAY | Data provenance |
| `names` | STRUCT | `{primary: STRING, common: MAP, ...}` |

---

## Regional Data Extraction

Overture data is global. You almost always want to extract a specific region. Here are all the approaches, from simplest to most precise.

### Strategy Overview

```
┌────────────────────────────────────────────────────────────────────┐
│  Extraction Strategy Decision Tree                                 │
│                                                                    │
│  "I want data for..."                                              │
│                                                                    │
│  A specific lat/lon area ──▶  Bounding Box Filter (fastest)        │
│  A city or neighborhood  ──▶  Overture Divisions boundary join     │
│  A US state              ──▶  Divisions + country/region filter    │
│  A ZIP code              ──▶  Overture Addresses postcode filter   │
│                               OR external ZIP boundary polygons    │
│  A custom polygon        ──▶  bbox pre-filter + ST_CONTAINS       │
│  A radius around a point ──▶  bbox pre-filter + ST_DWITHIN        │
│                                                                    │
│  KEY RULE: Always apply bbox filter FIRST for predicate pushdown.  │
│  Then refine with spatial predicates or attribute filters.          │
└────────────────────────────────────────────────────────────────────┘
```

### Method 1: Bounding Box (Lat/Lon Rectangle)

The fastest approach. Uses Parquet predicate pushdown on the `bbox` struct column -- Spark skips entire row groups that fall outside the box.

```python
OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2025-01-22.0"

# Common bounding boxes (approx)
BBOXES = {
    "san_francisco":  {"xmin": -122.52, "xmax": -122.35, "ymin": 37.70, "ymax": 37.82},
    "manhattan":      {"xmin": -74.02,  "xmax": -73.93,  "ymin": 40.70, "ymax": 40.80},
    "los_angeles":    {"xmin": -118.67, "xmax": -118.15, "ymin": 33.70, "ymax": 34.15},
    "chicago":        {"xmin": -87.94,  "xmax": -87.52,  "ymin": 41.64, "ymax": 42.02},
    "london":         {"xmin": -0.51,   "xmax": 0.33,    "ymin": 51.28, "ymax": 51.69},
    "california":     {"xmin": -124.48, "xmax": -114.13, "ymin": 32.53, "ymax": 42.01},
    "texas":          {"xmin": -106.65, "xmax": -93.51,  "ymin": 25.84, "ymax": 36.50},
    "new_york_state": {"xmin": -79.76,  "xmax": -71.86,  "ymin": 40.50, "ymax": 45.02},
}

def load_overture_region(theme, feature_type, bbox, base=OVERTURE_BASE):
    """Load any Overture theme filtered to a bounding box."""
    b = bbox
    return (
        spark.read.parquet(f"{base}/theme={theme}/type={feature_type}")
        .filter(f"bbox.xmin > {b['xmin']} AND bbox.xmax < {b['xmax']}")
        .filter(f"bbox.ymin > {b['ymin']} AND bbox.ymax < {b['ymax']}")
    )

# Usage
sf_places = load_overture_region("places", "place", BBOXES["san_francisco"])
sf_buildings = load_overture_region("buildings", "building", BBOXES["san_francisco"])
sf_roads = load_overture_region("transportation", "segment", BBOXES["san_francisco"])
sf_addresses = load_overture_region("addresses", "address", BBOXES["san_francisco"])
```

```sql
-- SQL: Extract buildings in Los Angeles area
SELECT id, names.primary AS name, height, class,
       ST_GEOGFROMWKB(geometry) AS geog
FROM overture_buildings
WHERE bbox.xmin > -118.67 AND bbox.xmax < -118.15
  AND bbox.ymin > 33.70   AND bbox.ymax < 34.15;
```

**How to find a bounding box for any location:**
```python
# Option 1: Use Overture Divisions to get the bbox automatically
# (see Method 3 below)

# Option 2: Look up on bboxfinder.com or geojson.io

# Option 3: Estimate from a center point + radius
import math

def bbox_from_point(lon, lat, radius_km):
    """Create a bounding box from a center point and radius in km."""
    deg_lat = radius_km / 111.0  # ~111 km per degree latitude
    deg_lon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return {
        "xmin": lon - deg_lon, "xmax": lon + deg_lon,
        "ymin": lat - deg_lat, "ymax": lat + deg_lat
    }

# 50km radius around Denver
denver_bbox = bbox_from_point(-104.9903, 39.7392, 50)
denver_places = load_overture_region("places", "place", denver_bbox)
```

### Method 2: Radius Around a Point

Extract everything within N meters of a coordinate.

```python
# All places within 5km of Times Square
center_lon, center_lat = -73.9855, 40.7580
radius_m = 5000

# Step 1: bbox pre-filter (fast, approximate)
import math
buffer_deg = radius_m / 111000  # rough degree equivalent
bbox = {
    "xmin": center_lon - buffer_deg,
    "xmax": center_lon + buffer_deg,
    "ymin": center_lat - buffer_deg,
    "ymax": center_lat + buffer_deg
}

# Step 2: Exact distance filter
nearby_places = (
    load_overture_region("places", "place", bbox)
    .filter(f"""
        ST_DWITHIN(
            ST_GEOGFROMWKB(geometry),
            ST_POINT({center_lon}, {center_lat})::GEOGRAPHY,
            {radius_m}
        )
    """)
    .selectExpr(
        "id", "names.primary AS name", "categories.primary AS category",
        f"ST_DISTANCESPHERE(ST_GEOGFROMWKB(geometry), ST_POINT({center_lon}, {center_lat})) AS distance_m"
    )
    .orderBy("distance_m")
)
nearby_places.show(20, truncate=False)
```

```sql
-- SQL: All buildings within 2km of the Space Needle
SELECT id, names.primary AS name, height,
       ST_DISTANCESPHERE(
         ST_GEOGFROMWKB(geometry),
         ST_POINT(-122.3493, 47.6205)
       ) AS distance_m
FROM overture_buildings
WHERE bbox.xmin > -122.38 AND bbox.xmax < -122.32  -- bbox pre-filter
  AND bbox.ymin > 47.59   AND bbox.ymax < 47.65
  AND ST_DWITHIN(
    ST_GEOGFROMWKB(geometry),
    ST_POINT(-122.3493, 47.6205)::GEOGRAPHY,
    2000
  )
ORDER BY distance_m;
```

### Method 3: By State, City, or Administrative Area (Using Overture Divisions)

Use Overture's own Divisions theme to get the exact boundary polygon, then join against it.

```python
import pyspark.sql.functions as F

OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2025-01-22.0"

# Step 1: Get the boundary polygon for your target area
division_areas = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division_area")

# --- By US State ---
california = (
    division_areas
    .filter("country = 'US'")
    .filter("subtype = 'region'")           # state-level
    .filter("names.primary = 'California'")
    .select("geometry", "names.primary")
    .first()
)

# --- By City ---
san_francisco = (
    division_areas
    .filter("country = 'US'")
    .filter("subtype = 'locality'")         # city-level
    .filter("names.primary = 'San Francisco'")
    .filter("bbox.xmin > -123 AND bbox.xmax < -122")  # narrow down if common name
    .select("geometry", "names.primary")
    .first()
)

# --- By Neighborhood ---
mission_district = (
    division_areas
    .filter("country = 'US'")
    .filter("subtype = 'neighborhood'")
    .filter("names.primary = 'Mission District'")
    .filter("bbox.xmin > -122.5 AND bbox.xmax < -122.4")
    .select("geometry", "names.primary")
    .first()
)

# Step 2: Use the boundary to filter any other theme
# Broadcast the boundary polygon for efficient join
from pyspark.sql.types import BinaryType

boundary_wkb = california.geometry  # this is WKB binary

# Get bbox of the boundary for fast pre-filter
ca_bbox = (
    division_areas
    .filter("country = 'US' AND subtype = 'region' AND names.primary = 'California'")
    .selectExpr("bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax")
    .first()
)

# Step 3: Load target theme with bbox pre-filter, then spatial refinement
ca_places = (
    spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")
    .filter(f"bbox.xmin > {ca_bbox.xmin} AND bbox.xmax < {ca_bbox.xmax}")
    .filter(f"bbox.ymin > {ca_bbox.ymin} AND bbox.ymax < {ca_bbox.ymax}")
    .filter(
        F.expr(f"ST_CONTAINS(ST_GEOGFROMWKB(X'{boundary_wkb.hex()}'), ST_GEOGFROMWKB(geometry))")
    )
)
print(f"Places in California: {ca_places.count():,}")
```

**Simpler approach using a join (better for large datasets):**

```python
# Load boundary as a 1-row DataFrame and broadcast join
ca_boundary = (
    division_areas
    .filter("country = 'US' AND subtype = 'region' AND names.primary = 'California'")
    .selectExpr("geometry AS boundary_geom", "bbox")
    .limit(1)
)

# Pre-filter places with bbox, then spatial join against boundary
ca_places = (
    spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")
    .filter(f"bbox.xmin > -124.48 AND bbox.xmax < -114.13")
    .filter(f"bbox.ymin > 32.53 AND bbox.ymax < 42.01")
    .alias("p")
    .crossJoin(F.broadcast(ca_boundary).alias("b"))
    .filter("ST_CONTAINS(ST_GEOGFROMWKB(b.boundary_geom), ST_GEOGFROMWKB(p.geometry))")
    .drop("boundary_geom", "bbox")
)
```

```sql
-- SQL: All buildings in Chicago (using divisions join)
WITH chicago_boundary AS (
  SELECT geometry AS boundary_geom, bbox
  FROM overture_division_areas
  WHERE country = 'US'
    AND subtype = 'locality'
    AND names.primary = 'Chicago'
  LIMIT 1
)
SELECT b.id, b.names.primary AS name, b.height, b.class
FROM overture_buildings b, chicago_boundary c
WHERE b.bbox.xmin > -87.94 AND b.bbox.xmax < -87.52
  AND b.bbox.ymin > 41.64  AND b.bbox.ymax < 42.02
  AND ST_CONTAINS(ST_GEOGFROMWKB(c.boundary_geom), ST_CENTROID(ST_GEOGFROMWKB(b.geometry)));
```

### Division Subtypes Reference

| Subtype | Level | Example |
|---------|-------|---------|
| `country` | National | United States, France, Japan |
| `dependency` | Territory | Puerto Rico, Guam |
| `macroregion` | Macro-regional | Midwest (US), Île-de-France |
| `region` | State/province | California, Ontario, Bavaria |
| `macrocounty` | Multi-county | (varies by country) |
| `county` | County/district | Los Angeles County, Landkreis |
| `localadmin` | Municipality | Township, commune |
| `locality` | City/town | San Francisco, London, Tokyo |
| `borough` | Borough | Manhattan, Brooklyn |
| `macrohood` | Macro-neighborhood | (varies) |
| `neighborhood` | Neighborhood | Mission District, SoHo, Shibuya |
| `microhood` | Micro-neighborhood | (very granular) |

### Method 4: By ZIP Code / Postal Code

Overture's Addresses theme has `postcode` fields, but does not include ZIP code boundary polygons. Two approaches:

**Approach A: Filter Addresses by postcode, then spatial join for other themes**

```python
# Step 1: Get all addresses in target ZIP codes
target_zips = ["94102", "94103", "94104", "94105"]

zip_addresses = (
    spark.read.parquet(f"{OVERTURE_BASE}/theme=addresses/type=address")
    .filter("country = 'US'")
    .filter(F.col("postcode").isin(target_zips))
)

# Step 2: Compute convex hull of the ZIP code area
zip_extent = zip_addresses.selectExpr(
    "ST_GEOGFROMWKB(geometry) AS point"
).agg(
    F.expr("ST_CONVEXHULL(ST_UNION_AGG(point))").alias("zip_boundary")
).first()

# Step 3: Use the boundary to filter other themes
# (same pattern as Method 3 -- bbox pre-filter + ST_CONTAINS)
```

**Approach B: Use Overture Addresses directly as spatial reference**

```python
# Use H3 cells from addresses to define the ZIP area, then join
zip_h3_cells = (
    spark.read.parquet(f"{OVERTURE_BASE}/theme=addresses/type=address")
    .filter("country = 'US' AND postcode = '94105'")
    .selectExpr("H3_POINTASH3STRING(ST_GEOGFROMWKB(geometry), 8) AS h3_cell")
    .distinct()
)

# Now find all buildings in those H3 cells
zip_buildings = (
    spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")
    .filter("bbox.xmin > -122.42 AND bbox.xmax < -122.37")
    .filter("bbox.ymin > 37.77 AND bbox.ymax < 37.80")
    .selectExpr(
        "*",
        "H3_POINTASH3STRING(ST_CENTROID(ST_GEOGFROMWKB(geometry)), 8) AS h3_cell"
    )
    .join(zip_h3_cells, "h3_cell")
)
```

**Approach C: Load external ZIP code boundary shapefile**

```python
# If you need exact ZIP boundaries, load the US Census ZCTA shapefile
# Download from: https://www.census.gov/cgi-bin/geo/shapefiles/index.php (ZCTA)

%pip install geopandas

import geopandas as gpd

# Read the shapefile (uploaded to a Volume)
zcta = gpd.read_file("/Volumes/my_catalog/my_schema/geo_data/tl_2023_us_zcta520.shp")

# Filter to target ZIPs
target = zcta[zcta['ZCTA5CE20'].isin(['94105', '94102'])]

# Convert to Spark
pdf = target[['ZCTA5CE20', 'geometry']].copy()
pdf['geometry_wkt'] = pdf.geometry.to_wkt()
pdf = pdf.drop(columns=['geometry'])
zip_boundaries = spark.createDataFrame(pdf)

# Now use these polygons to filter Overture data
zip_boundaries_geo = zip_boundaries.selectExpr(
    "ZCTA5CE20 AS zipcode",
    "ST_GEOGFROMTEXT(geometry_wkt) AS boundary"
)
```

### Method 5: Custom Polygon (GeoJSON / WKT)

For an arbitrary region drawn on a map or exported from a GIS tool.

```python
# From a GeoJSON polygon (e.g., drawn on geojson.io)
custom_geojson = '''
{
  "type": "Polygon",
  "coordinates": [[
    [-122.42, 37.78], [-122.40, 37.78], [-122.40, 37.80],
    [-122.42, 37.80], [-122.42, 37.78]
  ]]
}
'''

# Compute bbox for pre-filter
import json
coords = json.loads(custom_geojson)["coordinates"][0]
lons = [c[0] for c in coords]
lats = [c[1] for c in coords]
custom_bbox = {
    "xmin": min(lons) - 0.01, "xmax": max(lons) + 0.01,
    "ymin": min(lats) - 0.01, "ymax": max(lats) + 0.01
}

# Filter
custom_places = (
    load_overture_region("places", "place", custom_bbox)
    .filter(f"ST_CONTAINS(ST_GEOGFROMGEOJSON('{custom_geojson}'), ST_GEOGFROMWKB(geometry))")
)
```

```sql
-- SQL: From a WKT polygon
SELECT id, names.primary AS name, categories.primary AS category
FROM overture_places
WHERE bbox.xmin > -122.43 AND bbox.xmax < -122.39
  AND bbox.ymin > 37.77   AND bbox.ymax < 37.81
  AND ST_CONTAINS(
    ST_GEOGFROMTEXT('POLYGON((-122.42 37.78, -122.40 37.78, -122.40 37.80, -122.42 37.80, -122.42 37.78))'),
    ST_GEOGFROMWKB(geometry)
  );
```

### Method 6: Multi-Theme Regional Extract (All Themes at Once)

```python
import pyspark.sql.functions as F

OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2025-01-22.0"
CATALOG = "my_catalog"
SCHEMA = "overture_sf"

# Define region
REGION = "San Francisco"
BBOX = {"xmin": -122.52, "xmax": -122.35, "ymin": 37.70, "ymax": 37.82}

def bbox_filter(bbox):
    return (
        f"bbox.xmin > {bbox['xmin']} AND bbox.xmax < {bbox['xmax']} "
        f"AND bbox.ymin > {bbox['ymin']} AND bbox.ymax < {bbox['ymax']}"
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Load all themes for the region
themes = {
    "places":         ("places",         "place"),
    "buildings":      ("buildings",      "building"),
    "road_segments":  ("transportation", "segment"),
    "road_connectors":("transportation", "connector"),
    "addresses":      ("addresses",      "address"),
    "divisions":      ("divisions",      "division_area"),
    "water":          ("base",           "water"),
    "land_use":       ("base",           "land_use"),
    "infrastructure": ("base",           "infrastructure"),
}

for table_name, (theme, ftype) in themes.items():
    print(f"Loading {table_name}...")
    df = (
        spark.read.parquet(f"{OVERTURE_BASE}/theme={theme}/type={ftype}")
        .filter(bbox_filter(BBOX))
        .withColumn("_overture_release", F.lit("2025-01-22.0"))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    print(f"  -> {table_name}: {spark.table(f'{CATALOG}.{SCHEMA}.{table_name}').count():,} rows")

print("Done! All themes loaded.")
```

---

## 3. Theme: Places

64M+ points of interest: businesses, landmarks, schools, hospitals, restaurants, and more.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | GERS identifier |
| `geometry` | BINARY | Point geometry (WKB) |
| `names.primary` | STRING | Primary place name |
| `categories.primary` | STRING | Main category (e.g., `restaurant`, `hospital`) |
| `categories.alternate` | ARRAY | Additional categories |
| `confidence` | DOUBLE | Existence confidence (0-1) |
| `addresses` | ARRAY | Address components with `country` field |
| `brand.names.primary` | STRING | Brand name |
| `websites` | ARRAY | Website URLs |
| `phones` | ARRAY | Phone numbers |

### Example Queries

```python
# All restaurants in San Francisco
OVERTURE_BASE = "s3a://overturemaps-us-west-2/release/2025-01-22.0"

places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")

sf_restaurants = (
    places
    .filter("bbox.xmin > -122.52 AND bbox.xmax < -122.35")
    .filter("bbox.ymin > 37.70 AND bbox.ymax < 37.82")
    .filter("categories.primary = 'restaurant'")
    .filter("confidence > 0.8")
    .selectExpr(
        "id",
        "names.primary AS name",
        "categories.primary AS category",
        "confidence",
        "ST_GEOGFROMWKB(geometry) AS location"
    )
)
sf_restaurants.show(20, truncate=False)
```

```sql
-- Top place categories in a country
SELECT categories.primary AS category,
       COUNT(*) AS place_count
FROM overture_places
WHERE ARRAY_CONTAINS(
  TRANSFORM(addresses, a -> a.country),
  'US'
)
GROUP BY category
ORDER BY place_count DESC
LIMIT 20;
```

```python
# Filter by multiple countries
import pyspark.sql.functions as F

country_filter = F.arrays_overlap(
    F.col("addresses.country"),
    F.array(*[F.lit(c) for c in ["US", "CA", "MX"]])
)

north_america_places = places.filter(country_filter)
```

---

## 4. Theme: Buildings

2.5B+ building footprints sourced from OSM, Microsoft ML footprints, Google, and Esri.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | GERS identifier |
| `geometry` | BINARY | Polygon/MultiPolygon footprint (WKB) |
| `has_parts` | BOOLEAN | True if building has sub-parts |
| `height` | DOUBLE | Height in meters |
| `num_floors` | INT | Above-ground floor count |
| `num_floors_underground` | INT | Below-ground floor count |
| `class` | STRING | Building purpose (e.g., `residential`, `commercial`) |
| `subtype` | STRING | Broad category |
| `roof_shape` | STRING | Roof type (e.g., `flat`, `gabled`) |
| `facade_color` | STRING | Hex color code |
| `level` | INT | Z-order (0 = ground) |
| `is_underground` | BOOLEAN | Subsurface structure |

### Example Queries

```python
# Tall buildings in Manhattan
buildings = spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")

manhattan_tall = (
    buildings
    .filter("bbox.xmin > -74.02 AND bbox.xmax < -73.93")
    .filter("bbox.ymin > 40.70 AND bbox.ymax < 40.80")
    .filter("height > 100")  # taller than 100 meters
    .selectExpr(
        "id",
        "names.primary AS name",
        "height",
        "num_floors",
        "class",
        "ST_AREA(ST_GEOGFROMWKB(geometry)) AS footprint_sqm",
        "ST_ASGEOJSON(ST_GEOGFROMWKB(geometry)) AS geojson"
    )
    .orderBy(F.desc("height"))
)
manhattan_tall.show(20)
```

```sql
-- Building density analysis with H3
SELECT H3_LONGLATASH3STRING(
         ST_X(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         ST_Y(ST_CENTROID(ST_GEOGFROMWKB(geometry))),
         7
       ) AS h3_cell,
       COUNT(*) AS building_count,
       AVG(height) AS avg_height,
       SUM(ST_AREA(ST_GEOGFROMWKB(geometry))) AS total_footprint_sqm
FROM overture_buildings
WHERE bbox.xmin > -74.3 AND bbox.xmax < -73.7
  AND bbox.ymin > 40.5  AND bbox.ymax < 40.9
  AND height IS NOT NULL
GROUP BY h3_cell
ORDER BY building_count DESC
LIMIT 50;
```

---

## 5. Theme: Transportation

222M+ road segments with connectors for network analysis, routing, and traffic studies.

### Feature Types

| Type | Geometry | Description |
|------|----------|-------------|
| `segment` | LineString | Road/rail/water path with attributes |
| `connector` | Point | Junction where segments meet |

### Segment Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | GERS identifier |
| `geometry` | BINARY | LineString (WKB) |
| `subtype` | STRING | `road`, `rail`, `water` |
| `class` | STRING | `motorway`, `primary`, `residential`, etc. |
| `subclass` | STRING | `alley`, `driveway`, `parking_aisle`, etc. |
| `connectors` | ARRAY | Junction IDs for network topology |
| `speed_limits` | ARRAY | Speed limit rules |
| `access_restrictions` | ARRAY | Access rules by mode |
| `road_surface` | ARRAY | Surface material |
| `width_rules` | ARRAY | Road width info |
| `routes` | ARRAY | Named routes (e.g., Interstate numbers) |

### Example Queries

```python
# Highway network in California
segments = spark.read.parquet(f"{OVERTURE_BASE}/theme=transportation/type=segment")

ca_highways = (
    segments
    .filter("bbox.xmin > -124.5 AND bbox.xmax < -114.0")
    .filter("bbox.ymin > 32.5 AND bbox.ymax < 42.0")
    .filter("subtype = 'road'")
    .filter("class IN ('motorway', 'primary', 'secondary')")
    .selectExpr(
        "id",
        "class",
        "subclass",
        "ST_LENGTH(ST_GEOGFROMWKB(geometry)) AS length_meters",
        "ST_ASGEOJSON(ST_GEOGFROMWKB(geometry)) AS geojson"
    )
)
ca_highways.show()
```

```sql
-- Road network statistics by type
SELECT class,
       COUNT(*) AS segment_count,
       SUM(ST_LENGTH(ST_GEOGFROMWKB(geometry))) / 1000 AS total_km
FROM overture_segments
WHERE subtype = 'road'
  AND bbox.xmin > -122.6 AND bbox.xmax < -122.3
  AND bbox.ymin > 37.7   AND bbox.ymax < 37.85
GROUP BY class
ORDER BY total_km DESC;
```

```sql
-- Find intersections (connectors) near a point
SELECT c.id,
       ST_DISTANCESPHERE(
         ST_GEOGFROMWKB(c.geometry),
         ST_POINT(-122.4194, 37.7749)
       ) AS distance_m
FROM overture_connectors c
WHERE bbox.xmin > -122.45 AND bbox.xmax < -122.39
  AND bbox.ymin > 37.75   AND bbox.ymax < 37.80
ORDER BY distance_m
LIMIT 20;
```

---

## 6. Theme: Addresses

446M+ geocoded address points across 75 countries.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | GERS identifier |
| `geometry` | BINARY | Point geometry (WKB) |
| `country` | STRING | ISO 3166-1 alpha-2 code |
| `street` | STRING | Street name |
| `number` | STRING | House number (may include letters) |
| `unit` | STRING | Apartment/suite |
| `postcode` | STRING | Postal/ZIP code |
| `address_levels` | ARRAY | Admin divisions (city, state, etc.) |
| `postal_city` | STRING | Mailing city name |

### Example Queries

```python
# Addresses in a specific ZIP code
addresses = spark.read.parquet(f"{OVERTURE_BASE}/theme=addresses/type=address")

nyc_addresses = (
    addresses
    .filter("country = 'US'")
    .filter("postcode = '10001'")  # Chelsea, Manhattan
    .selectExpr(
        "id",
        "CONCAT(number, ' ', street) AS full_address",
        "postcode",
        "ST_Y(ST_GEOGFROMWKB(geometry)) AS lat",
        "ST_X(ST_GEOGFROMWKB(geometry)) AS lon"
    )
)
nyc_addresses.show(20, truncate=False)
```

```sql
-- Address density by postal code
SELECT postcode,
       COUNT(*) AS address_count
FROM overture_addresses
WHERE country = 'US'
  AND bbox.xmin > -74.1 AND bbox.xmax < -73.8
  AND bbox.ymin > 40.6  AND bbox.ymax < 40.9
GROUP BY postcode
ORDER BY address_count DESC;
```

---

## 7. Theme: Divisions

5.5M+ administrative boundary features from country to neighborhood level.

### Feature Types

| Type | Geometry | Description |
|------|----------|-------------|
| `division` | Point | Administrative entity (country, state, city, etc.) |
| `division_area` | Polygon | Boundary polygon |
| `division_boundary` | LineString | Shared border between divisions |

### Subtypes (Hierarchical)

```
country > dependency > macroregion > region > macrocounty > county >
localadmin > locality > borough > macrohood > neighborhood > microhood
```

### Example Queries

```python
# US state boundaries
division_areas = spark.read.parquet(f"{OVERTURE_BASE}/theme=divisions/type=division_area")

us_states = (
    division_areas
    .filter("country = 'US'")
    .filter("subtype = 'region'")
    .selectExpr(
        "id",
        "names.primary AS name",
        "subtype",
        "ST_AREA(ST_GEOGFROMWKB(geometry)) / 1e6 AS area_sqkm",
        "ST_ASGEOJSON(ST_GEOGFROMWKB(geometry)) AS geojson"
    )
)
us_states.show()
```

```sql
-- Choropleth: population proxy by neighborhood
-- (join with places to estimate activity)
SELECT d.names.primary AS neighborhood,
       COUNT(p.id) AS place_count
FROM overture_division_areas d
JOIN overture_places p
  ON ST_CONTAINS(
    ST_GEOGFROMWKB(d.geometry),
    ST_GEOGFROMWKB(p.geometry)
  )
WHERE d.subtype = 'neighborhood'
  AND d.bbox.xmin > -122.6 AND d.bbox.xmax < -122.3
  AND d.bbox.ymin > 37.7   AND d.bbox.ymax < 37.85
GROUP BY neighborhood
ORDER BY place_count DESC;
```

---

## 8. Theme: Base

Physical geography: land, water, land cover, land use, infrastructure, and bathymetry.

### Feature Types

| Type | Description | Source |
|------|-------------|--------|
| `land` | Continental land polygons | OSM Coastlines |
| `water` | Inland + ocean water | OSM |
| `land_cover` | Vegetation/surface classification | ESA WorldCover |
| `land_use` | Human land use classification | OSM |
| `infrastructure` | Towers, bridges, piers | OSM |
| `bathymetry` | Ocean depth contours | ETOPO1 / GLOBathy |

### Example Queries

```python
# Water bodies in a region
water = spark.read.parquet(f"{OVERTURE_BASE}/theme=base/type=water")

lakes = (
    water
    .filter("bbox.xmin > -122.6 AND bbox.xmax < -122.2")
    .filter("bbox.ymin > 37.6 AND bbox.ymax < 37.9")
    .filter("class = 'lake'")
    .selectExpr(
        "id",
        "names.primary AS name",
        "class",
        "ST_AREA(ST_GEOGFROMWKB(geometry)) / 1e6 AS area_sqkm"
    )
)
lakes.show()
```

```sql
-- Land cover composition in a bounding box
SELECT class,
       COUNT(*) AS feature_count,
       SUM(ST_AREA(ST_GEOGFROMWKB(geometry))) / 1e6 AS total_sqkm
FROM overture_land_cover
WHERE bbox.xmin > -122.6 AND bbox.xmax < -121.8
  AND bbox.ymin > 37.2   AND bbox.ymax < 37.9
GROUP BY class
ORDER BY total_sqkm DESC;
```

---

## 9. Loading into Delta Tables

### Full Theme Load

```python
OVERTURE_RELEASE = "2025-01-22.0"
OVERTURE_BASE = f"s3a://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"
CATALOG = "my_catalog"
SCHEMA = "overture"

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Load places into Delta
places = spark.read.parquet(f"{OVERTURE_BASE}/theme=places/type=place")

(places
 .withColumn("_overture_release", F.lit(OVERTURE_RELEASE))
 .withColumn("_ingest_timestamp", F.current_timestamp())
 .write
 .mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.places"))
```

### Filtered Regional Load

```python
# Load only US buildings (much smaller than full dataset)
buildings = spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")

us_buildings = (
    buildings
    .filter("bbox.xmin > -125.0 AND bbox.xmax < -66.0")
    .filter("bbox.ymin > 24.0 AND bbox.ymax < 50.0")
)

(us_buildings
 .write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.us_buildings"))
```

### Incremental Load with H3 Partitioning

```python
# Add H3 partition column for spatial locality
places_h3 = (
    places
    .selectExpr(
        "*",
        "H3_LONGLATASH3STRING(ST_X(ST_GEOGFROMWKB(geometry)), ST_Y(ST_GEOGFROMWKB(geometry)), 4) AS h3_4"
    )
)

(places_h3
 .write
 .mode("overwrite")
 .partitionBy("h3_4")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.places_h3"))
```

---

## 10. Performance Tips

### Always Use Bounding Box Filters First

```sql
-- The bbox struct enables predicate pushdown on GeoParquet
-- Always filter by bbox BEFORE spatial operations
SELECT *
FROM overture_places
WHERE bbox.xmin > -122.5 AND bbox.xmax < -122.3  -- fast: pushdown
  AND bbox.ymin > 37.7   AND bbox.ymax < 37.8
  AND ST_CONTAINS(...)                             -- slow: applied after
```

### Add H3 Index for Spatial Joins

```sql
-- Precompute H3 for join-heavy tables
ALTER TABLE my_places ADD COLUMN h3_9 STRING;
UPDATE my_places
SET h3_9 = H3_LONGLATASH3STRING(
  ST_X(ST_GEOGFROMWKB(geometry)),
  ST_Y(ST_GEOGFROMWKB(geometry)),
  9
);
OPTIMIZE my_places ZORDER BY (h3_9);
```

### Use ZORDER for Spatial Locality

```sql
-- Cluster by geographic columns
OPTIMIZE overture_buildings ZORDER BY (bbox.xmin, bbox.ymin);
```

### Limit Column Selection

```python
# Overture tables have many columns -- select only what you need
df = (spark.read.parquet(f"{OVERTURE_BASE}/theme=buildings/type=building")
      .select("id", "geometry", "bbox", "height", "class"))
```

### Cache Intermediate Results

```python
# If reusing a spatial subset multiple times
sf_buildings = buildings.filter("bbox.xmin > -122.52 AND bbox.xmax < -122.35 AND bbox.ymin > 37.70 AND bbox.ymax < 37.82")
sf_buildings.cache()
sf_buildings.count()  # trigger cache
```
