---
name: databricks-geospatial
description: "Use when working with geospatial data on Databricks — spatial SQL (ST_ functions), H3 hexagonal indexing, Overture Maps datasets, spatial joins, distance calculations, or map visualizations."
---

# Geographic Data Analysis on Databricks

A comprehensive guide to geospatial analysis on Databricks covering spatial SQL functions, H3 hexagonal indexing, Overture Maps open data, and real-world use cases.

## How to Use This Skill

**New to geospatial on Databricks:** Start with file 1 (foundations) and work through sequentially.

**Looking for Overture Maps data:** Jump to [4-overture-maps-data.md](4-overture-maps-data.md).

**Building a specific use case:** Go to [5-use-cases-and-recipes.md](5-use-cases-and-recipes.md).

**Need ArcGIS/Esri integration:** See [6-arcgis-integration.md](6-arcgis-integration.md).

## Routing Table

| File | Topic | When to Use |
|------|-------|-------------|
| [1-geospatial-foundations.md](1-geospatial-foundations.md) | Foundations | GEOGRAPHY/GEOMETRY types, coordinate systems, data formats (WKT, WKB, GeoJSON, GeoParquet) |
| [2-spatial-sql-functions.md](2-spatial-sql-functions.md) | Spatial SQL | ST_ functions, spatial relationships, measurements, transformations, constructors |
| [3-h3-hexagonal-indexing.md](3-h3-hexagonal-indexing.md) | H3 Indexing | Hexagonal grids, resolution selection, spatial aggregation, tessellation |
| [4-overture-maps-data.md](4-overture-maps-data.md) | Overture Maps | Accessing open map data: buildings, places, transportation, addresses, divisions, base |
| [5-use-cases-and-recipes.md](5-use-cases-and-recipes.md) | Recipes | Point-in-polygon, nearest neighbor, geocoding, catchment areas, route analysis |
| [6-arcgis-integration.md](6-arcgis-integration.md) | ArcGIS/Esri | ArcGIS GeoAnalytics, Esri tools, visualization, enterprise GIS workflows |

## Execution Workflow (AI Dev Kit Integration)

This skill is designed to work with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) MCP server. All code examples can be executed through the MCP tools.

### SQL Queries

Use the `execute_sql` MCP tool for all SQL examples in this skill:

```
Tool: execute_sql
Query: SELECT ST_POINT(-122.4194, 37.7749) AS sf_point
```

For multi-statement SQL (DDL + queries), use `execute_sql_multi`.

### PySpark Code

**Write code to a local file first, then execute on Databricks:**

1. **Write** PySpark code to a local file (e.g., `scripts/geospatial_analysis.py`)
2. **Execute** using the `run_python_file_on_databricks` MCP tool
3. **Reuse context** by passing the returned `cluster_id` and `context_id` for follow-up executions

```
First execution:
  Tool: run_python_file_on_databricks
  file_path: "scripts/geospatial_analysis.py"

Returns: { success, output, error, cluster_id, context_id }

Follow-up executions (faster -- reuses cluster + installed packages):
  Tool: run_python_file_on_databricks
  file_path: "scripts/next_step.py"
  cluster_id: <from previous>
  context_id: <from previous>
```

### Installing Packages

Some recipes require additional packages (e.g., `geopandas`, `arcgis`). Install using `execute_databricks_command`:

```
Tool: execute_databricks_command
code: "%pip install geopandas fiona"
```

Save the returned `cluster_id` and `context_id` for subsequent calls.

### Reading Overture Maps from S3

Overture Maps data is on public S3 (us-west-2). Databricks clusters can read it directly -- no credentials needed. Use `s3a://` URIs in PySpark code executed via `run_python_file_on_databricks`:

```python
# This runs on the cluster -- S3 access is automatic
df = spark.read.parquet("s3a://overturemaps-us-west-2/release/2026-02-18.0/theme=places/type=place")
```

For SQL access, first register as an external table:

```
Tool: execute_sql
Query: CREATE TABLE IF NOT EXISTS overture_places
       USING PARQUET
       LOCATION 's3a://overturemaps-us-west-2/release/2026-02-18.0/theme=places/type=place'
```

### Saving Results

Save analysis results to Unity Catalog:

```python
# In your PySpark script:
CATALOG = "my_catalog"       # ask user for their catalog
SCHEMA = "geospatial"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
result_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.my_analysis")
```

Or save to a Volume for download/export:

```python
result_df.write.mode("overwrite").parquet(f"/Volumes/{CATALOG}/{SCHEMA}/exports/my_results")
```

### Cross-Skill Workflow

This skill works seamlessly with other AI Dev Kit skills:

| Workflow | Skills Used |
|----------|-------------|
| Generate synthetic geo data, then analyze | `dbldatagen` or `databricks-synthetic-data-gen` → `databricks-geospatial` |
| Load Overture data into a pipeline | `databricks-geospatial` → `databricks-spark-declarative-pipelines` |
| Build a spatial dashboard | `databricks-geospatial` → `databricks-aibi-dashboards` |
| Deploy a geocoding endpoint | `databricks-geospatial` → `databricks-model-serving` |
| Schedule spatial ETL jobs | `databricks-geospatial` → `databricks-jobs` + `databricks-bundles` |
| Analyze geo data with AI functions | `databricks-geospatial` + `databricks-ai-functions` |
| Store geo results in Lakebase | `databricks-geospatial` → `databricks-lakebase-provisioned` |

---

## Quick Start: Load & Query Spatial Data in 5 Minutes

```sql
-- 1. Create a table with geographic coordinates
CREATE TABLE stores (
  id INT,
  name STRING,
  lat DOUBLE,
  lon DOUBLE
);

INSERT INTO stores VALUES
  (1, 'Downtown',    40.7128, -74.0060),
  (2, 'Midtown',     40.7549, -73.9840),
  (3, 'Brooklyn',    40.6782, -73.9442);

-- 2. Convert to GEOGRAPHY points
SELECT name,
       ST_POINT(lon, lat) AS location,
       ST_ASGEOJSON(ST_POINT(lon, lat)) AS geojson
FROM stores;

-- 3. Calculate distances between stores (in meters)
SELECT a.name AS from_store,
       b.name AS to_store,
       ST_DISTANCESPHERE(ST_POINT(a.lon, a.lat), ST_POINT(b.lon, b.lat)) AS distance_m
FROM stores a CROSS JOIN stores b
WHERE a.id < b.id
ORDER BY distance_m;

-- 4. Find stores within 5km of a point
SELECT name
FROM stores
WHERE ST_DWITHIN(
  ST_POINT(lon, lat)::GEOGRAPHY,
  ST_GEOGFROMTEXT('POINT(-73.99 40.73)')::GEOGRAPHY,
  5000
);

-- 5. Assign H3 cells for spatial aggregation
SELECT name,
       H3_LONGLATASH3STRING(lon, lat, 9) AS h3_cell
FROM stores;
```

```python
# PySpark equivalent
from pyspark.sql import functions as F

stores = spark.table("stores")

# Add geography column and calculate distances
stores_geo = stores.selectExpr(
    "name",
    "ST_POINT(lon, lat) AS location",
    "H3_LONGLATASH3STRING(lon, lat, 9) AS h3_cell"
)
stores_geo.show()
```

## Key Concepts at a Glance

| Concept | What It Does | When to Use |
|---------|-------------|-------------|
| `GEOGRAPHY` | Spherical coordinates (lon/lat in degrees) | Real-world distances, global data |
| `GEOMETRY` | Cartesian coordinates (projected, flat plane) | Local analysis, area calculations |
| `ST_*` functions | Spatial operations (100+ functions) | Any spatial query or transformation |
| `H3_*` functions | Hexagonal grid indexing | Aggregation, heatmaps, spatial joins |
| GeoParquet | Column-oriented spatial file format | Reading/writing large spatial datasets |
| Overture Maps | Free global map data (6 themes) | Buildings, places, roads, addresses |

## Databricks Runtime Requirements

| Feature | Minimum Runtime |
|---------|----------------|
| `GEOGRAPHY` / `GEOMETRY` types | DBR 17.1+ |
| `ST_*` functions | DBR 17.1+ |
| `H3_*` functions | DBR 11.2+ |
| GeoParquet read/write | DBR 13.0+ |

## Related Skills

- **[spark-job-optimization](../spark-job-optimization/SKILL.md)** -- optimize spatial queries that process large geographic datasets
- **[databricks-dbsql](../databricks-dbsql/SKILL.md)** -- advanced SQL features including spatial functions in SQL warehouses
- **[databricks-practice-skill](../databricks-practice-skill/SKILL.md)** -- practice exercises including geospatial scenarios
