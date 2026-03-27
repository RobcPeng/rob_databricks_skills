# ArcGIS and Esri Integration with Databricks

Patterns for integrating Esri ArcGIS tools, GeoAnalytics, and data formats with Databricks spatial workflows.

## Table of Contents

1. [Integration Overview](#1-integration-overview)
2. [ArcGIS GeoAnalytics Engine](#2-arcgis-geoanalytics-engine)
3. [Working with Esri File Formats](#3-working-with-esri-file-formats)
4. [ArcGIS Online / Enterprise Integration](#4-arcgis-online--enterprise-integration)
5. [Esri Community Maps and Overture](#5-esri-community-maps-and-overture)
6. [Visualization with ArcGIS](#6-visualization-with-arcgis)
7. [Common Workflows](#7-common-workflows)

---

## 1. Integration Overview

```
┌────────────────────────────────────────────────────────────┐
│                  Integration Architecture                   │
│                                                            │
│  ┌──────────────┐     ┌───────────────┐    ┌───────────┐  │
│  │  ArcGIS Pro  │     │  ArcGIS       │    │ Databricks│  │
│  │  Desktop     │────▶│  Online/      │◀──▶│ Workspace │  │
│  │              │     │  Enterprise   │    │           │  │
│  └──────────────┘     └───────────────┘    └───────────┘  │
│                             │                     │        │
│                       ┌─────┴─────┐        ┌──────┴─────┐ │
│                       │  Feature  │        │  Delta     │ │
│                       │  Services │        │  Tables    │ │
│                       │  (REST)   │        │  (UC)      │ │
│                       └───────────┘        └────────────┘ │
│                                                            │
│  Data Flow:                                                │
│  1. Esri data → Databricks via REST API or file export     │
│  2. Databricks analysis → Esri via GeoJSON/Feature Service │
│  3. GeoAnalytics Engine runs Esri analysis on Spark        │
└────────────────────────────────────────────────────────────┘
```

**Key integration points:**
- **ArcGIS GeoAnalytics Engine:** Esri's Spark-based analysis library (runs natively on Databricks)
- **Feature Services:** REST APIs for reading/writing ArcGIS data
- **File Formats:** Shapefiles, File Geodatabases, GeoJSON exchange
- **ArcPy:** Esri's Python library (limited on Databricks -- runs best in ArcGIS Pro)

---

## 2. ArcGIS GeoAnalytics Engine

GeoAnalytics Engine is Esri's commercial library that runs spatial analysis at scale on Spark. It provides advanced spatial operations beyond Databricks' native ST_ functions.

### Installation

```python
# GeoAnalytics Engine requires a license from Esri
# Install via execute_databricks_command MCP tool: code="%pip install geoanalytics"
# Or in a notebook:
%pip install geoanalytics

# Initialize with license
import geoanalytics
geoanalytics.auth(username="your_arcgis_user", password="your_password")
# Or use a license file
geoanalytics.auth(license_file="/dbfs/keys/geoanalytics_license.json")
```

### Key Capabilities

```python
from geoanalytics.sql import functions as ST
from geoanalytics import readers

# Read Esri data formats
df = readers.read_feature_service(
    url="https://services.arcgis.com/.../FeatureServer/0",
    spark=spark
)

# GeoAnalytics spatial operations
from geoanalytics.tools import (
    AggregatePoints,
    CreateBuffers,
    FindHotSpots,
    SummarizeWithin,
    DissolveBoundaries,
    DetectIncidents,
    FindSimilarLocations
)

# Example: Hot spot analysis (Getis-Ord Gi*)
result = FindHotSpots() \
    .setInputFeatures(crime_data) \
    .setBinSize(500) \
    .setBinSizeUnit("Meters") \
    .setNeighborhoodSize(1000) \
    .setNeighborhoodSizeUnit("Meters") \
    .run()

# Example: Summarize points within polygons
summary = SummarizeWithin() \
    .setInputFeatures(neighborhoods) \
    .setSumFeatures(events) \
    .setSummaryFields(["revenue SUM", "customers COUNT"]) \
    .run()
```

### When to Use GeoAnalytics vs Native ST_ Functions

| Capability | Native ST_ | GeoAnalytics Engine |
|-----------|-----------|-------------------|
| Basic spatial predicates | Yes | Yes |
| Distance calculations | Yes | Yes |
| Buffer/intersect/union | Yes | Yes |
| Hot spot analysis (Gi*) | No | Yes |
| Space-time pattern mining | No | Yes |
| Network analysis (routing) | No | Yes |
| Geocoding (batch) | No | Yes |
| Spatial clustering (DBSCAN) | No | Yes |
| Kernel density estimation | No | Yes |
| Read Esri formats directly | No | Yes |

---

## 3. Working with Esri File Formats

### Shapefiles

```python
# Shapefiles consist of multiple files (.shp, .shx, .dbf, .prj)
# Upload all files to a DBFS or Volume location, then read

# Option 1: Using GeoPandas (small datasets)
%pip install geopandas

import geopandas as gpd

gdf = gpd.read_file("/Volumes/my_catalog/my_schema/data/regions.shp")
spark_df = spark.createDataFrame(gdf.to_wkt())

# Option 2: Convert shapefile to GeoJSON first, then read
# (done outside Databricks, e.g., with ogr2ogr)
# ogr2ogr -f GeoJSON output.geojson input.shp
```

### File Geodatabase (.gdb)

```python
# File Geodatabases require GDAL/Fiona
%pip install geopandas fiona

import geopandas as gpd

# List layers in a geodatabase
import fiona
layers = fiona.listlayers("/Volumes/my_catalog/my_schema/data/my_data.gdb")
print(layers)

# Read a specific layer
gdf = gpd.read_file(
    "/Volumes/my_catalog/my_schema/data/my_data.gdb",
    layer="parcels"
)

# Convert to Spark DataFrame with WKT geometry
pdf = gdf.copy()
pdf['geometry_wkt'] = pdf.geometry.to_wkt()
pdf = pdf.drop(columns=['geometry'])
spark_df = spark.createDataFrame(pdf)

# Convert WKT to native GEOGRAPHY
spatial_df = spark_df.selectExpr(
    "*",
    "ST_GEOGFROMTEXT(geometry_wkt) AS geog"
)
```

### GeoJSON Exchange

```python
# Read GeoJSON
import json

# From a file
geojson_df = spark.read.json("/Volumes/my_catalog/my_schema/data/regions.geojson")

# Parse GeoJSON geometry strings
spatial_df = geojson_df.selectExpr(
    "properties.*",
    "ST_GEOGFROMGEOJSON(TO_JSON(geometry)) AS geog"
)
```

### Export to Esri-Compatible Formats

```python
# Export as GeoJSON (universal Esri interchange format)
result_df = spark.sql("""
    SELECT name, category,
           ST_ASGEOJSON(ST_GEOGFROMWKB(geometry)) AS geometry_geojson
    FROM my_analysis_results
""")

# Collect to driver and write GeoJSON FeatureCollection
import json

rows = result_df.collect()
features = []
for row in rows:
    features.append({
        "type": "Feature",
        "properties": {"name": row.name, "category": row.category},
        "geometry": json.loads(row.geometry_geojson)
    })

geojson = {"type": "FeatureCollection", "features": features}

# Save to Volume for download
with open("/Volumes/my_catalog/my_schema/exports/results.geojson", "w") as f:
    json.dump(geojson, f)
```

---

## 4. ArcGIS Online / Enterprise Integration

### Reading Feature Services

```python
import requests
import pandas as pd

# Query an ArcGIS Feature Service REST endpoint
service_url = "https://services.arcgis.com/YOUR_ORG/arcgis/rest/services/YOUR_SERVICE/FeatureServer/0/query"

params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "resultRecordCount": 5000
}

response = requests.get(service_url, params=params)
geojson_data = response.json()

# Convert to Spark DataFrame
features = geojson_data["features"]
rows = []
for f in features:
    row = f["properties"].copy()
    row["geometry_geojson"] = json.dumps(f["geometry"])
    rows.append(row)

df = spark.createDataFrame(pd.DataFrame(rows))
spatial_df = df.selectExpr("*", "ST_GEOGFROMGEOJSON(geometry_geojson) AS geog")
```

### Writing Results Back to ArcGIS

```python
# Push analysis results to ArcGIS Online as a hosted feature layer
# Requires ArcGIS API for Python

%pip install arcgis

from arcgis.gis import GIS
from arcgis.features import GeoAccessor

# Connect to ArcGIS Online
gis = GIS("https://your-org.maps.arcgis.com", "username", "password")

# Convert Spark DataFrame to Pandas GeoDataFrame
pdf = result_df.toPandas()

# Publish as hosted feature layer
feature_layer = gis.content.import_data(pdf)
print(f"Published: {feature_layer.url}")
```

---

## 5. Esri Community Maps and Overture

Esri Community Maps is one of the data sources contributing to Overture Maps (~17M buildings). When working with Overture data on Databricks, you're already using some Esri-sourced data.

```python
# Check data provenance in Overture -- which features came from Esri?
buildings = spark.read.parquet(
    "s3a://overturemaps-us-west-2/release/2026-02-18.0/theme=buildings/type=building"
)

# Filter to Esri-sourced buildings
esri_buildings = buildings.filter(
    F.exists("sources", lambda s: s.dataset.contains("Esri"))
)

esri_count = esri_buildings.count()
total_count = buildings.count()
print(f"Esri-sourced buildings: {esri_count:,} / {total_count:,} ({esri_count/total_count*100:.1f}%)")
```

---

## 6. Visualization with ArcGIS

### Export for ArcGIS Pro / Online

```python
# Create a visualization-ready dataset
viz_data = spark.sql("""
    SELECT
        names.primary AS name,
        categories.primary AS category,
        confidence,
        ST_Y(ST_GEOGFROMWKB(geometry)) AS latitude,
        ST_X(ST_GEOGFROMWKB(geometry)) AS longitude,
        ST_ASGEOJSON(ST_GEOGFROMWKB(geometry)) AS geojson
    FROM overture_places
    WHERE bbox.xmin > -122.52 AND bbox.xmax < -122.35
      AND bbox.ymin > 37.70   AND bbox.ymax < 37.82
      AND confidence > 0.8
""")

# Save as CSV (importable to ArcGIS Pro via XY Table)
viz_data.drop("geojson").write.mode("overwrite").csv(
    "/Volumes/my_catalog/my_schema/exports/places_for_arcgis",
    header=True
)

# Save as GeoJSON (directly opens in ArcGIS Pro)
# (Use the GeoJSON export pattern from Section 3 above)
```

### H3 Hexagon Visualization

```python
# Create H3 hex grid with metrics for ArcGIS visualization
hex_metrics = spark.sql("""
    SELECT h3_cell,
           event_count,
           avg_revenue,
           H3_BOUNDARYASGEOJSON(h3_cell) AS hex_boundary,
           H3_CENTERASWKT(h3_cell) AS hex_center
    FROM (
        SELECT H3_LONGLATASH3STRING(lon, lat, 8) AS h3_cell,
               COUNT(*) AS event_count,
               AVG(revenue) AS avg_revenue
        FROM sales_events
        GROUP BY h3_cell
    )
""")

# Export hexagons as GeoJSON polygons for ArcGIS
rows = hex_metrics.collect()
features = [{
    "type": "Feature",
    "properties": {
        "h3_cell": r.h3_cell,
        "event_count": r.event_count,
        "avg_revenue": float(r.avg_revenue) if r.avg_revenue else 0
    },
    "geometry": json.loads(r.hex_boundary)
} for r in rows]

geojson = {"type": "FeatureCollection", "features": features}
```

---

## 7. Common Workflows

### Workflow 1: Esri Data In → Databricks Analysis → Esri Visualization

```python
# 1. Read Esri Feature Service
service_url = "https://services.arcgis.com/.../FeatureServer/0/query"
response = requests.get(service_url, params={"where": "1=1", "outFields": "*", "f": "geojson"})
esri_data = spark.createDataFrame(pd.DataFrame([
    {**f["properties"], "geom": json.dumps(f["geometry"])}
    for f in response.json()["features"]
]))

# 2. Enrich with Overture data on Databricks
enriched = esri_data.alias("e").join(
    overture_places.alias("p"),
    F.expr("ST_DWITHIN(ST_GEOGFROMGEOJSON(e.geom), ST_GEOGFROMWKB(p.geometry), 500)")
).groupBy("e.OBJECTID").agg(
    F.count("p.id").alias("nearby_pois")
)

# 3. Export results as GeoJSON for ArcGIS
# (Use export patterns above)
```

### Workflow 2: ArcGIS Pro Spatial Analysis with Databricks Data

```
1. Create a Delta table with spatial analysis results in Databricks
2. Export as GeoJSON or CSV with lat/lon to a Volume
3. In ArcGIS Pro: Add Data → From File → select the exported file
4. Or: Use ArcGIS Pro's "Make XY Event Layer" for CSV with coordinates
5. Perform advanced ArcGIS analysis (network routing, 3D, etc.)
```

### Workflow 3: Databricks as Spatial ETL for ArcGIS Enterprise

```python
# Scheduled notebook that refreshes an ArcGIS feature layer
# 1. Run spatial analysis on Databricks
analysis_result = spark.sql("""
    SELECT region, metric, ST_ASGEOJSON(ST_GEOGFROMWKB(boundary)) AS geojson
    FROM daily_analysis_results
""")

# 2. Push to ArcGIS Enterprise via REST API
for row in analysis_result.collect():
    feature = {
        "attributes": {"region": row.region, "metric": row.metric},
        "geometry": json.loads(row.geojson)
    }
    requests.post(
        f"{feature_service_url}/addFeatures",
        data={"features": json.dumps([feature]), "f": "json"},
        auth=("user", "pass")
    )
```
