# Data Access Patterns

How to download, load, and manage HIFLD infrastructure data on Databricks.

## Table of Contents

1. [Download Options](#1-download-options)
2. [ArcGIS REST API Pattern](#2-arcgis-rest-api-pattern)
3. [Loading GeoJSON into Delta](#3-loading-geojson-into-delta)
4. [Loading CSV into Delta](#4-loading-csv-into-delta)
5. [Volume-Based Workflow](#5-volume-based-workflow)
6. [Automated Multi-Dataset Loader](#6-automated-multi-dataset-loader)
7. [Regional Filtering](#7-regional-filtering)

---

## 1. Download Options

Every HIFLD dataset is available through the ArcGIS Hub portal. Each dataset page offers multiple download formats:

| Format | Best for | Geometry included |
|--------|----------|-------------------|
| **GeoJSON** | Spatial analysis on Databricks | Yes (point, line, polygon) |
| **CSV** | Tabular analysis, quick loads | Lat/Lon columns only (no complex geometry) |
| **Shapefile** | GIS tools, ArcGIS integration | Yes |
| **KML** | Google Earth visualization | Yes |
| **API (Feature Service)** | Programmatic access, filtered queries | Yes |

**Recommendation:** Use **GeoJSON** for spatial work (preserves full geometry including lines and polygons). Use **CSV** for quick tabular loads where you only need point locations.

### Hub Download URLs

All datasets follow this pattern on the HIFLD ArcGIS Hub:

```
https://hifld-geoplatform.opendata.arcgis.com/datasets/<dataset-slug>
```

From each dataset page, click **Download** → choose format. The download URL pattern:

```
https://opendata.arcgis.com/api/v3/datasets/<dataset-id>/downloads/data?format=geojson&spatialRefId=4326
https://opendata.arcgis.com/api/v3/datasets/<dataset-id>/downloads/data?format=csv&spatialRefId=4326
```

---

## 2. ArcGIS REST API Pattern

Each HIFLD dataset is served as an ArcGIS Feature Service. You can query directly without downloading the full dataset.

### Base URL Pattern

```
https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/<ServiceName>/FeatureServer/0
```

### Query Endpoints

```
# Get metadata (fields, record count)
.../FeatureServer/0?f=json

# Query all records as GeoJSON (paginated — max 2000 per request)
.../FeatureServer/0/query?where=1=1&outFields=*&f=geojson&resultOffset=0&resultRecordCount=2000

# Query with spatial filter (bounding box)
.../FeatureServer/0/query?where=1=1&outFields=*&f=geojson&geometry=-105.5,39.5,-104.5,40.0&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects

# Query with attribute filter
.../FeatureServer/0/query?where=STATE='CO'&outFields=*&f=geojson

# Get record count
.../FeatureServer/0/query?where=1=1&returnCountOnly=true&f=json
```

> **Pagination:** ArcGIS Feature Services cap responses at 1,000-2,000 records. Use `resultOffset` and `resultRecordCount` to paginate through large datasets.

### Python: Paginated Download

```python
import requests
import json

def download_hifld_geojson(service_name, where="1=1", out_fields="*", max_records=2000):
    """Download all records from an HIFLD Feature Service as GeoJSON."""
    base_url = f"https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/{service_name}/FeatureServer/0/query"

    all_features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": out_fields,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": max_records,
        }
        resp = requests.get(base_url, params=params, timeout=60)
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += len(features)

        if len(features) < max_records:
            break

    return {
        "type": "FeatureCollection",
        "features": all_features
    }

# Example: Download Colorado substations
geojson = download_hifld_geojson("Electric_Substations", where="STATE='CO'")
print(f"Downloaded {len(geojson['features'])} features")
```

---

## 3. Loading GeoJSON into Delta

### From a Volume (recommended for large datasets)

```python
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Read GeoJSON from Volume
VOLUME_PATH = "/Volumes/my_catalog/infrastructure/hifld"

with open(f"/Volumes/my_catalog/infrastructure/hifld/electric_substations.geojson") as f:
    geojson = json.load(f)

# Flatten GeoJSON features to rows
rows = []
for feature in geojson["features"]:
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    props["_geometry_type"] = geom.get("type")
    props["_geometry_json"] = json.dumps(geom)
    # Extract point coordinates if available
    if geom.get("type") == "Point" and geom.get("coordinates"):
        props["_longitude"] = geom["coordinates"][0]
        props["_latitude"] = geom["coordinates"][1]
    rows.append(props)

df = spark.createDataFrame(rows)

# Convert GeoJSON geometry to Databricks GEOMETRY type
df_geo = df.withColumn(
    "geometry",
    F.expr("ST_GEOMFROMGEOJSON(_geometry_json)")
)

# Save as Delta table
df_geo.write.mode("overwrite").saveAsTable("my_catalog.infrastructure.electric_substations")
```

### Direct from REST API (for smaller datasets or filtered queries)

```python
import requests
import json
from pyspark.sql import functions as F

# Download directly
url = "https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/Electric_Substations/FeatureServer/0/query"
params = {"where": "STATE='CO'", "outFields": "*", "f": "geojson", "resultRecordCount": 2000}
resp = requests.get(url, params=params, timeout=60)
geojson = resp.json()

# Flatten and load
rows = []
for feature in geojson["features"]:
    props = feature["properties"]
    geom = feature["geometry"]
    props["_geometry_json"] = json.dumps(geom)
    if geom["type"] == "Point":
        props["_longitude"] = geom["coordinates"][0]
        props["_latitude"] = geom["coordinates"][1]
    rows.append(props)

df = spark.createDataFrame(rows)
df = df.withColumn("geometry", F.expr("ST_GEOMFROMGEOJSON(_geometry_json)"))
df.write.mode("overwrite").saveAsTable("my_catalog.infrastructure.co_substations")
```

---

## 4. Loading CSV into Delta

CSV downloads include latitude/longitude columns but no complex geometry (lines/polygons).

```python
# Read CSV from Volume
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/my_catalog/infrastructure/hifld/electric_substations.csv")
)

# Add point geometry from lat/lon columns
# Column names vary by dataset — common patterns: LATITUDE/LONGITUDE, LAT/LON, Y/X
df_geo = df.withColumn(
    "geometry",
    F.expr("ST_POINT(LONGITUDE, LATITUDE)")
)

df_geo.write.mode("overwrite").saveAsTable("my_catalog.infrastructure.electric_substations")
```

> **CSV limitation:** CSV downloads only work for point datasets (substations, hospitals, schools). Line datasets (transmission lines) and polygon datasets (service territories) require GeoJSON or Shapefile format.

---

## 5. Volume-Based Workflow

The recommended pattern for managing HIFLD data on Databricks:

```
┌─────────────────────────────────────────────────────────────────┐
│  HIFLD Data Loading Workflow                                     │
│                                                                   │
│  1. Download from HIFLD Hub (manually or via REST API)           │
│  2. Upload to Unity Catalog Volume                               │
│     └── /Volumes/<catalog>/<schema>/hifld_raw/                   │
│  3. Parse and load into Delta tables                             │
│     └── <catalog>.<schema>.<dataset_name>                        │
│  4. Add geometry column (ST_GEOMFROMGEOJSON or ST_POINT)         │
│  5. Tag with governance metadata                                 │
│     └── SET TAG ON TABLE ... `source` = `hifld`                  │
│  6. Query with spatial SQL                                       │
└─────────────────────────────────────────────────────────────────┘
```

### Setting Up the Volume

```sql
CREATE CATALOG IF NOT EXISTS infrastructure;
CREATE SCHEMA IF NOT EXISTS infrastructure.hifld;
CREATE VOLUME IF NOT EXISTS infrastructure.hifld.raw_data;
```

### Upload via MCP

```
Tool: upload_to_volume
volume_path: /Volumes/infrastructure/hifld/raw_data/electric_substations.geojson
local_path: ./downloads/electric_substations.geojson
```

---

## 6. Automated Multi-Dataset Loader

Load multiple HIFLD datasets in one script:

```python
import requests
import json
from pyspark.sql import functions as F

CATALOG = "infrastructure"
SCHEMA = "hifld"

# Dataset registry: service_name → table_name, geometry_type
HIFLD_DATASETS = {
    "Electric_Substations": {"table": "electric_substations", "geom": "Point"},
    "Power_Plants": {"table": "power_plants", "geom": "Point"},
    "Hospitals": {"table": "hospitals", "geom": "Point"},
    "Fire_Stations": {"table": "fire_stations", "geom": "Point"},
    "Local_Law_Enforcement_Locations": {"table": "law_enforcement", "geom": "Point"},
    "EMS_Stations": {"table": "ems_stations", "geom": "Point"},
    "Public_Schools": {"table": "public_schools", "geom": "Point"},
    "Colleges_and_Universities": {"table": "colleges_universities", "geom": "Point"},
    "Nursing_Homes": {"table": "nursing_homes", "geom": "Point"},
    "Pharmacies": {"table": "pharmacies", "geom": "Point"},
    "Cellular_Towers": {"table": "cellular_towers", "geom": "Point"},
    "Dams": {"table": "dams", "geom": "Point"},
}

def load_hifld_dataset(service_name, table_name, state_filter=None, geom_type="Point"):
    """Download and load a single HIFLD dataset."""
    base_url = f"https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/{service_name}/FeatureServer/0/query"

    where = f"STATE='{state_filter}'" if state_filter else "1=1"
    all_features = []
    offset = 0

    while True:
        params = {
            "where": where, "outFields": "*", "f": "geojson",
            "resultOffset": offset, "resultRecordCount": 2000,
        }
        resp = requests.get(base_url, params=params, timeout=120)
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        offset += len(features)
        if len(features) < 2000:
            break

    rows = []
    for feat in all_features:
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        props["_geometry_json"] = json.dumps(geom)
        if geom_type == "Point" and geom.get("coordinates"):
            props["_longitude"] = geom["coordinates"][0]
            props["_latitude"] = geom["coordinates"][1]
        rows.append(props)

    if not rows:
        print(f"No features found for {service_name}")
        return

    df = spark.createDataFrame(rows)
    df = df.withColumn("geometry", F.expr("ST_GEOMFROMGEOJSON(_geometry_json)"))
    df = df.withColumn("_hifld_source", F.lit(service_name))
    df = df.withColumn("_load_timestamp", F.current_timestamp())

    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    df.write.mode("overwrite").saveAsTable(full_table)
    print(f"Loaded {len(rows)} records → {full_table}")

# Load all datasets (optionally filter to a state)
for service, config in HIFLD_DATASETS.items():
    load_hifld_dataset(service, config["table"], state_filter="CO", geom_type=config["geom"])

print("Done loading HIFLD datasets")
```

---

## 7. Regional Filtering

### Common Bounding Boxes

```python
BBOXES = {
    "colorado": {"xmin": -109.06, "ymin": 36.99, "xmax": -102.04, "ymax": 41.00},
    "denver_metro": {"xmin": -105.20, "ymin": 39.55, "xmax": -104.70, "ymax": 39.95},
    "front_range": {"xmin": -105.50, "ymin": 38.80, "xmax": -104.50, "ymax": 40.60},
    "rocky_mountain": {"xmin": -111.05, "ymin": 36.99, "xmax": -102.04, "ymax": 45.01},  # CO+WY+MT
}
```

### REST API Spatial Filter

```python
bbox = BBOXES["colorado"]
params = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "geometry": f"{bbox['xmin']},{bbox['ymin']},{bbox['xmax']},{bbox['ymax']}",
    "geometryType": "esriGeometryEnvelope",
    "spatialRel": "esriSpatialRelIntersects",
    "resultRecordCount": 2000,
}
```

### SQL Filter After Loading

```sql
-- Filter to Colorado using state column
SELECT * FROM infrastructure.hifld.electric_substations WHERE STATE = 'CO';

-- Filter using bounding box (works for any dataset)
SELECT * FROM infrastructure.hifld.electric_substations
WHERE _latitude BETWEEN 36.99 AND 41.00
  AND _longitude BETWEEN -109.06 AND -102.04;

-- Filter using spatial predicate
SELECT * FROM infrastructure.hifld.electric_substations
WHERE ST_CONTAINS(
    ST_GEOMFROMTEXT('POLYGON((-109.06 36.99, -102.04 36.99, -102.04 41.00, -109.06 41.00, -109.06 36.99))'),
    geometry
);
```

### Tagging Loaded Data

```sql
-- Tag all HIFLD tables for governance
SET TAG ON TABLE infrastructure.hifld.electric_substations `source` = `hifld`;
SET TAG ON TABLE infrastructure.hifld.electric_substations `classification` = `public`;
SET TAG ON TABLE infrastructure.hifld.electric_substations `sector` = `energy`;
SET TAG ON TABLE infrastructure.hifld.electric_substations `refresh_cadence` = `quarterly`;
```

---

## Next Steps

- Explore energy infrastructure → [2-energy-infrastructure.md](2-energy-infrastructure.md)
- Emergency services data → [3-emergency-services.md](3-emergency-services.md)
