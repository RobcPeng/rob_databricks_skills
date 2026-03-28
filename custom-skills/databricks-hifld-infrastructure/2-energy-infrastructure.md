# Energy Infrastructure

Substations, transmission lines, power plants, and electric service territories from HIFLD.

## Table of Contents

1. [Electric Substations](#1-electric-substations)
2. [Electric Power Transmission Lines](#2-electric-power-transmission-lines)
3. [Power Plants](#3-power-plants)
4. [Electric Retail Service Territories](#4-electric-retail-service-territories)
5. [Energy Analysis Recipes](#5-energy-analysis-recipes)

---

## 1. Electric Substations

**~70,000+ substations** across the US — electrical facilities where voltage is transformed between transmission and distribution levels.

### Feature Service

```
Service: Electric_Substations
Geometry: Point
Records: ~70,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Substation name |
| `CITY` | STRING | City |
| `STATE` | STRING | State abbreviation |
| `ZIP` | STRING | ZIP code |
| `TYPE` | STRING | Substation type (TRANSMISSION, DISTRIBUTION, etc.) |
| `STATUS` | STRING | Operational status (IN SERVICE, etc.) |
| `OWNER` | STRING | Owner/operator |
| `LINES` | INT | Number of connected lines |
| `MAX_VOLT` | INT | Maximum voltage (kV) |
| `MIN_VOLT` | INT | Minimum voltage (kV) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |
| `NAICS_CODE` | STRING | NAICS industry code |
| `SOURCE` | STRING | Data source |

### Loading Recipe

```python
# Via REST API — filter to state
load_hifld_dataset("Electric_Substations", "electric_substations", state_filter="CO")
```

### Example Queries

```sql
-- Substations by type in Colorado
SELECT TYPE, COUNT(*) AS cnt, AVG(MAX_VOLT) AS avg_max_voltage
FROM infrastructure.hifld.electric_substations
WHERE STATE = 'CO'
GROUP BY TYPE
ORDER BY cnt DESC;

-- High-voltage substations (>= 345 kV)
SELECT NAME, CITY, STATE, MAX_VOLT, OWNER
FROM infrastructure.hifld.electric_substations
WHERE MAX_VOLT >= 345
ORDER BY MAX_VOLT DESC;

-- Substations per county (join with boundaries)
SELECT b.NAME AS county, COUNT(*) AS substation_count
FROM infrastructure.hifld.electric_substations s
JOIN infrastructure.hifld.county_boundaries b
  ON ST_CONTAINS(ST_GEOMFROMWKB(b.geometry), s.geometry)
WHERE s.STATE = 'CO'
GROUP BY b.NAME
ORDER BY substation_count DESC;
```

---

## 2. Electric Power Transmission Lines

**~180,000+ line segments** — high-voltage lines carrying electricity from generation to substations.

### Feature Service

```
Service: Electric_Power_Transmission_Lines
Geometry: Polyline
Records: ~180,000+
```

> **Important:** This is a LINE geometry dataset — CSV downloads will NOT include the line geometry. Use GeoJSON or Shapefile format.

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `ID` | INT | Unique identifier |
| `TYPE` | STRING | Line type (AC, DC) |
| `STATUS` | STRING | Operational status |
| `OWNER` | STRING | Owner/operator |
| `VOLTAGE` | INT | Voltage class (kV) |
| `VOLT_CLASS` | STRING | Voltage class label (UNDER 100, 100-161, 220-287, 345, 500, 735 AND ABOVE) |
| `SHAPE_Length` | DOUBLE | Line segment length |
| `SUB_1` | STRING | Connecting substation 1 |
| `SUB_2` | STRING | Connecting substation 2 |
| `SOURCE` | STRING | Data source |

### Loading Recipe (Line Geometry)

```python
import requests, json
from pyspark.sql import functions as F

# Transmission lines require GeoJSON for polyline geometry
# Download with pagination (lines are larger — may need more pages)
base_url = "https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/Electric_Power_Transmission_Lines/FeatureServer/0/query"

# Filter to Colorado bounding box for manageable size
bbox = "-109.06,36.99,-102.04,41.00"
all_features = []
offset = 0

while True:
    params = {
        "where": "1=1", "outFields": "*", "f": "geojson",
        "geometry": bbox, "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "resultOffset": offset, "resultRecordCount": 2000,
    }
    resp = requests.get(base_url, params=params, timeout=120)
    features = resp.json().get("features", [])
    if not features:
        break
    all_features.extend(features)
    offset += len(features)
    if len(features) < 2000:
        break

rows = []
for feat in all_features:
    props = feat["properties"]
    props["_geometry_json"] = json.dumps(feat["geometry"])
    rows.append(props)

df = spark.createDataFrame(rows)
df = df.withColumn("geometry", F.expr("ST_GEOMFROMGEOJSON(_geometry_json)"))
df.write.mode("overwrite").saveAsTable("infrastructure.hifld.co_transmission_lines")
```

### Example Queries

```sql
-- Transmission line mileage by voltage class
SELECT VOLT_CLASS, COUNT(*) AS segments,
       ROUND(SUM(ST_LENGTH(geometry::GEOGRAPHY)) / 1609.34, 1) AS total_miles
FROM infrastructure.hifld.co_transmission_lines
GROUP BY VOLT_CLASS
ORDER BY total_miles DESC;

-- Lines owned by a specific utility
SELECT OWNER, COUNT(*) AS segments, VOLT_CLASS
FROM infrastructure.hifld.co_transmission_lines
WHERE OWNER LIKE '%Xcel%' OR OWNER LIKE '%Public Service%'
GROUP BY OWNER, VOLT_CLASS
ORDER BY segments DESC;

-- Transmission corridors near populated areas (within 500m)
SELECT t.VOLTAGE, t.OWNER, p.NAME AS nearby_place
FROM infrastructure.hifld.co_transmission_lines t
JOIN infrastructure.hifld.public_schools p
  ON ST_DWITHIN(t.geometry::GEOGRAPHY, p.geometry::GEOGRAPHY, 500)
WHERE t.VOLTAGE >= 345;
```

---

## 3. Power Plants

**~10,000+ power plants** — electricity generation facilities with capacity and fuel type data.

### Feature Service

```
Service: Power_Plants
Geometry: Point
Records: ~10,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Plant name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `STATUS` | STRING | OP (operating), SB (standby), RE (retired) |
| `PRIM_FUEL` | STRING | Primary fuel type |
| `TOTAL_MW` | DOUBLE | Total nameplate capacity (MW) |
| `GEN_MW` | DOUBLE | Net generation capacity (MW) |
| `OPER_CAP` | DOUBLE | Operating capacity (MW) |
| `OWNER` | STRING | Owner/operator |
| `SECTOR` | STRING | Sector (Electric Utility, Industrial, Commercial) |
| `SOURCE_DESC` | STRING | Primary energy source description |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Fuel Type Codes

| Code | Fuel Type |
|------|-----------|
| `NG` | Natural Gas |
| `SUN` | Solar |
| `WND` | Wind |
| `WAT` | Hydroelectric |
| `BIT` | Bituminous Coal |
| `NUC` | Nuclear |
| `DFO` | Distillate Fuel Oil |
| `SUB` | Subbituminous Coal |
| `WDS` | Wood/Wood Waste |
| `GEO` | Geothermal |
| `LFG` | Landfill Gas |
| `BLQ` | Black Liquor |
| `MWH` | Battery Storage |

### Example Queries

```sql
-- Generation capacity by fuel type (Colorado)
SELECT PRIM_FUEL, SOURCE_DESC,
       COUNT(*) AS plant_count,
       ROUND(SUM(TOTAL_MW), 1) AS total_capacity_mw
FROM infrastructure.hifld.power_plants
WHERE STATE = 'CO' AND STATUS = 'OP'
GROUP BY PRIM_FUEL, SOURCE_DESC
ORDER BY total_capacity_mw DESC;

-- Top 10 largest plants in the Rocky Mountain region
SELECT NAME, STATE, CITY, PRIM_FUEL, TOTAL_MW, OWNER
FROM infrastructure.hifld.power_plants
WHERE STATE IN ('CO', 'WY', 'MT', 'UT', 'NM') AND STATUS = 'OP'
ORDER BY TOTAL_MW DESC
LIMIT 10;

-- Renewable vs fossil generation mix
SELECT
    CASE
        WHEN PRIM_FUEL IN ('SUN', 'WND', 'WAT', 'GEO') THEN 'Renewable'
        WHEN PRIM_FUEL IN ('NUC') THEN 'Nuclear'
        WHEN PRIM_FUEL IN ('MWH') THEN 'Storage'
        ELSE 'Fossil'
    END AS category,
    COUNT(*) AS plant_count,
    ROUND(SUM(TOTAL_MW), 1) AS total_mw,
    ROUND(SUM(TOTAL_MW) * 100.0 / SUM(SUM(TOTAL_MW)) OVER(), 1) AS pct_capacity
FROM infrastructure.hifld.power_plants
WHERE STATE = 'CO' AND STATUS = 'OP'
GROUP BY 1
ORDER BY total_mw DESC;
```

---

## 4. Electric Retail Service Territories

**~3,200+ service territory polygons** — geographic boundaries of electric utilities.

### Feature Service

```
Service: Electric_Retail_Service_Territories
Geometry: Polygon
Records: ~3,200+
```

> **Important:** Polygon dataset — CSV downloads will NOT include geometry. Use GeoJSON.

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Utility name |
| `STATE` | STRING | State |
| `TYPE` | STRING | Utility type (INVESTOR OWNED, MUNICIPAL, COOPERATIVE, etc.) |
| `CUSTOMERS` | INT | Number of customers served |
| `REVENUE` | DOUBLE | Annual revenue ($) |
| `SALES_MWH` | DOUBLE | Annual sales (MWh) |
| `CUST_COUNT` | INT | Customer count |

### Example Queries

```sql
-- Which utility serves a specific location?
SELECT NAME, TYPE, CUSTOMERS
FROM infrastructure.hifld.service_territories
WHERE ST_CONTAINS(
    ST_GEOMFROMWKB(geometry),
    ST_POINT(-104.9903, 39.7392)  -- Denver
);

-- Utility market share by customer count
SELECT NAME, TYPE, CUSTOMERS,
       ROUND(CUSTOMERS * 100.0 / SUM(CUSTOMERS) OVER(), 1) AS market_pct
FROM infrastructure.hifld.service_territories
WHERE STATE = 'CO'
ORDER BY CUSTOMERS DESC;
```

---

## 5. Energy Analysis Recipes

### Grid Vulnerability: Substations Near Flood Zones

```sql
-- Substations within 1km of dams
SELECT s.NAME AS substation, s.MAX_VOLT,
       d.DAM_NAME, d.HAZARD,
       ROUND(ST_DISTANCESPHERE(s.geometry, d.geometry)) AS distance_m
FROM infrastructure.hifld.electric_substations s
JOIN infrastructure.hifld.dams d
  ON ST_DWITHIN(s.geometry::GEOGRAPHY, d.geometry::GEOGRAPHY, 1000)
WHERE s.STATE = 'CO' AND d.HAZARD = 'HIGH'
ORDER BY distance_m;
```

### Generation Capacity Within Service Territories

```sql
-- How much generation capacity does each utility have?
SELECT t.NAME AS utility, t.CUSTOMERS,
       COUNT(p.NAME) AS plants,
       ROUND(SUM(p.TOTAL_MW), 1) AS total_mw
FROM infrastructure.hifld.service_territories t
JOIN infrastructure.hifld.power_plants p
  ON ST_CONTAINS(ST_GEOMFROMWKB(t.geometry), p.geometry)
WHERE t.STATE = 'CO' AND p.STATUS = 'OP'
GROUP BY t.NAME, t.CUSTOMERS
ORDER BY total_mw DESC;
```

### Substation Density by H3 Hex

```sql
-- H3 heatmap of substation density
SELECT H3_LONGLATASH3STRING(_longitude, _latitude, 5) AS h3_cell,
       COUNT(*) AS substation_count,
       AVG(MAX_VOLT) AS avg_max_voltage
FROM infrastructure.hifld.electric_substations
WHERE STATE = 'CO'
GROUP BY h3_cell
ORDER BY substation_count DESC;
```

---

## Next Steps

- Emergency services data → [3-emergency-services.md](3-emergency-services.md)
- Cross-sector analysis → [7-analysis-recipes.md](7-analysis-recipes.md)
