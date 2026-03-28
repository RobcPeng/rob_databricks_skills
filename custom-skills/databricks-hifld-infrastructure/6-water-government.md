# Water & Government Facilities

Dams, water treatment, wastewater, federal buildings, and government facilities from HIFLD.

## Table of Contents

1. [Dams](#1-dams)
2. [Water Treatment Plants](#2-water-treatment-plants)
3. [Wastewater Treatment Plants](#3-wastewater-treatment-plants)
4. [Federal Buildings](#4-federal-buildings)
5. [State Capitol Buildings](#5-state-capitol-buildings)
6. [County / City Boundaries](#6-county--city-boundaries)
7. [Analysis Recipes](#7-analysis-recipes)

---

## 1. Dams

**~90,000+ dams** — from the National Inventory of Dams (NID). Includes large and small dams with hazard classifications.

### Feature Service

```
Service: Dams
Geometry: Point
Records: ~90,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `DAM_NAME` | STRING | Dam name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `RIVER` | STRING | River name |
| `OWNER_TYPE` | STRING | Owner type (FEDERAL, STATE, LOCAL, PRIVATE, PUBLIC UTILITY) |
| `PRIMARY_PURPOSE` | STRING | Purpose (RECREATION, WATER SUPPLY, FLOOD CONTROL, IRRIGATION, HYDROELECTRIC) |
| `HAZARD` | STRING | Hazard classification: HIGH, SIGNIFICANT, LOW, UNDETERMINED |
| `CONDITION` | STRING | Condition assessment: SATISFACTORY, FAIR, POOR, UNSATISFACTORY, NOT RATED |
| `DAM_HEIGHT` | DOUBLE | Height of dam (feet) |
| `DAM_LENGTH` | DOUBLE | Length of dam (feet) |
| `MAX_STORAGE` | DOUBLE | Maximum storage (acre-feet) |
| `NORMAL_STORAGE` | DOUBLE | Normal storage (acre-feet) |
| `NID_STORAGE` | DOUBLE | NID storage (acre-feet) |
| `SURFACE_AREA` | DOUBLE | Surface area (acres) |
| `YEAR_COMPLETED` | INT | Year completed |
| `YEAR_MODIFIED` | INT | Year of last modification |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

> **Hazard classification:** HIGH = failure would cause loss of life. SIGNIFICANT = failure would cause significant property damage. LOW = failure would cause minimal damage.

### Example Queries

```sql
-- Dam hazard classification in Colorado
SELECT HAZARD, CONDITION, COUNT(*) AS dam_count
FROM infrastructure.hifld.dams
WHERE STATE = 'CO'
GROUP BY HAZARD, CONDITION
ORDER BY HAZARD, CONDITION;

-- High-hazard dams in poor condition
SELECT DAM_NAME, RIVER, COUNTY, DAM_HEIGHT, MAX_STORAGE,
       HAZARD, CONDITION, YEAR_COMPLETED, OWNER_TYPE
FROM infrastructure.hifld.dams
WHERE STATE = 'CO'
  AND HAZARD = 'HIGH'
  AND CONDITION IN ('POOR', 'UNSATISFACTORY')
ORDER BY DAM_HEIGHT DESC;

-- Largest reservoirs by storage
SELECT DAM_NAME, RIVER, COUNTY, PRIMARY_PURPOSE,
       MAX_STORAGE, SURFACE_AREA, DAM_HEIGHT
FROM infrastructure.hifld.dams
WHERE STATE = 'CO' AND MAX_STORAGE > 0
ORDER BY MAX_STORAGE DESC
LIMIT 20;

-- Dams by purpose
SELECT PRIMARY_PURPOSE, COUNT(*) AS cnt,
       ROUND(SUM(MAX_STORAGE), 0) AS total_storage_acft
FROM infrastructure.hifld.dams
WHERE STATE = 'CO'
GROUP BY PRIMARY_PURPOSE
ORDER BY cnt DESC;

-- Aging infrastructure: dams older than 75 years
SELECT DAM_NAME, COUNTY, YEAR_COMPLETED, (2026 - YEAR_COMPLETED) AS age_years,
       HAZARD, CONDITION, DAM_HEIGHT
FROM infrastructure.hifld.dams
WHERE STATE = 'CO' AND YEAR_COMPLETED > 0 AND (2026 - YEAR_COMPLETED) > 75
ORDER BY age_years DESC;
```

---

## 2. Water Treatment Plants

**~16,000+ water treatment facilities** — public water systems.

### Feature Service

```
Service: Water_Treatment_Plants
Geometry: Point
Records: ~16,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Plant name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `SOURCE_WATER` | STRING | Source (SURFACE WATER, GROUND WATER, PURCHASED) |
| `POP_SERVED` | INT | Population served |
| `OWNER` | STRING | Owner |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Water treatment capacity by source
SELECT SOURCE_WATER, COUNT(*) AS plants, SUM(POP_SERVED) AS total_pop
FROM infrastructure.hifld.water_treatment
WHERE STATE = 'CO'
GROUP BY SOURCE_WATER
ORDER BY total_pop DESC;
```

---

## 3. Wastewater Treatment Plants

**~15,000+ wastewater treatment plants** — municipal and industrial wastewater facilities.

### Feature Service

```
Service: Wastewater_Treatment_Plants
Geometry: Point
Records: ~15,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Plant name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `TOTAL_FLOW` | DOUBLE | Total design flow (MGD — million gallons/day) |
| `ACTUAL_FLOW` | DOUBLE | Actual average flow (MGD) |
| `POP_SERVED` | INT | Population served |
| `OWNER` | STRING | Owner |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Utilization: actual vs design capacity
SELECT NAME, CITY, TOTAL_FLOW AS design_mgd, ACTUAL_FLOW AS actual_mgd,
       ROUND(ACTUAL_FLOW * 100.0 / NULLIF(TOTAL_FLOW, 0), 1) AS utilization_pct
FROM infrastructure.hifld.wastewater_treatment
WHERE STATE = 'CO' AND TOTAL_FLOW > 0
ORDER BY utilization_pct DESC;
```

---

## 4. Federal Buildings

**~8,000+ federal buildings** — owned or leased by the US General Services Administration (GSA).

### Feature Service

```
Service: Federal_Buildings
Geometry: Point
Records: ~8,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Building name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `AGENCY` | STRING | Occupying federal agency |
| `BLDG_STATUS` | STRING | Status (ACTIVE, etc.) |
| `SQFT` | INT | Square footage |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 5. State Capitol Buildings

**~50+ state capitol buildings** — one per state plus territories.

### Feature Service

```
Service: State_Capitol_Buildings
Geometry: Point
Records: ~56
```

---

## 6. County / City Boundaries

HIFLD provides boundary polygons for governance and spatial analysis.

### Feature Services

```
Service: US_County_Boundaries
Geometry: Polygon

Service: Local_Government_Boundaries
Geometry: Polygon
```

> **Polygon datasets:** Must use GeoJSON or Shapefile format, not CSV.

### Example: Load County Boundaries

```python
import requests, json
from pyspark.sql import functions as F

# County boundaries for Colorado
base_url = "https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/US_County_Boundaries/FeatureServer/0/query"
all_features = []
offset = 0

while True:
    params = {
        "where": "STATE_NAME='Colorado'", "outFields": "*", "f": "geojson",
        "resultOffset": offset, "resultRecordCount": 2000
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
df.write.mode("overwrite").saveAsTable("infrastructure.hifld.co_county_boundaries")
```

---

## 7. Analysis Recipes

### Dam Risk Near Population Centers

```sql
-- High-hazard dams near schools
SELECT d.DAM_NAME, d.HAZARD, d.CONDITION, d.DAM_HEIGHT,
       s.NAME AS nearby_school, s.ENROLLMENT,
       ROUND(ST_DISTANCESPHERE(d.geometry, s.geometry) / 1609.34, 1) AS distance_miles
FROM infrastructure.hifld.dams d
JOIN infrastructure.hifld.public_schools s
  ON ST_DWITHIN(d.geometry::GEOGRAPHY, s.geometry::GEOGRAPHY, 8046.72)  -- 5 miles
WHERE d.STATE = 'CO' AND d.HAZARD = 'HIGH'
  AND s.STATE = 'CO' AND s.STATUS = '1'
ORDER BY d.DAM_NAME, distance_miles;
```

### Water Infrastructure Coverage

```sql
-- Counties with water treatment vs wastewater treatment
WITH water AS (
    SELECT COUNTY, COUNT(*) AS water_plants, SUM(POP_SERVED) AS water_pop
    FROM infrastructure.hifld.water_treatment WHERE STATE = 'CO'
    GROUP BY COUNTY
),
waste AS (
    SELECT COUNTY, COUNT(*) AS waste_plants, SUM(POP_SERVED) AS waste_pop
    FROM infrastructure.hifld.wastewater_treatment WHERE STATE = 'CO'
    GROUP BY COUNTY
)
SELECT COALESCE(w.COUNTY, ww.COUNTY) AS county,
       COALESCE(w.water_plants, 0) AS water_plants,
       COALESCE(w.water_pop, 0) AS water_pop_served,
       COALESCE(ww.waste_plants, 0) AS wastewater_plants,
       COALESCE(ww.waste_pop, 0) AS wastewater_pop_served
FROM water w
FULL OUTER JOIN waste ww ON w.COUNTY = ww.COUNTY
ORDER BY county;
```

### Federal Presence by City

```sql
-- Federal buildings by city with total square footage
SELECT CITY, COUNT(*) AS buildings,
       SUM(SQFT) AS total_sqft,
       COLLECT_SET(AGENCY) AS agencies
FROM infrastructure.hifld.federal_buildings
WHERE STATE = 'CO' AND BLDG_STATUS = 'ACTIVE'
GROUP BY CITY
ORDER BY buildings DESC;
```

---

## Next Steps

- Cross-sector analysis → [7-analysis-recipes.md](7-analysis-recipes.md)
