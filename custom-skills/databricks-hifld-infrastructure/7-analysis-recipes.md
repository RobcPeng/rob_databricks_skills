# Cross-Sector Analysis Recipes

Advanced analysis patterns combining multiple HIFLD datasets for infrastructure assessment, vulnerability analysis, and coverage gap identification.

## Table of Contents

1. [Infrastructure Dashboard Dataset](#1-infrastructure-dashboard-dataset)
2. [Coverage Gap Analysis](#2-coverage-gap-analysis)
3. [Vulnerability Assessment](#3-vulnerability-assessment)
4. [Proximity Analysis](#4-proximity-analysis)
5. [Infrastructure Density Scoring](#5-infrastructure-density-scoring)
6. [Colorado / Rocky Mountain Focus](#6-colorado--rocky-mountain-focus)
7. [Data Freshness and Maintenance](#7-data-freshness-and-maintenance)

---

## 1. Infrastructure Dashboard Dataset

Create a unified view across all HIFLD sectors for dashboarding:

```sql
-- Unified infrastructure points for dashboard visualization
CREATE OR REPLACE VIEW infrastructure.hifld.all_infrastructure AS

SELECT 'Energy' AS sector, 'Substation' AS asset_type, NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.electric_substations

UNION ALL
SELECT 'Energy', 'Power Plant', NAME, CITY, STATE, NULL,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.power_plants WHERE STATUS = 'OP'

UNION ALL
SELECT 'Emergency', 'Fire Station', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.fire_stations WHERE STATUS = 'OPEN'

UNION ALL
SELECT 'Emergency', 'EMS Station', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.ems_stations WHERE STATUS = 'OPEN'

UNION ALL
SELECT 'Emergency', 'Law Enforcement', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.law_enforcement

UNION ALL
SELECT 'Health', 'Hospital', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.hospitals WHERE STATUS = 'OPEN'

UNION ALL
SELECT 'Education', 'School', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.public_schools WHERE STATUS = '1'

UNION ALL
SELECT 'Water', 'Dam', DAM_NAME, NULL, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.dams

UNION ALL
SELECT 'Communications', 'Cell Tower', NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.cellular_towers

UNION ALL
SELECT 'Transportation', 'Airport', FAC_NAME, CITY, STATE, COUNTY,
       _latitude, _longitude, geometry
FROM infrastructure.hifld.airports WHERE USE = 'PU';

-- Quick count by sector
SELECT sector, asset_type, COUNT(*) AS cnt
FROM infrastructure.hifld.all_infrastructure
WHERE STATE = 'CO'
GROUP BY sector, asset_type
ORDER BY sector, cnt DESC;
```

---

## 2. Coverage Gap Analysis

### Emergency Services vs Population Centers

```sql
-- Schools (population proxy) ranked by distance to nearest emergency services
WITH school_distances AS (
    SELECT
        s.NAME AS school,
        s.CITY,
        s.ENROLLMENT,
        MIN(ST_DISTANCESPHERE(s.geometry, f.geometry)) / 1609.34 AS nearest_fire_miles,
        MIN(ST_DISTANCESPHERE(s.geometry, e.geometry)) / 1609.34 AS nearest_ems_miles,
        MIN(ST_DISTANCESPHERE(s.geometry, h.geometry)) / 1609.34 AS nearest_hospital_miles
    FROM infrastructure.hifld.public_schools s
    LEFT JOIN infrastructure.hifld.fire_stations f
        ON f.STATE = 'CO' AND f.STATUS = 'OPEN'
        AND ST_DWITHIN(s.geometry::GEOGRAPHY, f.geometry::GEOGRAPHY, 80467)  -- 50 miles max
    LEFT JOIN infrastructure.hifld.ems_stations e
        ON e.STATE = 'CO' AND e.STATUS = 'OPEN'
        AND ST_DWITHIN(s.geometry::GEOGRAPHY, e.geometry::GEOGRAPHY, 80467)
    LEFT JOIN infrastructure.hifld.hospitals h
        ON h.STATE = 'CO' AND h.STATUS = 'OPEN'
        AND ST_DWITHIN(s.geometry::GEOGRAPHY, h.geometry::GEOGRAPHY, 80467)
    WHERE s.STATE = 'CO' AND s.STATUS = '1'
    GROUP BY s.NAME, s.CITY, s.ENROLLMENT
)
SELECT school, CITY, ENROLLMENT,
       ROUND(nearest_fire_miles, 1) AS fire_mi,
       ROUND(nearest_ems_miles, 1) AS ems_mi,
       ROUND(nearest_hospital_miles, 1) AS hospital_mi,
       CASE
           WHEN nearest_hospital_miles > 30 THEN 'CRITICAL'
           WHEN nearest_hospital_miles > 15 THEN 'UNDERSERVED'
           WHEN nearest_fire_miles > 10 THEN 'LIMITED'
           ELSE 'COVERED'
       END AS coverage_status
FROM school_distances
ORDER BY nearest_hospital_miles DESC
LIMIT 50;
```

### Cell Coverage vs School Locations

```sql
-- Schools without a cell tower within 5 miles
SELECT s.NAME AS school, s.CITY, s.COUNTY, s.ENROLLMENT
FROM infrastructure.hifld.public_schools s
LEFT JOIN infrastructure.hifld.cellular_towers t
    ON ST_DWITHIN(s.geometry::GEOGRAPHY, t.geometry::GEOGRAPHY, 8046.72)
    AND t.STATE = 'CO'
WHERE s.STATE = 'CO' AND s.STATUS = '1' AND t.NAME IS NULL
ORDER BY s.ENROLLMENT DESC;
```

---

## 3. Vulnerability Assessment

### Multi-Hazard Infrastructure Risk

```sql
-- Infrastructure assets near high-hazard dams
SELECT
    d.DAM_NAME,
    d.HAZARD,
    d.CONDITION,
    d.DAM_HEIGHT,
    'Substation' AS asset_type,
    s.NAME AS asset_name,
    ROUND(ST_DISTANCESPHERE(d.geometry, s.geometry) / 1609.34, 1) AS distance_miles
FROM infrastructure.hifld.dams d
JOIN infrastructure.hifld.electric_substations s
    ON ST_DWITHIN(d.geometry::GEOGRAPHY, s.geometry::GEOGRAPHY, 16093.4)  -- 10 miles
WHERE d.STATE = 'CO' AND d.HAZARD = 'HIGH'

UNION ALL

SELECT
    d.DAM_NAME, d.HAZARD, d.CONDITION, d.DAM_HEIGHT,
    'Hospital', h.NAME,
    ROUND(ST_DISTANCESPHERE(d.geometry, h.geometry) / 1609.34, 1)
FROM infrastructure.hifld.dams d
JOIN infrastructure.hifld.hospitals h
    ON ST_DWITHIN(d.geometry::GEOGRAPHY, h.geometry::GEOGRAPHY, 16093.4)
WHERE d.STATE = 'CO' AND d.HAZARD = 'HIGH' AND h.STATUS = 'OPEN'

UNION ALL

SELECT
    d.DAM_NAME, d.HAZARD, d.CONDITION, d.DAM_HEIGHT,
    'School', s.NAME,
    ROUND(ST_DISTANCESPHERE(d.geometry, s.geometry) / 1609.34, 1)
FROM infrastructure.hifld.dams d
JOIN infrastructure.hifld.public_schools s
    ON ST_DWITHIN(d.geometry::GEOGRAPHY, s.geometry::GEOGRAPHY, 16093.4)
WHERE d.STATE = 'CO' AND d.HAZARD = 'HIGH' AND s.STATUS = '1'

ORDER BY DAM_NAME, distance_miles;
```

### Grid Resilience: Substations Supporting Hospitals

```sql
-- Hospitals and their nearest substations
SELECT h.NAME AS hospital, h.BEDS, h.TRAUMA,
       s.NAME AS nearest_substation, s.MAX_VOLT,
       ROUND(ST_DISTANCESPHERE(h.geometry, s.geometry) / 1609.34, 1) AS distance_miles
FROM infrastructure.hifld.hospitals h
CROSS JOIN infrastructure.hifld.electric_substations s
WHERE h.STATE = 'CO' AND s.STATE = 'CO'
  AND h.STATUS = 'OPEN'
QUALIFY ROW_NUMBER() OVER(PARTITION BY h.NAME ORDER BY ST_DISTANCESPHERE(h.geometry, s.geometry)) = 1
ORDER BY distance_miles DESC;
```

---

## 4. Proximity Analysis

### What's Within N Miles of a Point?

```sql
-- Everything within 5 miles of the Colorado State Capitol
WITH target AS (
    SELECT ST_POINT(-104.9847, 39.7393) AS geometry
)
SELECT 'Hospital' AS type, h.NAME, h.BEDS AS metric,
       ROUND(ST_DISTANCESPHERE(t.geometry, h.geometry) / 1609.34, 1) AS miles
FROM target t, infrastructure.hifld.hospitals h
WHERE h.STATE = 'CO' AND ST_DWITHIN(t.geometry::GEOGRAPHY, h.geometry::GEOGRAPHY, 8046.72)

UNION ALL
SELECT 'Fire Station', f.NAME, NULL,
       ROUND(ST_DISTANCESPHERE(t.geometry, f.geometry) / 1609.34, 1)
FROM target t, infrastructure.hifld.fire_stations f
WHERE f.STATE = 'CO' AND f.STATUS = 'OPEN'
  AND ST_DWITHIN(t.geometry::GEOGRAPHY, f.geometry::GEOGRAPHY, 8046.72)

UNION ALL
SELECT 'School', s.NAME, s.ENROLLMENT,
       ROUND(ST_DISTANCESPHERE(t.geometry, s.geometry) / 1609.34, 1)
FROM target t, infrastructure.hifld.public_schools s
WHERE s.STATE = 'CO' AND s.STATUS = '1'
  AND ST_DWITHIN(t.geometry::GEOGRAPHY, s.geometry::GEOGRAPHY, 8046.72)

ORDER BY type, miles;
```

---

## 5. Infrastructure Density Scoring

### H3 Composite Infrastructure Score

```sql
-- Multi-layer infrastructure density per H3 hex
WITH infrastructure_points AS (
    SELECT _latitude, _longitude, 'fire' AS layer FROM infrastructure.hifld.fire_stations WHERE STATE = 'CO' AND STATUS = 'OPEN'
    UNION ALL SELECT _latitude, _longitude, 'ems' FROM infrastructure.hifld.ems_stations WHERE STATE = 'CO' AND STATUS = 'OPEN'
    UNION ALL SELECT _latitude, _longitude, 'hospital' FROM infrastructure.hifld.hospitals WHERE STATE = 'CO' AND STATUS = 'OPEN'
    UNION ALL SELECT _latitude, _longitude, 'school' FROM infrastructure.hifld.public_schools WHERE STATE = 'CO' AND STATUS = '1'
    UNION ALL SELECT _latitude, _longitude, 'substation' FROM infrastructure.hifld.electric_substations WHERE STATE = 'CO'
    UNION ALL SELECT _latitude, _longitude, 'cell_tower' FROM infrastructure.hifld.cellular_towers WHERE STATE = 'CO'
)
SELECT
    H3_LONGLATASH3STRING(_longitude, _latitude, 6) AS h3_cell,
    COUNT(*) AS total_assets,
    COUNT(DISTINCT layer) AS layer_diversity,
    SUM(CASE WHEN layer = 'fire' THEN 1 ELSE 0 END) AS fire,
    SUM(CASE WHEN layer = 'ems' THEN 1 ELSE 0 END) AS ems,
    SUM(CASE WHEN layer = 'hospital' THEN 1 ELSE 0 END) AS hospitals,
    SUM(CASE WHEN layer = 'school' THEN 1 ELSE 0 END) AS schools,
    SUM(CASE WHEN layer = 'substation' THEN 1 ELSE 0 END) AS substations,
    SUM(CASE WHEN layer = 'cell_tower' THEN 1 ELSE 0 END) AS cell_towers,
    H3_BOUNDARYASGEOJSON(H3_LONGLATASH3STRING(_longitude, _latitude, 6)) AS geojson
FROM infrastructure_points
GROUP BY h3_cell
ORDER BY total_assets DESC;
```

---

## 6. Colorado / Rocky Mountain Focus

### Pre-Built State Filters

```python
# Rocky Mountain SLED states
ROCKY_MOUNTAIN_STATES = ['CO', 'WY', 'MT', 'UT', 'NM']

# Colorado bounding box
CO_BBOX = {"xmin": -109.06, "ymin": 36.99, "xmax": -102.04, "ymax": 41.00}

# Front Range corridor (Fort Collins → Pueblo)
FRONT_RANGE_BBOX = {"xmin": -105.50, "ymin": 38.30, "xmax": -104.50, "ymax": 40.60}

# Denver Metro
DENVER_METRO_BBOX = {"xmin": -105.20, "ymin": 39.55, "xmax": -104.70, "ymax": 39.95}
```

### Colorado Infrastructure Summary

```sql
-- Complete infrastructure inventory for Colorado
SELECT sector, asset_type, COUNT(*) AS count
FROM infrastructure.hifld.all_infrastructure
WHERE STATE = 'CO'
GROUP BY sector, asset_type
ORDER BY sector, count DESC;
```

### County-Level Infrastructure Report

```sql
-- Infrastructure assets per county
SELECT COUNTY,
       SUM(CASE WHEN asset_type = 'Fire Station' THEN 1 ELSE 0 END) AS fire_stations,
       SUM(CASE WHEN asset_type = 'Hospital' THEN 1 ELSE 0 END) AS hospitals,
       SUM(CASE WHEN asset_type = 'School' THEN 1 ELSE 0 END) AS schools,
       SUM(CASE WHEN asset_type = 'Substation' THEN 1 ELSE 0 END) AS substations,
       SUM(CASE WHEN asset_type = 'Cell Tower' THEN 1 ELSE 0 END) AS cell_towers,
       COUNT(*) AS total
FROM infrastructure.hifld.all_infrastructure
WHERE STATE = 'CO'
GROUP BY COUNTY
ORDER BY total DESC;
```

---

## 7. Data Freshness and Maintenance

### HIFLD Update Cadence

HIFLD datasets are updated at varying frequencies:

| Dataset | Typical Update Frequency |
|---------|-------------------------|
| Power Plants | Annually (EIA data) |
| Substations | Quarterly |
| Hospitals | Annually |
| Schools | Annually (NCES data) |
| Fire Stations | Semi-annually |
| Cell Towers | Quarterly (FCC data) |
| Dams | Annually (NID data) |
| Bridges | Annually (NBI data) |

### Tracking Data Currency

```sql
-- Tag tables with load date for freshness tracking
SET TAG ON TABLE infrastructure.hifld.electric_substations `last_loaded` = `2026-03-28`;
SET TAG ON TABLE infrastructure.hifld.electric_substations `hifld_vintage` = `2026-Q1`;
```

### Refresh Pattern

```python
# Periodic refresh: compare record counts to detect updates
import requests

def check_hifld_count(service_name):
    """Check current record count for an HIFLD dataset."""
    url = f"https://services1.arcgis.com/Hp6G80Pky0om6HgA/arcgis/rest/services/{service_name}/FeatureServer/0/query"
    params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
    resp = requests.get(url, params=params, timeout=30)
    return resp.json().get("count", 0)

# Compare against loaded table
for service, config in HIFLD_DATASETS.items():
    api_count = check_hifld_count(service)
    table_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{config['table']}").collect()[0][0]
    diff = api_count - table_count
    if abs(diff) > 10:
        print(f"⚠️  {service}: API={api_count}, Table={table_count}, Diff={diff}")
    else:
        print(f"✓  {service}: {table_count} records (current)")
```

---

## Related Skills

- **[databricks-geospatial](../databricks-geospatial/SKILL.md)** — ST_ functions, H3, spatial joins
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** — build dashboards from HIFLD data
- **[databricks-governance](../databricks-governance/SKILL.md)** — tag and classify infrastructure datasets
