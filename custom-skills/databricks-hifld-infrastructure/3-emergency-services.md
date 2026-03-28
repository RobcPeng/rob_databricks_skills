# Emergency Services

Fire stations, EMS, law enforcement, 911 call centers, and emergency operations centers from HIFLD.

## Table of Contents

1. [Fire Stations](#1-fire-stations)
2. [EMS Stations](#2-ems-stations)
3. [Law Enforcement Locations](#3-law-enforcement-locations)
4. [PSAP 911 Call Centers](#4-psap-911-call-centers)
5. [Emergency Operations Centers](#5-emergency-operations-centers)
6. [Emergency Services Analysis Recipes](#6-emergency-services-analysis-recipes)

---

## 1. Fire Stations

**~60,000+ fire stations** — career, volunteer, and combination departments.

### Feature Service

```
Service: Fire_Stations
Geometry: Point
Records: ~60,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Station name |
| `CITY` | STRING | City |
| `STATE` | STRING | State abbreviation |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County name |
| `ADDRESS` | STRING | Street address |
| `TYPE` | STRING | Type (CAREER, VOLUNTEER, COMBINATION) |
| `STATUS` | STRING | Operational status (OPEN, CLOSED) |
| `TELEPHONE` | STRING | Phone number |
| `POPULATION` | INT | Population served |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Fire station types in Colorado
SELECT TYPE, COUNT(*) AS station_count
FROM infrastructure.hifld.fire_stations
WHERE STATE = 'CO' AND STATUS = 'OPEN'
GROUP BY TYPE
ORDER BY station_count DESC;

-- Volunteer vs career ratio by state (Rocky Mountain)
SELECT STATE, TYPE, COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY STATE), 1) AS pct
FROM infrastructure.hifld.fire_stations
WHERE STATE IN ('CO', 'WY', 'MT', 'UT', 'NM') AND STATUS = 'OPEN'
GROUP BY STATE, TYPE
ORDER BY STATE, cnt DESC;

-- Fire stations with largest reported service populations
SELECT NAME, CITY, STATE, POPULATION, TYPE
FROM infrastructure.hifld.fire_stations
WHERE STATE = 'CO' AND POPULATION > 0
ORDER BY POPULATION DESC
LIMIT 20;
```

---

## 2. EMS Stations

**~28,000+ EMS stations** — ambulance services, rescue squads, paramedic units.

### Feature Service

```
Service: EMS_Stations
Geometry: Point
Records: ~28,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Station name |
| `CITY` | STRING | City |
| `STATE` | STRING | State abbreviation |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `ADDRESS` | STRING | Street address |
| `TYPE` | STRING | Service level (BLS, ALS, BLS/ALS) |
| `STATUS` | STRING | Operational status |
| `OWNER` | STRING | Ownership type |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

> **BLS** = Basic Life Support, **ALS** = Advanced Life Support

### Example Queries

```sql
-- EMS service level distribution
SELECT TYPE, COUNT(*) AS station_count
FROM infrastructure.hifld.ems_stations
WHERE STATE = 'CO' AND STATUS = 'OPEN'
GROUP BY TYPE;

-- Areas with EMS but no fire stations within 5km
SELECT e.NAME AS ems_station, e.CITY
FROM infrastructure.hifld.ems_stations e
LEFT JOIN infrastructure.hifld.fire_stations f
  ON ST_DWITHIN(e.geometry::GEOGRAPHY, f.geometry::GEOGRAPHY, 5000)
WHERE e.STATE = 'CO' AND e.STATUS = 'OPEN'
  AND f.NAME IS NULL;
```

---

## 3. Law Enforcement Locations

**~26,000+ law enforcement facilities** — local police, sheriff offices, state police.

### Feature Service

```
Service: Local_Law_Enforcement_Locations
Geometry: Point
Records: ~26,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Agency/facility name |
| `CITY` | STRING | City |
| `STATE` | STRING | State abbreviation |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `ADDRESS` | STRING | Street address |
| `TYPE` | STRING | Agency type (LOCAL POLICE, SHERIFF, STATE POLICE, etc.) |
| `STATUS` | STRING | Operational status |
| `POPULATION` | INT | Population served |
| `OFFICERS` | INT | Number of sworn officers |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Officer-to-population ratio by agency
SELECT NAME, CITY, TYPE, OFFICERS, POPULATION,
       ROUND(POPULATION / NULLIF(OFFICERS, 0), 0) AS residents_per_officer
FROM infrastructure.hifld.law_enforcement
WHERE STATE = 'CO' AND OFFICERS > 0 AND POPULATION > 0
ORDER BY residents_per_officer DESC;

-- Law enforcement by type
SELECT TYPE, COUNT(*) AS facility_count, SUM(OFFICERS) AS total_officers
FROM infrastructure.hifld.law_enforcement
WHERE STATE = 'CO'
GROUP BY TYPE
ORDER BY total_officers DESC;
```

---

## 4. PSAP 911 Call Centers

**~6,500+ Public Safety Answering Points** — facilities that receive 911 calls.

### Feature Service

```
Service: Public_Safety_Answering_Points_PSAP
Geometry: Point
Records: ~6,500+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | PSAP name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `TYPE` | STRING | PRIMARY or SECONDARY |
| `COUNTY` | STRING | County |
| `LEVEL` | STRING | Government level (CITY, COUNTY, STATE, TRIBAL) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- 911 centers by level of government
SELECT LEVEL, TYPE, COUNT(*) AS cnt
FROM infrastructure.hifld.psap_911
WHERE STATE = 'CO'
GROUP BY LEVEL, TYPE
ORDER BY cnt DESC;
```

---

## 5. Emergency Operations Centers

**~2,500+ EOCs** — facilities that coordinate emergency response during disasters.

### Feature Service

```
Service: Emergency_Operations_Centers_EOC
Geometry: Point
Records: ~2,500+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | EOC name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `LEVEL` | STRING | Government level (LOCAL, COUNTY, STATE, FEDERAL) |
| `STATUS` | STRING | Status |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 6. Emergency Services Analysis Recipes

### Response Coverage Gaps

```sql
-- Areas farther than 10 minutes from any fire station
-- (approximate: 10 min ≈ 8 km at average urban response speed)
-- Use H3 hexagons to identify coverage gaps
WITH fire_coverage AS (
    SELECT H3_LONGLATASH3STRING(_longitude, _latitude, 7) AS h3_cell
    FROM infrastructure.hifld.fire_stations
    WHERE STATE = 'CO' AND STATUS = 'OPEN'
),
all_cells AS (
    -- Generate H3 cells from substations/schools as population proxies
    SELECT DISTINCT H3_LONGLATASH3STRING(_longitude, _latitude, 7) AS h3_cell
    FROM infrastructure.hifld.public_schools
    WHERE STATE = 'CO'
)
SELECT a.h3_cell,
       H3_CENTERASWKT(a.h3_cell) AS center_wkt
FROM all_cells a
LEFT JOIN fire_coverage f ON a.h3_cell = f.h3_cell
WHERE f.h3_cell IS NULL;
```

### Combined Emergency Services Density

```sql
-- Emergency services per H3 hex (all types combined)
SELECT h3_cell, fire_count, ems_count, le_count,
       (fire_count + ems_count + le_count) AS total_services,
       H3_BOUNDARYASGEOJSON(h3_cell) AS geojson
FROM (
    SELECT
        H3_LONGLATASH3STRING(f._longitude, f._latitude, 6) AS h3_cell,
        COUNT(DISTINCT CASE WHEN f._hifld_source = 'Fire_Stations' THEN f.NAME END) AS fire_count,
        COUNT(DISTINCT CASE WHEN f._hifld_source = 'EMS_Stations' THEN f.NAME END) AS ems_count,
        COUNT(DISTINCT CASE WHEN f._hifld_source = 'Local_Law_Enforcement_Locations' THEN f.NAME END) AS le_count
    FROM (
        SELECT NAME, _longitude, _latitude, _hifld_source FROM infrastructure.hifld.fire_stations WHERE STATE = 'CO'
        UNION ALL
        SELECT NAME, _longitude, _latitude, _hifld_source FROM infrastructure.hifld.ems_stations WHERE STATE = 'CO'
        UNION ALL
        SELECT NAME, _longitude, _latitude, _hifld_source FROM infrastructure.hifld.law_enforcement WHERE STATE = 'CO'
    ) f
    GROUP BY h3_cell
)
ORDER BY total_services DESC;
```

### Nearest Hospital to Each Fire Station

```sql
SELECT f.NAME AS fire_station, h.NAME AS nearest_hospital,
       ROUND(ST_DISTANCESPHERE(f.geometry, h.geometry) / 1609.34, 1) AS distance_miles
FROM infrastructure.hifld.fire_stations f
CROSS JOIN infrastructure.hifld.hospitals h
WHERE f.STATE = 'CO' AND h.STATE = 'CO'
  AND f.STATUS = 'OPEN'
QUALIFY ROW_NUMBER() OVER(PARTITION BY f.NAME ORDER BY ST_DISTANCESPHERE(f.geometry, h.geometry)) = 1
ORDER BY distance_miles DESC
LIMIT 20;
```

---

## Next Steps

- Health & education data → [4-health-education.md](4-health-education.md)
- Cross-sector analysis → [7-analysis-recipes.md](7-analysis-recipes.md)
