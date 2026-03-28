# Transportation & Communications

Airports, bridges, tunnels, railroad crossings, ports, cell towers, and broadcast infrastructure from HIFLD.

## Table of Contents

1. [Airports](#1-airports)
2. [Bridges](#2-bridges)
3. [Tunnels](#3-tunnels)
4. [Railroad Crossings](#4-railroad-crossings)
5. [Ports](#5-ports)
6. [Intermodal Freight Facilities](#6-intermodal-freight-facilities)
7. [Cellular Towers](#7-cellular-towers)
8. [Broadcast Towers](#8-broadcast-towers)
9. [FM and AM Transmitters](#9-fm-and-am-transmitters)
10. [Analysis Recipes](#10-analysis-recipes)

---

## 1. Airports

**~19,000+ airports and landing facilities** — commercial, general aviation, military, private.

### Feature Service

```
Service: Airports
Geometry: Point
Records: ~19,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `FAC_NAME` | STRING | Facility name |
| `FAC_TYPE` | STRING | AIRPORT, HELIPORT, SEAPLANE BASE, ULTRALIGHT, GLIDERPORT |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `OWNER_TYPE` | STRING | PU (public), PR (private), MA (Air Force), MN (Navy), etc. |
| `USE` | STRING | PU (public use), PR (private use) |
| `LOC_ID` | STRING | FAA location identifier (e.g., DEN, DIA) |
| `ELEV` | DOUBLE | Elevation (feet MSL) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Public-use airports by type in Colorado
SELECT FAC_TYPE, COUNT(*) AS cnt
FROM infrastructure.hifld.airports
WHERE STATE = 'CO' AND USE = 'PU'
GROUP BY FAC_TYPE
ORDER BY cnt DESC;

-- Highest elevation airports
SELECT FAC_NAME, CITY, ELEV, FAC_TYPE, LOC_ID
FROM infrastructure.hifld.airports
WHERE STATE = 'CO'
ORDER BY ELEV DESC
LIMIT 10;
```

---

## 2. Bridges

**~600,000+ bridges** — highway, railroad, and pedestrian bridges from the National Bridge Inventory.

### Feature Service

```
Service: Bridges
Geometry: Point
Records: ~615,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `FACILITY_CARRIED` | STRING | Road/feature carried by bridge |
| `FEATURE_INTERSECTED` | STRING | Feature crossed (river, highway, etc.) |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `YEAR_BUILT` | INT | Year built |
| `DECK_COND` | STRING | Deck condition rating (0-9, N) |
| `SUPERSTRUCTURE_COND` | STRING | Superstructure condition rating |
| `SUBSTRUCTURE_COND` | STRING | Substructure condition rating |
| `STRUCT_EVAL` | STRING | Structural evaluation rating |
| `TRAFFIC` | INT | Average daily traffic |
| `DECK_WIDTH` | DOUBLE | Deck width (meters) |
| `STRUCTURE_LEN` | DOUBLE | Structure length (meters) |
| `OWNER` | STRING | Owner code |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

> **Condition ratings:** 9 = Excellent, 7 = Good, 4 = Poor, 0-3 = Critical. Ratings ≤ 4 indicate structurally deficient.

### Example Queries

```sql
-- Bridges by condition in Colorado
SELECT
    CASE
        WHEN CAST(DECK_COND AS INT) >= 7 THEN 'Good'
        WHEN CAST(DECK_COND AS INT) >= 5 THEN 'Fair'
        WHEN CAST(DECK_COND AS INT) >= 1 THEN 'Poor/Critical'
        ELSE 'Not Rated'
    END AS condition_category,
    COUNT(*) AS bridge_count,
    ROUND(AVG(TRAFFIC), 0) AS avg_daily_traffic
FROM infrastructure.hifld.bridges
WHERE STATE = 'CO'
GROUP BY 1
ORDER BY bridge_count DESC;

-- Oldest bridges still in service
SELECT FACILITY_CARRIED, FEATURE_INTERSECTED, CITY, YEAR_BUILT, TRAFFIC,
       DECK_COND, SUPERSTRUCTURE_COND
FROM infrastructure.hifld.bridges
WHERE STATE = 'CO' AND YEAR_BUILT > 0
ORDER BY YEAR_BUILT
LIMIT 20;

-- High-traffic bridges with poor condition
SELECT FACILITY_CARRIED, FEATURE_INTERSECTED, CITY, YEAR_BUILT,
       TRAFFIC, DECK_COND, SUPERSTRUCTURE_COND
FROM infrastructure.hifld.bridges
WHERE STATE = 'CO'
  AND TRAFFIC > 10000
  AND CAST(DECK_COND AS INT) <= 4
ORDER BY TRAFFIC DESC;
```

---

## 3. Tunnels

**~700+ tunnels** — highway and railroad tunnels.

### Feature Service

```
Service: Tunnels
Geometry: Point
Records: ~700+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Tunnel name |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | Type (HIGHWAY, RAILROAD) |
| `LENGTH_FT` | DOUBLE | Length in feet |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 4. Railroad Crossings

**~250,000+ railroad crossings** — at-grade, overpass, and underpass crossings.

### Feature Service

```
Service: Railroad_Crossings
Geometry: Point
Records: ~250,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `CROSSING` | STRING | Crossing ID |
| `RAILROAD` | STRING | Railroad company |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | PUBLIC, PRIVATE |
| `POSXING` | STRING | Position type (AT GRADE, ABOVE GRADE, BELOW GRADE) |
| `APTS` | INT | Average daily traffic |
| `TRAINS` | INT | Average daily trains |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 5. Ports

**~4,500+ port facilities** — maritime ports, inland waterway ports, Great Lakes ports.

### Feature Service

```
Service: Major_Ports
Geometry: Point
Records: ~4,500+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `PORT_NAME` | STRING | Port name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `NAV_UNIT_NAME` | STRING | Navigation unit |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 6. Intermodal Freight Facilities

**~1,300+ intermodal facilities** — terminals where freight transfers between transportation modes.

### Feature Service

```
Service: Intermodal_Freight_Facilities
Geometry: Point
Records: ~1,300+
```

---

## 7. Cellular Towers

**~200,000+ cell towers** — wireless communication towers with carrier and technology data.

### Feature Service

```
Service: Cellular_Towers
Geometry: Point
Records: ~200,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Tower name/ID |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `COUNTY` | STRING | County |
| `OWNER` | STRING | Tower owner |
| `STATUS` | STRING | Status |
| `STRUCTHGT` | DOUBLE | Structure height (feet) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Cell tower density by county
SELECT COUNTY, COUNT(*) AS tower_count
FROM infrastructure.hifld.cellular_towers
WHERE STATE = 'CO'
GROUP BY COUNTY
ORDER BY tower_count DESC;

-- Coverage gaps: H3 hexes with schools but no cell towers
WITH school_hexes AS (
    SELECT DISTINCT H3_LONGLATASH3STRING(_longitude, _latitude, 7) AS h3_cell
    FROM infrastructure.hifld.public_schools WHERE STATE = 'CO'
),
tower_hexes AS (
    SELECT DISTINCT H3_LONGLATASH3STRING(_longitude, _latitude, 7) AS h3_cell
    FROM infrastructure.hifld.cellular_towers WHERE STATE = 'CO'
)
SELECT s.h3_cell, H3_CENTERASWKT(s.h3_cell) AS center
FROM school_hexes s
LEFT JOIN tower_hexes t ON s.h3_cell = t.h3_cell
WHERE t.h3_cell IS NULL;
```

---

## 8. Broadcast Towers

**~130,000+ broadcast towers** — TV, radio, and land mobile transmission towers.

### Feature Service

```
Service: Land_Mobile_Broadcast_Towers_LMBT
Geometry: Point
Records: ~130,000+
```

---

## 9. FM and AM Transmitters

**~30,000+ FM transmitters** and **~5,000+ AM transmitters** — licensed broadcast radio.

### Feature Services

```
Service: FM_Transmission_Towers
Geometry: Point
Records: ~30,000+

Service: AM_Transmission_Towers
Geometry: Point
Records: ~5,000+
```

### Key Fields (FM)

| Field | Type | Description |
|-------|------|-------------|
| `CALLSIGN` | STRING | Station call sign (e.g., KBCO) |
| `FREQUENCY` | DOUBLE | Broadcast frequency (MHz) |
| `CITY` | STRING | City of license |
| `STATE` | STRING | State |
| `POWER_KW` | DOUBLE | Effective radiated power (kW) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 10. Analysis Recipes

### Bridge Infrastructure Risk Score

```sql
-- Composite risk score combining age, condition, and traffic
SELECT FACILITY_CARRIED, FEATURE_INTERSECTED, CITY, YEAR_BUILT, TRAFFIC,
       DECK_COND, SUPERSTRUCTURE_COND,
       (
           (2026 - YEAR_BUILT) * 0.3 +                            -- age factor
           (10 - CAST(DECK_COND AS INT)) * 20 +                   -- deck condition
           (10 - CAST(SUPERSTRUCTURE_COND AS INT)) * 20 +         -- superstructure
           LEAST(TRAFFIC / 1000.0, 50) * 0.5                      -- traffic exposure
       ) AS risk_score
FROM infrastructure.hifld.bridges
WHERE STATE = 'CO' AND YEAR_BUILT > 0
  AND DECK_COND RLIKE '^[0-9]$'
  AND SUPERSTRUCTURE_COND RLIKE '^[0-9]$'
ORDER BY risk_score DESC
LIMIT 25;
```

### Cell Tower Coverage vs Population Density

```sql
-- Compare cell tower density to school enrollment (population proxy)
WITH hex_towers AS (
    SELECT H3_LONGLATASH3STRING(_longitude, _latitude, 6) AS h3_cell,
           COUNT(*) AS towers
    FROM infrastructure.hifld.cellular_towers WHERE STATE = 'CO'
    GROUP BY h3_cell
),
hex_schools AS (
    SELECT H3_LONGLATASH3STRING(_longitude, _latitude, 6) AS h3_cell,
           SUM(ENROLLMENT) AS students
    FROM infrastructure.hifld.public_schools WHERE STATE = 'CO' AND STATUS = '1'
    GROUP BY h3_cell
)
SELECT COALESCE(t.h3_cell, s.h3_cell) AS h3_cell,
       COALESCE(t.towers, 0) AS towers,
       COALESCE(s.students, 0) AS students,
       CASE
           WHEN s.students > 5000 AND COALESCE(t.towers, 0) = 0 THEN 'CRITICAL GAP'
           WHEN s.students > 1000 AND COALESCE(t.towers, 0) < 2 THEN 'UNDERSERVED'
           ELSE 'ADEQUATE'
       END AS coverage_status
FROM hex_towers t
FULL OUTER JOIN hex_schools s ON t.h3_cell = s.h3_cell
WHERE COALESCE(s.students, 0) > 0
ORDER BY students DESC;
```

---

## Next Steps

- Water & government facilities → [6-water-government.md](6-water-government.md)
- Cross-sector analysis → [7-analysis-recipes.md](7-analysis-recipes.md)
