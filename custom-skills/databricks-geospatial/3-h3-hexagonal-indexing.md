# H3 Hexagonal Indexing on Databricks

H3 is Uber's hexagonal hierarchical spatial index. Databricks provides native H3 functions (DBR 11.2+) for efficient spatial aggregation, joins, and analysis.

## Table of Contents

1. [Why H3?](#1-why-h3)
2. [Resolution Selection](#2-resolution-selection)
3. [Core Functions](#3-core-functions)
4. [Spatial Aggregation with H3](#4-spatial-aggregation-with-h3)
5. [H3-Based Spatial Joins](#5-h3-based-spatial-joins)
6. [Tessellation and Coverage](#6-tessellation-and-coverage)
7. [Hierarchical Operations](#7-hierarchical-operations)
8. [Practical Patterns](#8-practical-patterns)

---

## 1. Why H3?

```
┌──────────────────────────────────────────────────────────────┐
│  Square Grids              vs            Hexagonal Grids     │
│                                                              │
│  ┌──┬──┬──┐                     ╱╲    ╱╲    ╱╲              │
│  │  │  │  │                    ╱  ╲  ╱  ╲  ╱  ╲             │
│  ├──┼──┼──┤                   ╱    ╲╱    ╲╱    ╲            │
│  │  │  │  │                   ╲    ╱╲    ╱╲    ╱            │
│  ├──┼──┼──┤                    ╲  ╱  ╲  ╱  ╲  ╱             │
│  │  │  │  │                     ╲╱    ╲╱    ╲╱              │
│  └──┴──┴──┘                                                  │
│  - 2 neighbor distances        - All neighbors equidistant   │
│  - Edge distortion             - Uniform coverage            │
│  - Poor for circular queries   - Natural for radius queries  │
└──────────────────────────────────────────────────────────────┘
```

**Advantages of H3:**
- Every cell has exactly 6 neighbors at equal distance
- Hierarchical: each cell contains ~7 children at the next resolution
- Efficient spatial joins: index both sides, then join on cell ID (equality join)
- Consistent area at each resolution (minimal distortion)

---

## 2. Resolution Selection

| Resolution | Avg Edge Length | Avg Cell Area | Use Case |
|-----------|----------------|---------------|----------|
| 0 | 1,107 km | 4,357,449 km² | Continental / global |
| 1 | 418 km | 609,789 km² | Country-level |
| 2 | 158 km | 86,802 km² | State/province |
| 3 | 59.8 km | 12,393 km² | Metro region |
| 4 | 22.6 km | 1,770 km² | Large city |
| 5 | 8.5 km | 252.9 km² | City district |
| 6 | 3.2 km | 36.13 km² | Neighborhood |
| 7 | 1.22 km | 5.161 km² | Large blocks |
| 8 | 461 m | 0.737 km² | City blocks |
| 9 | 174 m | 0.105 km² | Individual buildings |
| 10 | 65.9 m | 15,047 m² | Parcels / lots |
| 11 | 24.9 m | 2,149 m² | Sub-parcel |
| 12 | 9.4 m | 307 m² | Room-level |
| 13 | 3.6 m | 43.9 m² | Meter precision |
| 14 | 1.3 m | 6.27 m² | Sub-meter |
| 15 | 0.5 m | 0.90 m² | Centimeter precision |

**Rules of thumb:**
- **City-level analytics (heatmaps, demand):** Resolution 7-9
- **Ride-sharing / delivery zones:** Resolution 8-9
- **Neighborhood aggregation:** Resolution 6-7
- **Regional planning:** Resolution 4-5
- **Point-of-sale / store proximity:** Resolution 9-10

---

## 3. Core Functions

### Converting Coordinates to H3

```sql
-- From longitude, latitude (returns BIGINT)
SELECT H3_LONGLATASH3(-122.4194, 37.7749, 9) AS h3_cell;
-- Returns: 617700169958293503

-- From longitude, latitude (returns STRING hex)
SELECT H3_LONGLATASH3STRING(-122.4194, 37.7749, 9) AS h3_cell;
-- Returns: '8928308280fffff'

-- From a GEOGRAPHY point
SELECT H3_POINTASH3(ST_POINT(-122.4194, 37.7749), 9) AS h3_cell;
SELECT H3_POINTASH3STRING(ST_POINT(-122.4194, 37.7749), 9) AS h3_cell;
```

### Converting H3 Back to Geometry

> **⚠️ `H3_CENTERASLAT` / `H3_CENTERASLNG` do not exist in Databricks SQL.** These are `h3-py` Python library functions, not Databricks built-in functions. To extract lat/lon from an H3 cell, use `H3_CENTERASGEOJSON` and parse with `GET_JSON_OBJECT`. Note: GeoJSON coordinate order is `[lon, lat]`, so `coordinates[0]` = longitude, `coordinates[1]` = latitude:
> ```sql
> SELECT
>   GET_JSON_OBJECT(H3_CENTERASGEOJSON(h3_cell), '$.coordinates[1]') AS lat,
>   GET_JSON_OBJECT(H3_CENTERASGEOJSON(h3_cell), '$.coordinates[0]') AS lon
> FROM my_table;
> ```

```sql
-- Cell center as WKT
SELECT H3_CENTERASWKT('8928308280fffff') AS center;
-- Returns: 'POINT(-122.41905246614842 37.77519674498076)'

-- Cell boundary as WKT polygon
SELECT H3_BOUNDARYASWKT('8928308280fffff') AS boundary;

-- As GeoJSON (useful for visualization)
SELECT H3_BOUNDARYASGEOJSON('8928308280fffff') AS boundary_geojson;
SELECT H3_CENTERASGEOJSON('8928308280fffff') AS center_geojson;

-- As WKB (for spatial operations)
SELECT H3_BOUNDARYASWKB('8928308280fffff') AS boundary_wkb;
SELECT H3_CENTERASWKB('8928308280fffff') AS center_wkb;
```

### Format Conversion

```sql
-- BIGINT <-> STRING
SELECT H3_H3TOSTRING(617700169958293503) AS hex_string;
-- Returns: '8928308280fffff'

SELECT H3_STRINGTOH3('8928308280fffff') AS bigint_id;
-- Returns: 617700169958293503
```

### Validation

```sql
-- Check if a value is a valid H3 cell ID
SELECT H3_ISVALID('8928308280fffff') AS valid;     -- true
SELECT H3_ISVALID('invalid') AS valid;              -- false
SELECT H3_ISVALID(617700169958293503) AS valid;     -- true

-- Validate (returns input or errors)
SELECT H3_VALIDATE('8928308280fffff');               -- returns the value
SELECT H3_TRY_VALIDATE('invalid');                   -- returns NULL

-- Check if cell is a pentagon (12 pentagons per resolution)
SELECT H3_ISPENTAGON('8928308280fffff') AS is_pentagon;
```

---

## 4. Spatial Aggregation with H3

The most common use case: aggregate point data into hexagonal cells for heatmaps and density analysis.

### Event Density Heatmap

```sql
-- Count events per H3 cell
SELECT H3_LONGLATASH3STRING(longitude, latitude, 8) AS h3_cell,
       COUNT(*) AS event_count,
       AVG(amount) AS avg_amount
FROM events
GROUP BY h3_cell
ORDER BY event_count DESC;
```

### Revenue by Hex Cell

```sql
-- Revenue heatmap at resolution 7
SELECT H3_LONGLATASH3STRING(store_lon, store_lat, 7) AS hex,
       SUM(revenue) AS total_revenue,
       COUNT(DISTINCT customer_id) AS unique_customers,
       H3_BOUNDARYASGEOJSON(H3_LONGLATASH3STRING(store_lon, store_lat, 7)) AS geojson
FROM transactions
GROUP BY hex
ORDER BY total_revenue DESC;
```

### Population Density

```sql
-- Aggregate population data into hex grid
WITH hex_pop AS (
  SELECT H3_LONGLATASH3STRING(lon, lat, 6) AS h3_cell,
         SUM(population) AS total_pop
  FROM census_blocks
  GROUP BY h3_cell
)
SELECT h3_cell,
       total_pop,
       H3_CENTERASWKT(h3_cell) AS center,
       H3_BOUNDARYASGEOJSON(h3_cell) AS boundary
FROM hex_pop
WHERE total_pop > 10000;
```

---

## 5. H3-Based Spatial Joins

H3 turns expensive spatial joins into fast equality joins.

### Traditional Spatial Join (Slow)

```sql
-- O(n*m) -- compares every pair
SELECT a.*, b.*
FROM table_a a JOIN table_b b
  ON ST_INTERSECTS(a.geometry, b.geometry);
```

### H3 Spatial Join (Fast)

```sql
-- O(n+m) -- hash join on cell ID
WITH a_hex AS (
  SELECT *, H3_LONGLATASH3STRING(lon, lat, 9) AS h3 FROM table_a
),
b_hex AS (
  SELECT *, H3_LONGLATASH3STRING(lon, lat, 9) AS h3 FROM table_b
)
SELECT a_hex.*, b_hex.*
FROM a_hex JOIN b_hex ON a_hex.h3 = b_hex.h3;
```

### Multi-Resolution Join (for fuzzy proximity)

```sql
-- Join at coarser resolution for nearby matches
WITH customers AS (
  SELECT *, H3_LONGLATASH3STRING(lon, lat, 7) AS h3_7 FROM customer_locations
),
stores AS (
  SELECT *, H3_LONGLATASH3STRING(lon, lat, 7) AS h3_7 FROM store_locations
)
SELECT c.customer_id, s.store_id, s.store_name
FROM customers c JOIN stores s ON c.h3_7 = s.h3_7;
```

---

## 6. Tessellation and Coverage

### Cover a Polygon with H3 Cells

```sql
-- Get all H3 cells that cover a polygon
SELECT EXPLODE(
  H3_COVERASH3STRING(
    ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
    8
  )
) AS h3_cell;

-- Polyfill: cells whose centers are INSIDE the polygon (stricter)
SELECT EXPLODE(
  H3_POLYFILLASH3STRING(
    ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
    8
  )
) AS h3_cell;
```

### Tessellate (with geometry fragments)

```sql
-- Returns struct with h3 cell and clipped geometry
SELECT EXPLODE(
  H3_TESSELLATEASWKB(
    ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
    8
  )
) AS tessellation;
-- Each row: { cellId: BIGINT, geometry: BINARY (WKB) }
```

---

## 7. Hierarchical Operations

### Parent/Child Navigation

```sql
-- Get parent cell at coarser resolution
SELECT H3_TOPARENT('8928308280fffff', 7) AS parent_cell;

-- Get all children at finer resolution
SELECT EXPLODE(H3_TOCHILDREN('8728308280fffff', 9)) AS child_cell;

-- Min/max child (useful for range queries)
SELECT H3_MINCHILD('8728308280fffff', 9) AS min_child,
       H3_MAXCHILD('8728308280fffff', 9) AS max_child;

-- Check parent-child relationship
SELECT H3_ISCHILDOF('8928308280fffff', '8728308280fffff') AS is_child;

-- Get resolution of a cell
SELECT H3_RESOLUTION('8928308280fffff') AS resolution;
-- Returns: 9
```

### Compaction

```sql
-- Compact: merge cells into larger parent cells where possible
SELECT H3_COMPACT(COLLECT_LIST(h3_cell)) AS compacted
FROM my_hex_cells;

-- Uncompact: expand back to a target resolution
SELECT EXPLODE(H3_UNCOMPACT(compacted_cells, 9)) AS h3_cell
FROM compacted_data;
```

---

## 8. Practical Patterns

### Neighbor Analysis (K-Ring)

```sql
-- Get all cells within distance k (inclusive)
SELECT EXPLODE(H3_KRING('8928308280fffff', 1)) AS neighbor;
-- Returns: 7 cells (center + 6 neighbors)

SELECT EXPLODE(H3_KRING('8928308280fffff', 2)) AS neighbor;
-- Returns: 19 cells (center + ring 1 + ring 2)

-- Hollow ring only (just the border)
SELECT EXPLODE(H3_HEXRING('8928308280fffff', 2)) AS ring_cell;
-- Returns: 12 cells (only ring 2)

-- K-ring with distances
SELECT INLINE(H3_KRINGDISTANCES('8928308280fffff', 2));
-- Returns: (cell_id, distance) pairs

-- Grid distance between two cells
SELECT H3_DISTANCE('8928308280fffff', '89283082873ffff') AS grid_distance;
```

### Catchment Area Analysis

```sql
-- Find all events within a 2-ring catchment of each store
WITH store_cells AS (
  SELECT store_id,
         H3_LONGLATASH3STRING(lon, lat, 8) AS store_h3
  FROM stores
),
store_catchments AS (
  SELECT store_id,
         EXPLODE(H3_KRING(store_h3, 2)) AS catchment_h3
  FROM store_cells
),
event_cells AS (
  SELECT event_id, amount,
         H3_LONGLATASH3STRING(lon, lat, 8) AS event_h3
  FROM events
)
SELECT sc.store_id,
       COUNT(*) AS events_in_catchment,
       SUM(e.amount) AS total_amount
FROM store_catchments sc
JOIN event_cells e ON sc.catchment_h3 = e.event_h3
GROUP BY sc.store_id;
```

### Multi-Resolution Drill-Down

```sql
-- Start coarse, drill into hot spots
WITH res5 AS (
  SELECT H3_LONGLATASH3STRING(lon, lat, 5) AS h3_5,
         COUNT(*) AS cnt
  FROM events GROUP BY h3_5
),
hot_zones AS (
  SELECT h3_5 FROM res5 WHERE cnt > 1000
),
res8_detail AS (
  SELECT H3_LONGLATASH3STRING(e.lon, e.lat, 8) AS h3_8,
         COUNT(*) AS cnt
  FROM events e
  WHERE H3_LONGLATASH3STRING(e.lon, e.lat, 5) IN (SELECT h3_5 FROM hot_zones)
  GROUP BY h3_8
)
SELECT h3_8, cnt,
       H3_CENTERASWKT(h3_8) AS center,
       H3_BOUNDARYASGEOJSON(h3_8) AS boundary
FROM res8_detail
ORDER BY cnt DESC
LIMIT 100;
```

### Precompute H3 Index Column

```sql
-- Add H3 index to a Delta table for fast joins
ALTER TABLE events ADD COLUMN h3_9 STRING;

UPDATE events
SET h3_9 = H3_LONGLATASH3STRING(longitude, latitude, 9);

-- Optimize the table with the H3 column for spatial locality
OPTIMIZE events ZORDER BY (h3_9);
```
