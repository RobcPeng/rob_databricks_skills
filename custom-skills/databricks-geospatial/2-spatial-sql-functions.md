# Spatial SQL Functions Reference

Complete reference for Databricks ST_ geospatial functions (DBR 17.1+) organized by category with practical examples.

## Table of Contents

1. [Constructor Functions](#1-constructor-functions)
2. [Measurement Functions](#2-measurement-functions)
3. [Relationship Functions (Predicates)](#3-relationship-functions-predicates)
4. [Overlay Functions](#4-overlay-functions)
5. [Accessor Functions](#5-accessor-functions)
6. [Transformation Functions](#6-transformation-functions)
7. [Processing Functions](#7-processing-functions)
8. [Validation Functions](#8-validation-functions)
9. [Aggregate Functions](#9-aggregate-functions)
10. [Common Spatial Query Patterns](#10-common-spatial-query-patterns)

---

## 1. Constructor Functions

Build spatial objects from coordinates, text, or binary data.

### Points

```sql
-- From longitude, latitude (NOTE: lon first, lat second)
SELECT ST_POINT(-122.4194, 37.7749) AS sf_point;

-- With explicit SRID
SELECT ST_POINT(-122.4194, 37.7749, 4326) AS sf_point_srid;
```

### Lines

```sql
-- From an array of points
SELECT ST_MAKELINE(ARRAY(
  ST_POINT(-122.4, 37.7),
  ST_POINT(-122.5, 37.8),
  ST_POINT(-122.3, 37.9)
)) AS route;
```

### Polygons

```sql
-- From a linestring ring (must be closed -- first point = last point)
SELECT ST_MAKEPOLYGON(
  ST_GEOMFROMTEXT('LINESTRING(-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7)')
) AS area;

-- With a hole (inner ring)
SELECT ST_MAKEPOLYGON(
  ST_GEOMFROMTEXT('LINESTRING(-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7)'),
  ARRAY(ST_GEOMFROMTEXT('LINESTRING(-122.48 37.72, -122.42 37.72, -122.42 37.78, -122.48 37.78, -122.48 37.72)'))
) AS area_with_hole;
```

### From Text and Binary Formats

```sql
-- WKT
SELECT ST_GEOGFROMTEXT('POINT(-122.4 37.8)');
SELECT ST_GEOMFROMTEXT('POINT(-122.4 37.8)', 4326);

-- WKB
SELECT ST_GEOGFROMWKB(geometry_binary_col);

-- GeoJSON
SELECT ST_GEOGFROMGEOJSON('{"type":"Point","coordinates":[-122.4,37.8]}');

-- EWKT (WKT with embedded SRID)
SELECT ST_GEOMFROMEWKT('SRID=4326;POINT(-122.4 37.8)');

-- Geohash
SELECT ST_GEOMFROMGEOHASH('9q8yyk9');

-- Generic converter (auto-detects format)
SELECT TO_GEOGRAPHY('POINT(-122.4 37.8)');  -- WKT, WKB, or GeoJSON
SELECT TRY_TO_GEOGRAPHY(maybe_invalid_col); -- returns NULL on error
```

---

## 2. Measurement Functions

Calculate distances, areas, lengths, and bearings.

### Distance

```sql
-- Spherical distance in meters (GEOGRAPHY)
SELECT ST_DISTANCESPHERE(
  ST_POINT(-122.4194, 37.7749),  -- San Francisco
  ST_POINT(-118.2437, 34.0522)   -- Los Angeles
) AS distance_meters;
-- Returns: ~559,120 meters

-- Geodesic distance on WGS84 spheroid (most accurate)
SELECT ST_DISTANCESPHEROID(
  ST_POINT(-122.4194, 37.7749),
  ST_POINT(-118.2437, 34.0522)
) AS distance_meters;

-- Cartesian distance (GEOMETRY, result in SRID units)
SELECT ST_DISTANCE(
  ST_GEOMFROMTEXT('POINT(500000 4000000)', 32610),
  ST_GEOMFROMTEXT('POINT(510000 4010000)', 32610)
) AS distance_meters;

-- Distance threshold check (true/false)
SELECT ST_DWITHIN(
  ST_POINT(-122.4, 37.8)::GEOGRAPHY,
  ST_POINT(-122.5, 37.7)::GEOGRAPHY,
  5000  -- within 5km?
) AS within_5km;
```

### Area and Length

```sql
-- Area of a polygon (GEOGRAPHY returns square meters)
SELECT ST_AREA(
  ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))')
) AS area_sqm;

-- Length of a linestring (GEOGRAPHY returns meters)
SELECT ST_LENGTH(
  ST_GEOGFROMTEXT('LINESTRING(-122.4 37.7, -122.5 37.8, -122.3 37.9)')
) AS length_meters;

-- Perimeter of a polygon
SELECT ST_PERIMETER(
  ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))')
) AS perimeter_meters;
```

### Bearing and Direction

```sql
-- Azimuth (bearing) from north in radians
SELECT ST_AZIMUTH(
  ST_POINT(-122.4194, 37.7749),  -- from
  ST_POINT(-118.2437, 34.0522)   -- to
) AS bearing_radians;

-- Convert to degrees
SELECT DEGREES(ST_AZIMUTH(
  ST_POINT(-122.4194, 37.7749),
  ST_POINT(-118.2437, 34.0522)
)) AS bearing_degrees;
```

### Closest Point

```sql
-- Find the nearest point on a line to a given point
SELECT ST_CLOSESTPOINT(
  ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10)'),
  ST_GEOMFROMTEXT('POINT(5 0)')
) AS nearest_point;
```

---

## 3. Relationship Functions (Predicates)

Test spatial relationships between geometries. Return BOOLEAN.

```
┌─────────────────────────────────────────────────────────┐
│  Spatial Relationship Hierarchy                         │
│                                                         │
│  ST_EQUALS     ── shapes are identical                  │
│  ST_CONTAINS   ── A fully contains B (not touching)     │
│  ST_COVERS     ── A fully contains B (may touch edge)   │
│  ST_WITHIN     ── A is fully inside B                   │
│  ST_INTERSECTS ── A and B share any space               │
│  ST_TOUCHES    ── A and B share boundary only           │
│  ST_DISJOINT   ── A and B share nothing                 │
│  ST_DWITHIN    ── A and B within distance threshold     │
└─────────────────────────────────────────────────────────┘
```

### Examples

```sql
-- Does a polygon contain a point?
SELECT ST_CONTAINS(
  ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
  ST_POINT(-122.45, 37.75)
) AS point_in_polygon;
-- Returns: true

-- Do two polygons overlap?
SELECT ST_INTERSECTS(
  ST_GEOGFROMTEXT('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
  ST_GEOGFROMTEXT('POLYGON((-122.45 37.75, -122.35 37.75, -122.35 37.85, -122.45 37.85, -122.45 37.75))')
) AS overlaps;
-- Returns: true

-- Are two geometries completely separate?
SELECT ST_DISJOINT(
  ST_POINT(-122.4, 37.8),
  ST_POINT(-73.9, 40.7)
) AS no_overlap;
-- Returns: true

-- Is point A within 1km of point B?
SELECT ST_DWITHIN(
  ST_POINT(-122.4, 37.8)::GEOGRAPHY,
  ST_POINT(-122.41, 37.81)::GEOGRAPHY,
  1000
) AS within_1km;
```

### Spatial Join Pattern

```sql
-- Find all stores within each sales territory
SELECT t.territory_name, s.store_name
FROM territories t
JOIN stores s
  ON ST_CONTAINS(
    ST_GEOGFROMWKB(t.geometry),
    ST_POINT(s.longitude, s.latitude)
  );
```

---

## 4. Overlay Functions

Compute new geometries from combinations of inputs.

```sql
-- Intersection: shared area between two polygons
SELECT ST_INTERSECTION(polygon_a, polygon_b) AS shared_area;

-- Difference: area in A but not in B
SELECT ST_DIFFERENCE(polygon_a, polygon_b) AS a_minus_b;

-- Union: merge two geometries
SELECT ST_UNION(polygon_a, polygon_b) AS combined;

-- Aggregate union: merge all geometries in a group
SELECT region,
       ST_UNION_AGG(ST_GEOGFROMWKB(geometry)) AS merged_boundary
FROM parcels
GROUP BY region;
```

---

## 5. Accessor Functions

Extract information from spatial objects.

```sql
-- Coordinate extraction
SELECT ST_X(point_geom) AS longitude,
       ST_Y(point_geom) AS latitude,
       ST_Z(point_geom) AS elevation;

-- Bounding box
SELECT ST_XMIN(geom) AS min_lon, ST_XMAX(geom) AS max_lon,
       ST_YMIN(geom) AS min_lat, ST_YMAX(geom) AS max_lat;

-- Geometry info
SELECT ST_GEOMETRYTYPE(geom)    AS type,      -- 'ST_Point', 'ST_Polygon', etc.
       ST_DIMENSION(geom)       AS dimension,  -- 0=point, 1=line, 2=polygon
       ST_NPOINTS(geom)         AS num_points,
       ST_NUMGEOMETRIES(geom)   AS num_parts;  -- for multi-geometries

-- Extract components
SELECT ST_CENTROID(polygon_geom)     AS center,
       ST_ENVELOPE(polygon_geom)     AS bounding_rect,
       ST_EXTERIORRING(polygon_geom) AS outer_ring,
       ST_STARTPOINT(line_geom)      AS first_point,
       ST_ENDPOINT(line_geom)        AS last_point;

-- Aggregate bounding box
SELECT ST_ENVELOPE_AGG(ST_GEOGFROMWKB(geometry)) AS total_extent
FROM spatial_table;
```

---

## 6. Transformation Functions

Modify geometry coordinates.

```sql
-- Reproject between coordinate systems
SELECT ST_TRANSFORM(
  ST_GEOMFROMTEXT('POINT(-122.4194 37.7749)', 4326),
  32610  -- to UTM Zone 10N
) AS projected;

-- Flip X/Y coordinates (common fix for lat/lon swap)
SELECT ST_FLIPCOORDINATES(geom) AS fixed;

-- Flatten 3D to 2D
SELECT ST_FORCE2D(geom_3d) AS geom_2d;

-- Rotate, scale, translate
SELECT ST_ROTATE(geom, RADIANS(45))              AS rotated,
       ST_SCALE(geom, 2.0, 2.0)                  AS scaled,
       ST_TRANSLATE(geom, 100.0, 200.0)           AS shifted;

-- Convert to multi-type
SELECT ST_MULTI(single_polygon) AS multi_polygon;
```

---

## 7. Processing Functions

Derive new geometries through spatial operations.

```sql
-- Buffer: expand a point to a circle (radius in degrees for GEOMETRY, meters for GEOGRAPHY)
SELECT ST_BUFFER(ST_POINT(-122.4, 37.8)::GEOMETRY, 0.01) AS buffered_circle;

-- Centroid: geometric center
SELECT ST_CENTROID(polygon_geom) AS center;

-- Boundary: extract the edge
SELECT ST_BOUNDARY(polygon_geom) AS edge;

-- Simplify: reduce vertices (Douglas-Peucker algorithm)
SELECT ST_SIMPLIFY(complex_polygon, 0.001) AS simplified;

-- Convex hull: smallest convex shape enclosing all points
SELECT ST_CONVEXHULL(
  ST_GEOMFROMTEXT('MULTIPOINT((0 0), (1 0), (0.5 1), (0.5 0.5))')
) AS hull;

-- Concave hull: tighter-fitting boundary
SELECT ST_CONCAVEHULL(multi_point_geom, 0.5) AS tight_hull;
```

---

## 8. Validation Functions

```sql
-- Check if geometry is valid (OGC compliant)
SELECT ST_ISVALID(geom) AS is_valid;

-- Check if geometry is empty
SELECT ST_ISEMPTY(geom) AS is_empty;

-- Estimate best SRID for projection
SELECT ST_ESTIMATESRID(geom) AS recommended_srid;

-- Get/set SRID
SELECT ST_SRID(geom) AS current_srid;
SELECT ST_SETSRID(geom, 4326) AS with_srid;
```

---

## 9. Aggregate Functions

```sql
-- Merge all geometries into one
SELECT ST_UNION_AGG(ST_GEOGFROMWKB(geometry)) AS merged
FROM parcels
WHERE city = 'San Francisco';

-- Bounding box of all features
SELECT ST_ENVELOPE_AGG(ST_GEOGFROMWKB(geometry)) AS total_extent
FROM buildings;
```

---

## 10. Common Spatial Query Patterns

### Point-in-Polygon Assignment

```sql
-- Assign each store to a sales territory
SELECT s.store_id, s.store_name, t.territory_name
FROM stores s
JOIN territories t
  ON ST_CONTAINS(
    ST_GEOGFROMWKB(t.boundary),
    ST_POINT(s.lon, s.lat)
  );
```

### Nearest Neighbor (using distance + QUALIFY)

```sql
-- Find the closest hospital to each school
SELECT school_name, hospital_name, distance_m
FROM (
  SELECT s.name AS school_name,
         h.name AS hospital_name,
         ST_DISTANCESPHERE(ST_POINT(s.lon, s.lat), ST_POINT(h.lon, h.lat)) AS distance_m,
         ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY
           ST_DISTANCESPHERE(ST_POINT(s.lon, s.lat), ST_POINT(h.lon, h.lat))
         ) AS rn
  FROM schools s
  CROSS JOIN hospitals h
)
WHERE rn = 1;
```

### Buffer and Intersect

```sql
-- Find all restaurants within 500m of each transit stop
SELECT t.stop_name, r.restaurant_name
FROM transit_stops t
JOIN restaurants r
  ON ST_DWITHIN(
    ST_POINT(t.lon, t.lat)::GEOGRAPHY,
    ST_POINT(r.lon, r.lat)::GEOGRAPHY,
    500  -- meters
  );
```

### Spatial Aggregation

```sql
-- Count buildings per neighborhood
SELECT n.name AS neighborhood,
       COUNT(*) AS building_count,
       SUM(ST_AREA(ST_GEOGFROMWKB(b.geometry))) AS total_area_sqm
FROM neighborhoods n
JOIN buildings b
  ON ST_CONTAINS(
    ST_GEOGFROMWKB(n.boundary),
    ST_CENTROID(ST_GEOGFROMWKB(b.geometry))
  )
GROUP BY n.name
ORDER BY building_count DESC;
```
