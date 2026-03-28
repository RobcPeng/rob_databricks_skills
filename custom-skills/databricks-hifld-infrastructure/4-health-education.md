# Health & Education

Hospitals, nursing homes, pharmacies, public/private schools, and colleges from HIFLD.

## Table of Contents

1. [Hospitals](#1-hospitals)
2. [Nursing Homes](#2-nursing-homes)
3. [Pharmacies](#3-pharmacies)
4. [Public Schools](#4-public-schools)
5. [Private Schools](#5-private-schools)
6. [Colleges and Universities](#6-colleges-and-universities)
7. [Health & Education Analysis Recipes](#7-health--education-analysis-recipes)

---

## 1. Hospitals

**~7,500+ hospitals** — general acute care, children's, psychiatric, rehabilitation, VA, and military hospitals.

### Feature Service

```
Service: Hospitals
Geometry: Point
Records: ~7,500+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Hospital name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | GENERAL ACUTE CARE, CRITICAL ACCESS, CHILDREN, PSYCHIATRIC, etc. |
| `STATUS` | STRING | OPEN, CLOSED |
| `OWNER` | STRING | Ownership (GOVERNMENT - FEDERAL/STATE/LOCAL, NON-PROFIT, PROPRIETARY) |
| `BEDS` | INT | Number of beds |
| `TRAUMA` | STRING | Trauma center level (LEVEL I, II, III, IV, NOT AVAILABLE) |
| `HELIPAD` | STRING | Helipad available (Y/N) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |
| `NAICS_CODE` | STRING | NAICS industry code |
| `TELEPHONE` | STRING | Phone number |
| `WEBSITE` | STRING | Website URL |

### Example Queries

```sql
-- Hospitals by type in Colorado
SELECT TYPE, COUNT(*) AS cnt, SUM(BEDS) AS total_beds
FROM infrastructure.hifld.hospitals
WHERE STATE = 'CO' AND STATUS = 'OPEN'
GROUP BY TYPE
ORDER BY total_beds DESC;

-- Trauma centers by level
SELECT TRAUMA, COUNT(*) AS cnt, SUM(BEDS) AS total_beds
FROM infrastructure.hifld.hospitals
WHERE STATE = 'CO' AND STATUS = 'OPEN' AND TRAUMA != 'NOT AVAILABLE'
GROUP BY TRAUMA
ORDER BY TRAUMA;

-- Beds per capita by county (requires population data)
SELECT COUNTY, SUM(BEDS) AS total_beds, COUNT(*) AS hospitals
FROM infrastructure.hifld.hospitals
WHERE STATE = 'CO' AND STATUS = 'OPEN'
GROUP BY COUNTY
ORDER BY total_beds DESC;

-- Hospitals with helipads
SELECT NAME, CITY, BEDS, TRAUMA, TYPE
FROM infrastructure.hifld.hospitals
WHERE STATE = 'CO' AND HELIPAD = 'Y' AND STATUS = 'OPEN'
ORDER BY BEDS DESC;
```

---

## 2. Nursing Homes

**~15,000+ nursing homes** — Medicare/Medicaid certified skilled nursing facilities.

### Feature Service

```
Service: Nursing_Homes
Geometry: Point
Records: ~15,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Facility name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | Facility type |
| `STATUS` | STRING | Status |
| `BEDS` | INT | Number of certified beds |
| `POPULATION` | INT | Resident population |
| `OWNER` | STRING | Ownership |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Nursing home capacity by county
SELECT COUNTY, COUNT(*) AS facilities, SUM(BEDS) AS total_beds
FROM infrastructure.hifld.nursing_homes
WHERE STATE = 'CO'
GROUP BY COUNTY
ORDER BY total_beds DESC;
```

---

## 3. Pharmacies

**~45,000+ pharmacies** — retail, hospital, and specialty pharmacies.

### Feature Service

```
Service: Pharmacies
Geometry: Point
Records: ~45,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Pharmacy name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | Type (RETAIL, HOSPITAL, etc.) |
| `STATUS` | STRING | Status |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 4. Public Schools

**~100,000+ public schools** — elementary, middle, high schools.

### Feature Service

```
Service: Public_Schools
Geometry: Point
Records: ~100,000+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | School name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `LEVEL_` | STRING | Grade level (ELEMENTARY, MIDDLE, HIGH, OTHER) |
| `TYPE` | STRING | School type (REGULAR, SPECIAL EDUCATION, VOCATIONAL, ALTERNATIVE) |
| `STATUS` | STRING | Status (1=Open, 2=Closed) |
| `ENROLLMENT` | INT | Student enrollment |
| `FT_TEACHER` | DOUBLE | Full-time equivalent teachers |
| `DISTRICT` | STRING | School district name |
| `DISTRICTID` | STRING | District ID (NCES) |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Schools by level in Colorado
SELECT LEVEL_, COUNT(*) AS school_count, SUM(ENROLLMENT) AS total_students
FROM infrastructure.hifld.public_schools
WHERE STATE = 'CO' AND STATUS = '1'
GROUP BY LEVEL_
ORDER BY total_students DESC;

-- Student-teacher ratio by district
SELECT DISTRICT,
       SUM(ENROLLMENT) AS students,
       ROUND(SUM(FT_TEACHER), 0) AS teachers,
       ROUND(SUM(ENROLLMENT) / NULLIF(SUM(FT_TEACHER), 0), 1) AS student_teacher_ratio
FROM infrastructure.hifld.public_schools
WHERE STATE = 'CO' AND STATUS = '1' AND ENROLLMENT > 0
GROUP BY DISTRICT
ORDER BY student_teacher_ratio DESC;

-- Largest schools
SELECT NAME, CITY, LEVEL_, ENROLLMENT, DISTRICT
FROM infrastructure.hifld.public_schools
WHERE STATE = 'CO' AND STATUS = '1'
ORDER BY ENROLLMENT DESC
LIMIT 20;
```

---

## 5. Private Schools

**~30,000+ private schools** — religious, Montessori, charter, independent.

### Feature Service

```
Service: Private_Schools
Geometry: Point
Records: ~30,000+
```

### Key Fields

Similar to public schools but with additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | School name |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `LEVEL_` | STRING | Grade level |
| `ENROLLMENT` | INT | Enrollment |
| `FT_TEACHER` | DOUBLE | Full-time equivalent teachers |
| `AFFILIATION` | STRING | Religious/organizational affiliation |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

---

## 6. Colleges and Universities

**~7,800+ institutions** — community colleges, 4-year universities, technical schools.

### Feature Service

```
Service: Colleges_and_Universities
Geometry: Point
Records: ~7,800+
```

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `NAME` | STRING | Institution name |
| `ADDRESS` | STRING | Street address |
| `CITY` | STRING | City |
| `STATE` | STRING | State |
| `ZIP` | STRING | ZIP code |
| `COUNTY` | STRING | County |
| `TYPE` | STRING | Type code |
| `NAICS_DESC` | STRING | NAICS description |
| `POPULATION` | INT | Student population |
| `WEBSITE` | STRING | Website URL |
| `LATITUDE` | DOUBLE | Latitude |
| `LONGITUDE` | DOUBLE | Longitude |

### Example Queries

```sql
-- Colleges by enrollment in Colorado
SELECT NAME, CITY, POPULATION, NAICS_DESC
FROM infrastructure.hifld.colleges_universities
WHERE STATE = 'CO' AND POPULATION > 0
ORDER BY POPULATION DESC;
```

---

## 7. Health & Education Analysis Recipes

### Healthcare Desert Analysis

```sql
-- Schools farther than 15 miles from any hospital
SELECT s.NAME AS school, s.CITY, s.ENROLLMENT,
       MIN(ROUND(ST_DISTANCESPHERE(s.geometry, h.geometry) / 1609.34, 1)) AS nearest_hospital_miles
FROM infrastructure.hifld.public_schools s
CROSS JOIN infrastructure.hifld.hospitals h
WHERE s.STATE = 'CO' AND h.STATE = 'CO'
  AND s.STATUS = '1' AND h.STATUS = 'OPEN'
GROUP BY s.NAME, s.CITY, s.ENROLLMENT
HAVING MIN(ST_DISTANCESPHERE(s.geometry, h.geometry) / 1609.34) > 15
ORDER BY nearest_hospital_miles DESC;
```

### Hospital Bed Coverage by H3 Hex

```sql
-- H3 heatmap of hospital bed capacity
SELECT H3_LONGLATASH3STRING(_longitude, _latitude, 5) AS h3_cell,
       SUM(BEDS) AS total_beds,
       COUNT(*) AS hospital_count,
       H3_BOUNDARYASGEOJSON(H3_LONGLATASH3STRING(_longitude, _latitude, 5)) AS geojson
FROM infrastructure.hifld.hospitals
WHERE STATE = 'CO' AND STATUS = 'OPEN'
GROUP BY h3_cell
ORDER BY total_beds DESC;
```

### Schools Near Hazardous Infrastructure

```sql
-- Schools within 1 mile of power plants
SELECT s.NAME AS school, s.ENROLLMENT, s.LEVEL_,
       p.NAME AS power_plant, p.PRIM_FUEL, p.TOTAL_MW,
       ROUND(ST_DISTANCESPHERE(s.geometry, p.geometry) / 1609.34, 2) AS distance_miles
FROM infrastructure.hifld.public_schools s
JOIN infrastructure.hifld.power_plants p
  ON ST_DWITHIN(s.geometry::GEOGRAPHY, p.geometry::GEOGRAPHY, 1609.34)
WHERE s.STATE = 'CO' AND p.STATE = 'CO'
  AND s.STATUS = '1' AND p.STATUS = 'OP'
ORDER BY distance_miles;
```

### Pharmacy Access by County

```sql
-- Pharmacies per 10,000 population (approximated by school enrollment as population proxy)
WITH county_pop AS (
    SELECT COUNTY, SUM(ENROLLMENT) AS approx_pop
    FROM infrastructure.hifld.public_schools
    WHERE STATE = 'CO' AND STATUS = '1'
    GROUP BY COUNTY
),
county_pharm AS (
    SELECT COUNTY, COUNT(*) AS pharmacy_count
    FROM infrastructure.hifld.pharmacies
    WHERE STATE = 'CO'
    GROUP BY COUNTY
)
SELECT p.COUNTY,
       p.pharmacy_count,
       c.approx_pop,
       ROUND(p.pharmacy_count * 10000.0 / NULLIF(c.approx_pop, 0), 1) AS pharmacies_per_10k
FROM county_pharm p
JOIN county_pop c ON p.COUNTY = c.COUNTY
ORDER BY pharmacies_per_10k;
```

---

## Next Steps

- Transportation & communications → [5-transportation-communications.md](5-transportation-communications.md)
- Cross-sector analysis → [7-analysis-recipes.md](7-analysis-recipes.md)
