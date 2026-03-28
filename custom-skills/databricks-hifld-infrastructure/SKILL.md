---
name: databricks-hifld-infrastructure
description: >
  Use when working with HIFLD (Homeland Infrastructure Foundation-Level Data) on Databricks —
  public infrastructure datasets from DHS/CISA covering energy, health, education, emergency
  services, transportation, communications, water, and government facilities. Triggers on:
  'HIFLD', 'infrastructure data', 'substations', 'transmission lines', 'power plants',
  'hospitals', 'fire stations', 'EMS', 'schools', 'cell towers', 'dams', 'airports',
  'bridges', 'nursing homes', 'law enforcement', 'emergency services', 'critical infrastructure',
  'public safety', 'SLED data', 'government facilities', 'service territories', 'grid data',
  'energy infrastructure', 'ArcGIS Feature Service', 'HIFLD open data', 'GeoJSON infrastructure',
  'infrastructure analysis', 'coverage gaps', 'proximity analysis', 'vulnerability assessment'.
---

# HIFLD Infrastructure Data on Databricks

A comprehensive guide to loading, querying, and analyzing Homeland Infrastructure Foundation-Level Data (HIFLD) on Databricks — 300+ public datasets covering critical infrastructure across the United States.

## What is HIFLD?

HIFLD (Homeland Infrastructure Foundation-Level Data) is a Department of Homeland Security (DHS/CISA) initiative that publishes geospatial datasets for critical infrastructure across the US. All data is **public, free, and updated regularly**.

- **Source:** https://hifld-geoplatform.opendata.arcgis.com
- **Format:** GeoJSON, CSV, Shapefile, KML (via ArcGIS Hub)
- **License:** Public domain (US Government Work)
- **Coverage:** National (all 50 states + territories)

## How to Use This Skill

**Loading any HIFLD dataset:** Start with [1-data-access-patterns.md](1-data-access-patterns.md) — covers download, REST API, and loading into Delta tables.

**Looking for a specific sector:**

| Sector | File |
|--------|------|
| Energy (substations, transmission, plants) | [2-energy-infrastructure.md](2-energy-infrastructure.md) |
| Emergency services (fire, EMS, law enforcement) | [3-emergency-services.md](3-emergency-services.md) |
| Health & Education (hospitals, schools) | [4-health-education.md](4-health-education.md) |
| Transportation & Communications | [5-transportation-communications.md](5-transportation-communications.md) |
| Water & Government facilities | [6-water-government.md](6-water-government.md) |

**Cross-sector analysis recipes:** [7-analysis-recipes.md](7-analysis-recipes.md)

## Routing Table

| User intent | Start here |
|-------------|------------|
| "Load HIFLD data into Databricks" | [1-data-access-patterns.md](1-data-access-patterns.md) |
| "Substations / power grid / energy" | [2-energy-infrastructure.md](2-energy-infrastructure.md) |
| "Fire stations / 911 / EMS / police" | [3-emergency-services.md](3-emergency-services.md) |
| "Hospitals / schools / colleges" | [4-health-education.md](4-health-education.md) |
| "Airports / bridges / cell towers" | [5-transportation-communications.md](5-transportation-communications.md) |
| "Dams / water treatment / government" | [6-water-government.md](6-water-government.md) |
| "Coverage gap analysis" | [7-analysis-recipes.md](7-analysis-recipes.md) |
| "Infrastructure proximity / vulnerability" | [7-analysis-recipes.md](7-analysis-recipes.md) |
| "Colorado / Rocky Mountain region" | [7-analysis-recipes.md](7-analysis-recipes.md) (includes regional filters) |

## HIFLD Sector Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│  HIFLD Critical Infrastructure Sectors                                    │
│                                                                            │
│  ENERGY               │  EMERGENCY SERVICES    │  HEALTH & EDUCATION      │
│  ├─ Substations       │  ├─ Fire Stations      │  ├─ Hospitals            │
│  ├─ Transmission Lines │  ├─ EMS Stations       │  ├─ Nursing Homes       │
│  ├─ Power Plants      │  ├─ Law Enforcement    │  ├─ Pharmacies          │
│  └─ Service Territories│  ├─ 911 Call Centers   │  ├─ Public Schools      │
│                        │  └─ Emergency Ops Ctrs │  ├─ Private Schools     │
│  TRANSPORTATION       │                         │  └─ Colleges/Univ.     │
│  ├─ Airports          │  COMMUNICATIONS         │                         │
│  ├─ Bridges           │  ├─ Cell Towers         │  WATER & GOVERNMENT    │
│  ├─ Tunnels           │  ├─ FM Transmitters     │  ├─ Dams               │
│  ├─ Railroad Crossings │  ├─ AM Transmitters     │  ├─ Water Treatment    │
│  ├─ Ports             │  ├─ Broadcast Towers    │  ├─ Wastewater         │
│  └─ Intermodal Freight │  └─ Microwave Towers   │  ├─ Federal Buildings  │
│                        │                         │  └─ State Capitol Bldgs│
└──────────────────────────────────────────────────────────────────────────┘
```

## MCP Tool Mapping

| Action | MCP tool |
|--------|----------|
| Load GeoJSON/CSV from Volume | `execute_sql` or `run_python_file_on_databricks` |
| Upload downloaded files | `upload_to_volume` |
| Query loaded tables | `execute_sql` |
| Spatial analysis | `execute_sql` (with ST_ functions) |

## Related Skills

- **[databricks-geospatial](../databricks-geospatial/SKILL.md)** — ST_ functions, H3, spatial joins, Overture Maps
- **[databricks-governance](../databricks-governance/SKILL.md)** — tagging infrastructure data, access control
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — volume operations for file uploads
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** — dashboard infrastructure data
