---
name: databricks-noaa-storm-events
description: "Use when working with NOAA Storm Events data on Databricks — loading severe weather CSVs, parsing event types, damage estimation, geocoding, and spatial analysis of storms."
---

# NOAA Storm Events on Databricks

Load, transform, and analyze NOAA Storm Events data — the authoritative US record of severe weather events including tornadoes, hail, floods, thunderstorm wind, winter storms, and more. Each record includes event type, location (lat/lon), damage estimates, injuries/deaths, and narrative descriptions.

## Routing Table

| Topic | File | What's Inside |
|-------|------|---------------|
| Loading & schema | [1-loading-data.md](1-loading-data.md) | Volume upload, batch read, FTP download, Auto Loader, explicit schema, free tier notes |
| Transformations & event types | [2-transformations.md](2-transformations.md) | Timestamp parsing, damage value parsing, coordinate cleaning, data quality checks, locations/fatalities files, event type reference |
| Spatial analysis | [3-spatial-analysis.md](3-spatial-analysis.md) | Point radius queries, H3 hex aggregation, tornado path lines, Overture Maps joins, trend/seasonal/window analysis, AI Functions |
| Pipeline & dashboards | [4-pipeline-and-dashboards.md](4-pipeline-and-dashboards.md) | Delta table save, partitioning, SDP Bronze/Silver/Gold pipeline, AI/BI dashboard SQL, common mistakes |

## Data Source

| Field | Details |
|-------|---------|
| **Source** | NOAA National Centers for Environmental Information (NCEI) |
| **URL** | https://www.ncdc.noaa.gov/stormevents/ftp.jsp |
| **Bulk FTP** | https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/ |
| **Format** | Gzipped CSV files (`.csv.gz`), one per year per file type |
| **Coverage** | 1950–present (updated monthly) |
| **Size** | ~50-100 MB per year (details), ~2 GB total uncompressed |
| **License** | Public domain (US government data) |

### File Types

The bulk download has three file types per year:

| File Pattern | Contents | Key Fields |
|-------------|----------|------------|
| `StormEvents_details-ftp_v1.0_d{YYYY}_c*.csv.gz` | Event details | event_type, state, begin_lat/lon, damage, injuries, deaths, narrative |
| `StormEvents_locations-ftp_v1.0_d{YYYY}_c*.csv.gz` | Location details | lat/lon per event (multiple points for paths) |
| `StormEvents_fatalities-ftp_v1.0_d{YYYY}_c*.csv.gz` | Fatality records | age, sex, location of fatality per event |

**Start with `details`** — it has everything for most analyses. Use `locations` for path-based events (tornadoes) and `fatalities` for mortality studies.

## Related Skills

- `databricks-geospatial` — Spatial SQL, H3, Overture Maps
- `databricks-free-tier-guardrails` — Serverless/free tier compatibility
- `databricks-pipeline-guardrails` — Non-equi join best practices, schema contracts
- `databricks-aibi-dashboards` — Lakeview dashboard creation
- `databricks-ai-functions` — AI Functions (ai_classify, ai_extract, ai_forecast)
