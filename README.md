Under Construction -

How to Use:

- Drag these folders to "databricks-skills" (https://github.com/databricks-solutions/ai-dev-kit)
- Load the skill ("Please use Serverless Guardrails" | "Please use dbldatagen")

This works in conjunction with ai-dev-kit - recommended approach is writing to parquet for a proper bronze layer for ingestion.

For the ai-dev-kit - I've found that creating the catalog/schemas/folder structure through MCP gives more context to claude during your session.

Example Log Below:

# ai_practice_2 — Session Steps & Lessons Learned

## Steps Completed

### 1. Catalog & Schema Setup

- Created Unity Catalog `ai_practice_2` using R2 managed storage
  ```
  r2://[REDACTED].com/
  ```
- Created schemas with numeric prefixes for ordering: `00_bronze`, `01_silver`, `02_gold`
- Added `storage_root` variable to `databricks.yml`

### 2. Bundle File Structure

- Created proper folder layout for a production DAB:
  ```
  src/
    notebooks/
      setup/standup.py
      00_bronze/generate_iot_data.py
    pipelines/
      silver/silver_devices.py
      silver/silver_readings.py
      silver/silver_alerts.py
      gold/gold_device_daily_stats.py
      gold/gold_alert_daily_summary.py
    dashboards/iot_dashboard.lvdash.json
  resources/
    pipelines/pipeline.yml
    dashboards/iot_dashboard.yml
    jobs/              (deprecated main_job.yml removed)
  ```
- Added `dbldatagen>=0.4.0` to `pyproject.toml` dependencies

### 3. Serverless Compute Configuration

- Removed all `job_cluster_key` / `job_clusters` from jobs → replaced with `environment_key` + `environments.spec`
- Set `serverless: true` in pipeline resource
- Confirmed free tier SQL warehouse name: `Serverless Starter Warehouse`
- Added `warehouse_id` lookup variable pointing to correct warehouse

### 4. Volume Creation

- Created `ai_practice_2.00_bronze.pipeline_metadata` (MANAGED) for Auto Loader schema location
- Pipeline configuration references: `/Volumes/ai_practice_2/00_bronze/pipeline_metadata/schemas`

### 5. SDP Pipeline — Silver Layer

- Split by layer into separate `.py` files (one table per file) — best practice
- `silver_devices` → Materialized View (active devices only, batch)
- `silver_readings_clean` → Streaming Table (incremental, quality-filtered)
- `silver_alerts_clean` → Streaming Table (incremental, with derived resolution fields)
- Used `@dp.expect_or_drop` for inline data quality enforcement

### 6. SDP Pipeline — Gold Layer

- `gold_device_daily_stats` → Materialized View, aggregates readings by device × metric × day
  - Enriched with device metadata (`device_type`, `region`, etc.) via `F.broadcast(silver_devices)`
- `gold_alert_daily_summary` → Materialized View, aggregates alerts by type × severity × day
  - Derives `open_count`, `resolution_rate`

### 7. Data Generation

- Used `dbldatagen>=0.4.0` for 10M+ row IoT synthetic dataset
- Three bronze tables: `devices` (10K), `sensor_readings` (10M), `alerts` (500K)
- Device type correlated to metric values via broadcast join + `F.randn()`
- Alert resolution time uses `dist.Exponential(rate=1/12)` (mean ~12 hrs)
- ~20% open alerts via probabilistic `_is_resolved` column
- `%pip install` removed from notebook — declared in job `environments.spec.dependencies`

### 8. AI/BI Dashboard

- 2 canvas pages + 1 global filters page
- **Fleet Overview**: 3 KPIs, daily trend lines, severity bar, alert type pie
- **Sensor Metrics**: 3 KPIs, fleet avg bar chart, device detail table
- **Global Filters**: date range, metric type, alert severity
- `dataset_catalog` / `dataset_schema` parameters set in dashboard resource YAML

### 9. Cleanup

- Removed deprecated files: `pipeline.py`, `ingest.py`, `transform.py`, `aggregate.py`, `main_job.yml`
- Bundle validates clean after each major change
