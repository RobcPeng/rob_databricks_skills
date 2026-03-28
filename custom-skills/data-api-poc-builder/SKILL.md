---
name: data-api-poc-builder
description: "Build POC Databricks artifacts on top of external Kafka, PostgreSQL, and Neo4j data sources via data-api-collector-python."
disable-model-invocation: true
---

# Data API POC Builder

Build proof-of-concept Databricks artifacts — pipelines, dashboards, AI/ML models — on top of external Kafka, PostgreSQL, and Neo4j data sources. This skill bridges external data offerings with Databricks AI Dev Kit skills.

## Assumptions

- Data sources are already running and populated (Kafka topics have data, PG tables exist, Neo4j graphs are loaded)
- Databricks secrets are configured in a `data-api` scope (or user-specified scope)
- User has a Unity Catalog catalog and schema for output tables
- AI Dev Kit skills are installed and available

## Workflow

```
1. Identify use case → pre-built (9 available) or custom
2. Identify what to build → pipeline, dashboard, AI/ML, or combination
3. Provide blueprint → schemas, connection patterns, recommended approach
4. Delegate to AI Dev Kit skill → with pre-filled context
```

### Step 1: Identify the Use Case

Ask the user which use case they want to build on:

**Core streaming use cases:**
| Use Case | Kafka Topic | Domain |
|----------|-------------|--------|
| Fraud Detection | `streaming-fraud-transactions` | Financial crime detection |
| Telemetry | `streaming-device-telemetry` | IoT monitoring & anomaly detection |
| Web Traffic | `streaming-web-traffic` | Clickstream analytics |

**SLED use cases** (State/Local/Education — available across Kafka, PostgreSQL, and Neo4j):
| Use Case | Kafka Topic | Domain |
|----------|-------------|--------|
| Student Enrollment | `streaming-student-enrollment` | Academic records & course management |
| Grant & Budget | `streaming-grant-budget` | Public funding & vendor management |
| Citizen Services | `streaming-citizen-services` | 311 municipal service requests |
| K-12 Early Warning | `streaming-k12-early-warning` | Student risk indicators & interventions |
| Procurement | `streaming-procurement` | Contract & vendor management |
| Case Management | `streaming-case-management` | HHS client cases & benefits |

If the user describes something not in this list, follow the **Custom Use Cases** in [3-custom-use-cases.md](3-custom-use-cases.md).

### Step 2: Identify What to Build

Ask what they want to build. Common patterns:
- **Pipeline only** — Ingest, transform, persist to Delta
- **Pipeline + Dashboard** — End-to-end from stream to visualization
- **Pipeline + AI/ML** — Ingest then apply models or AI functions
- **Full stack** — Pipeline + Dashboard + AI/ML + Job orchestration
- **Single component** — Just a dashboard on existing tables, or just an ML model

### Step 3: Provide Blueprint

Use the use-case blueprints in [2-use-case-catalog.md](2-use-case-catalog.md) to provide:
- Exact schemas for the data sources involved
- Connection patterns from [1-connection-patterns.md](1-connection-patterns.md)
- Recommended POC components and approach
- Specific SQL queries, model suggestions, or pipeline structures

### Step 4: Delegate to AI Dev Kit Skill

Route to the appropriate skill with use-case-specific context:

| Component | Delegate To | Context to Provide |
|-----------|-------------|-------------------|
| Streaming pipeline (Kafka) | `databricks-spark-declarative-pipelines` | Kafka topic, parsed schema, bronze/silver/gold table definitions |
| Batch ingestion (PostgreSQL) | `databricks-spark-declarative-pipelines` | JDBC config, table name, column schema |
| Graph ingestion (Neo4j) | Connection patterns (no dedicated skill) | Bolt config, node labels, Cypher queries |
| AI/BI Dashboard | `databricks-aibi-dashboards` | SQL queries against Delta tables |
| ML model / agent | `databricks-model-serving` | Feature columns, label, model type |
| AI Functions on data | `databricks-ai-functions` | Column names, classification/extraction prompts |
| Agent evaluation | `databricks-mlflow-evaluation` | Scorer suggestions, eval dataset shape |
| Job orchestration | `databricks-jobs` | Task dependency graph |
| Web app | `databricks-app-python` | Data source connection, viz framework |
| CI/CD bundle | `databricks-bundles` | Resource definitions |

When delegating, always provide the use-case-specific schemas, table names, and suggested queries so the downstream skill can generate the right artifact.

---

## Supporting Files

| File | Contents |
|------|----------|
| [1-connection-patterns.md](1-connection-patterns.md) | Secrets setup, Kafka streaming, PostgreSQL JDBC, Neo4j Spark connector |
| [2-use-case-catalog.md](2-use-case-catalog.md) | All 9 pre-built use case blueprints with schemas and POC patterns |
| [3-custom-use-cases.md](3-custom-use-cases.md) | Custom use case workflow and audience adaptation guidance |
