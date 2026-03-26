# Databricks & Spark Skills for Claude Code

Custom skills that extend [Claude Code](https://docs.anthropic.com/en/docs/claude-code) with Databricks and Spark expertise. Wraps the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) with additional custom skills.

## Repository Structure

```
rob_skills/
├── ai-dev-kit/              # Git submodule — pulled fresh on install/update
│   ├── databricks-mcp-server/
│   ├── databricks-builder-app/
│   ├── databricks-tools-core/
│   ├── databricks-skills/
│   ├── hooks/
│   └── install.sh
├── custom-skills/           # Custom skills (this repo owns these)
│   ├── data-api-poc-builder/
│   ├── databricks-practice-skill/
│   ├── spark-job-optimization/
│   ├── dbldatagen/
│   ├── databricks-geospatial/
│   └── databricks-free-tier-guardrails/
├── update.sh                # One script: pull + install + custom skills
└── README.md
```

## Quick Start

One command to install everything — AI Dev Kit (MCP server, skills, hooks, configs) plus custom skills:

```bash
./update.sh
```

This will:
1. Pull the latest AI Dev Kit from upstream
2. Run the AI Dev Kit interactive installer (you'll answer prompts for tool selection, Databricks profile, scope, skill profiles, etc.)
3. Copy all custom skills into the same target directories

To force a full reinstall:
```bash
./update.sh --force
```

## Custom Skills

### data-api-poc-builder

Build proof-of-concept Databricks pipelines, dashboards, and AI/ML demos on top of external Kafka, PostgreSQL, and Neo4j data sources. Bridges the [data-api-collector-python](https://github.com/RobcPeng/data-api-collector-python) sandbox with Databricks AI Dev Kit skills.

| Feature | Details |
|---------|---------|
| **9 use-case blueprints** | Fraud Detection, Telemetry, Web Traffic, Student Enrollment, Grant & Budget, Citizen Services, K-12 Early Warning, Procurement, Case Management |
| **3 data sources** | Kafka (SASL_SSL streaming), PostgreSQL (JDBC + Lakehouse Federation), Neo4j (Spark Connector) |
| **POC components** | Pipelines (bronze/silver/gold), AI/BI Dashboards, AI Functions, ML models, Jobs, Apps, Bundles |
| **Skill routing** | Delegates to 10+ AI Dev Kit skills with pre-filled schemas and connection patterns |
| **Custom use cases** | Build ad-hoc blueprints for any Kafka topic, PG table, or Neo4j graph |

Assumes data sources are already running. Adapts for demo builders (fast, visual) and learners (step-by-step).

### databricks-practice-skill

An interactive coaching skill that turns Claude Code into a Spark & Databricks practice environment. Generates exercises, validates solutions, and provides feedback.

| Mode | Example Prompts |
|------|----------------|
| **Quick Drill** | "quick drill", "5 min practice" |
| **Deep Practice** | "practice window functions", "help me learn Delta Lake" |
| **Mock Exam** | "quiz me", "certification prep" |
| **Code Review** | "review my code", "what's wrong with this" |
| **Gap Assessment** | "what should I work on?" |

Covers: DataFrames, Spark SQL, Joins, Window Functions, Delta Lake, Structured Streaming, Auto Loader, SDP/DLT, Unity Catalog, Performance Tuning, Jobs, MLflow, and more.

### spark-job-optimization

A comprehensive reference for understanding, diagnosing, and optimizing Apache Spark jobs on Databricks. Covers foundational internals through advanced troubleshooting.

| Topic | File |
|-------|------|
| Spark Internals | `1-spark-internals-foundation.md` |
| Reading Query Plans | `2-reading-query-plans.md` |
| Spark UI Guide | `3-spark-ui-guide.md` |
| Databricks Diagnostics | `4-databricks-diagnostics.md` |
| Join Optimization | `5-join-optimization.md` |
| Shuffle & Partitioning | `6-shuffle-and-partitioning.md` |
| Memory & Spill | `7-memory-and-spill.md` |
| I/O & Storage | `8-io-and-storage.md` |
| Symptom Troubleshooting | `9-symptom-troubleshooting.md` |
| Hands-on Labs | `10-hands-on-labs.md` |

### databricks-geospatial

A comprehensive guide to geographic data analysis on Databricks covering spatial SQL, H3 hexagonal indexing, Overture Maps open data, ArcGIS/Esri integration, and real-world use cases.

| Topic | File |
|-------|------|
| Geospatial Foundations | `1-geospatial-foundations.md` |
| Spatial SQL Functions | `2-spatial-sql-functions.md` |
| H3 Hexagonal Indexing | `3-h3-hexagonal-indexing.md` |
| Overture Maps Data | `4-overture-maps-data.md` |
| Use Cases & Recipes | `5-use-cases-and-recipes.md` |
| ArcGIS Integration | `6-arcgis-integration.md` |

### dbldatagen

Generate large-scale synthetic data using [Databricks Labs dbldatagen](https://github.com/databrickslabs/dbldatagen) — a Spark-native, declarative library for generating millions to billions of rows efficiently as PySpark DataFrames.

| Feature | Details |
|---------|---------|
| **Non-linear distributions** | Gamma, Normal, Exponential, Beta, weighted categorical |
| **Text templates** | Regex-like patterns for emails, phone numbers, IDs |
| **Date ranges** | Controlled date/timestamp generation with `DateRange` |
| **Row coherence** | Correlated columns via `baseColumn` + `expr` |
| **Referential integrity** | Hash-based foreign keys across multi-table datasets |
| **Event spikes** | Two-spec union pattern for anomaly/incident periods |

### databricks-free-tier-guardrails

Compatibility filter applied after other generation skills to ensure artifacts work on Databricks free tier (serverless compute only). Checks for banned APIs, library availability, and cluster configuration restrictions.

## Usage

Open Claude Code in a project where the skills are installed and try:

```
> Build a fraud detection POC with pipeline and dashboard

> I want a streaming pipeline for the K-12 early warning data

> Create a full-stack POC for citizen services — pipeline, dashboard, and AI

> Build a telemetry anomaly detection demo

> Let's practice Spark

> Give me a Delta Lake MERGE challenge

> My Spark job is slow, help me optimize it

> Generate synthetic customer and order data for my project

> Load Overture Maps buildings data for San Francisco

> Create an H3 heatmap of sales by hexagonal grid
```

## Manual Installation

If you prefer to install custom skills without running the full AI Dev Kit installer:

```bash
# Copy custom skills into your project's skill directory
for skill in custom-skills/*/; do
    cp -r "$skill" .claude/skills/$(basename "$skill")
done
```

## License

These skills are provided for educational use. They reference the Databricks AI Dev Kit (Databricks License) and Apache Spark (Apache 2.0 License).
