# Databricks & Spark Skills for Claude Code

Custom skills that extend [Claude Code](https://docs.anthropic.com/en/docs/claude-code) with Databricks and Spark expertise. Designed to work with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit).

## Skills

This works in conjunction with ai-dev-kit - recommended approach is writing to parquet for a proper bronze layer for ingestion.

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

Generates raw data saved as Parquet to Unity Catalog Volumes, ready for downstream Spark Declarative Pipelines.

## Installation

### With the AI Dev Kit (recommended)

If you already have the [AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) installed, copy the skill folders into your project's `.claude/skills/` directory:

```bash
# From your project directory (where .claude/ exists)
cp -r databricks-practice-skill .claude/skills/databricks-practice
cp -r spark-job-optimization .claude/skills/spark-job-optimization
cp -r dbldatagen .claude/skills/dbldatagen
```

### Standalone

```bash
# Create the skills directory if it doesn't exist
mkdir -p .claude/skills

# Copy the skills
cp -r databricks-practice-skill .claude/skills/databricks-practice
cp -r spark-job-optimization .claude/skills/spark-job-optimization
cp -r dbldatagen .claude/skills/dbldatagen
```

The practice skill works without the AI Dev Kit in "review mode" only (code review without live execution). With the AI Dev Kit MCP server, exercises run against your real Databricks workspace.

## Usage

Open Claude Code in a project where the skills are installed and try:

```
> Let's practice Spark

> Give me a Delta Lake MERGE challenge

> Quiz me — I'm preparing for the DE Associate exam

> My Spark job is slow, help me optimize it

> Explain this query plan to me

> I'm getting OOM errors on my executor
>
> Generate synthetic customer and order data for my project
>
> Create test data with realistic distributions and referential integrity
```

## License

These skills are provided for educational use. They reference the Databricks AI Dev Kit (Databricks License) and Apache Spark (Apache 2.0 License).
