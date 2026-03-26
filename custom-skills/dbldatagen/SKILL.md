---
name: dbldatagen
description: >
  Generate large-scale synthetic data using Databricks Labs dbldatagen (Spark-native, declarative)
  or Spark + Faker + Pandas UDFs. Use when creating test data, demo datasets, or synthetic tables
  at scale. Covers both dbldatagen and Faker approaches with guidance on when to use each.
  Triggers on: 'dbldatagen', 'synthetic data', 'generate data', 'test data', 'fake data',
  'data generator', 'DataGenerator', 'withColumn spec', 'mock data', 'sample dataset',
  'Faker', 'fake names', 'fake addresses', 'realistic data', 'Pandas UDF'.
---

# Synthetic Data Generation with dbldatagen

Generate synthetic data for Databricks using **dbldatagen** — a Spark-native, declarative library from Databricks Labs designed for generating large volumes of data efficiently. It generates data directly as PySpark DataFrames without pandas, making it ideal for large datasets (millions to billions of rows).

## How to Use This Skill

**Quick reference (API lookup):** Jump to [1-core-api.md](1-core-api.md).

**Need realistic distributions or text:** See [2-distributions-and-text.md](2-distributions-and-text.md).

**Building related tables or correlated data:** See [3-relationships-and-coherence.md](3-relationships-and-coherence.md).

**Advanced features (streaming, CDC, complex types):** See [4-advanced-features.md](4-advanced-features.md).

**Need a domain template or full example:** See [5-recipes-and-examples.md](5-recipes-and-examples.md).

**Want realistic names/addresses via Faker:** See [6-faker-alternative.md](6-faker-alternative.md).

## Routing Table

| File | Topic | When to Use |
|------|-------|-------------|
| [1-core-api.md](1-core-api.md) | Core API | DataGenerator, withColumn, withColumnSpec, withColumnSpecs, withSchema, standard datasets |
| [2-distributions-and-text.md](2-distributions-and-text.md) | Distributions & Text | Non-linear distributions, text templates, ILText, Faker integration, date ranges |
| [3-relationships-and-coherence.md](3-relationships-and-coherence.md) | Relationships | Row coherence, multi-table FKs, event spikes |
| [4-advanced-features.md](4-advanced-features.md) | Advanced | Complex types, constraints, streaming, CDC, DataAnalyzer |
| [5-recipes-and-examples.md](5-recipes-and-examples.md) | Recipes | Domain templates, complete example, storage, best practices |
| [6-faker-alternative.md](6-faker-alternative.md) | Faker | Spark + Faker + Pandas UDFs, realistic text, locales, hybrid approach, complete Faker example |

## Installation

dbldatagen is NOT pre-installed on Databricks. Install it using `execute_databricks_command` tool:
- `code`: "%pip install dbldatagen"

Save the returned `cluster_id` and `context_id` for subsequent calls.

## Workflow

1. **Write Python code to a local file** (e.g., `scripts/generate_data.py`)
2. **Execute on Databricks** using the `run_python_file_on_databricks` MCP tool
3. **If execution fails**: Edit the local file to fix the error, then re-execute
4. **Reuse the context** by passing the returned `cluster_id` and `context_id`

**Always work with local files first, then execute.**

### Context Reuse Pattern

**First execution**:
- `file_path`: "scripts/generate_data.py"

Returns: `{ success, output, error, cluster_id, context_id, ... }`

**If execution fails:**
1. Edit the local Python file to fix the issue
2. Re-execute: `file_path`, `cluster_id`, `context_id`

**Follow-up executions** reuse context (faster, keeps installed libraries).

## Quick Start

```python
import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import LongType

gen = (
    dg.DataGenerator(spark, name="quick_start", rows=10_000,
                     partitions=4, randomSeedMethod="hash_fieldname")
    .withIdOutput()
    .withColumn("name", "string", template=r'\w \w')
    .withColumn("tier", "string",
                values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
                random=True)
    .withColumn("amount", "double", minValue=0, maxValue=5000,
                distribution=dist.Gamma(1.5, 200.0), random=True)
    .withColumn("created_at", "date",
                data_range=dg.DateRange("2024-01-01", "2024-12-31", "days=1"),
                random=True)
    .build()
)
gen.show(10)
```

## When to Use dbldatagen vs Faker

| Use Case | dbldatagen | Faker |
|----------|------------|-------|
| Millions/billions of rows | Best choice | Too slow |
| Purely Spark-native pipeline | Best choice | Needs pandas |
| Realistic text (names, addresses) | Template-based | Best choice |
| Complex multi-step business logic | expr can be verbose | Python loops |
| Referential integrity at scale | hash-based FKs | Memory-limited |
| Reproducibility by default | randomSeed | Requires seeding |
| Delta Live Tables / SDP support | Native | Needs workarounds |
| Streaming data generation | Native (withStreaming) | Not supported |
| Schema inference from existing data | DataAnalyzer | Not supported |
| CDC simulation | scriptMerge | Manual |
| Complex types (struct, array, JSON) | withStructColumn | Manual |
| Constraints / business rules | withSqlConstraint | Manual |

## Related Skills

- **[spark-declarative-pipelines](../spark-declarative-pipelines/SKILL.md)** — build bronze/silver/gold pipelines on top of generated data
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** — visualize generated data
- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — manage catalogs, schemas, and volumes
- **[databricks-free-tier-guardrails](../databricks-free-tier-guardrails/SKILL.md)** — compatibility check for serverless compute
- **[databricks-geospatial](../databricks-geospatial/SKILL.md)** — generate synthetic geo data with lat/lon columns
