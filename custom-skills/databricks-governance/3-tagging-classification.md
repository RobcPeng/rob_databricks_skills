# Tagging & Data Classification

Organize, classify, and discover data assets using Unity Catalog tags — from ad-hoc labels to governed classification taxonomies.

## Table of Contents

1. [What Are Tags?](#1-what-are-tags)
2. [Tag Types](#2-tag-types)
3. [SQL Syntax](#3-sql-syntax)
4. [Querying Tags via Information Schema](#4-querying-tags-via-information-schema)
5. [Data Classification Patterns](#5-data-classification-patterns)
6. [Tag Inheritance in ABAC](#6-tag-inheritance-in-abac)
7. [Common Recipes](#7-common-recipes)

---

## 1. What Are Tags?

Tags are key-value attributes you attach to Unity Catalog objects for organization, classification, search, and policy enforcement (ABAC).

### Supported Objects

Tags can be applied to: catalogs, schemas, tables, table columns, volumes, views, registered models, model versions, dashboards, Genie spaces, Databricks apps, notebooks.

### Constraints

| Constraint | Limit |
|------------|-------|
| Tag key max length | 255 characters |
| Tag value max length | 1,000 characters |
| Max tags per object | 50 |
| Max column tags per table | 1,000 total |
| Disallowed key characters | `. , - = / :` |
| Leading/trailing spaces | Not allowed |

> **Security warning:** Tag data is stored as plain text and may replicate globally. Never store secrets or sensitive PII in tag keys or values.

---

## 2. Tag Types

### Custom Tags

User-defined tags for any purpose. Any user with `APPLY TAG` privilege can create and assign them.

```sql
SET TAG ON TABLE sales.gold.revenue `team` = `analytics`;
SET TAG ON TABLE sales.gold.revenue `data_owner` = `alice@company.com`;
```

### Governed Tags (Public Preview)

Account-level tags with enforced consistency:
- Only assignable by users with `ASSIGN` permission
- Must use predefined values from tag policies
- Marked with a lock icon in the Catalog Explorer UI
- Cannot be freely created — requires admin setup

Use governed tags when you need enterprise-wide consistency (e.g., data classification, compliance labels).

### System Tags

Predefined by Databricks:
- Keys and values are immutable
- Marked with a wrench icon in the UI
- Provide standardized tagging across organizations

---

## 3. SQL Syntax

### Setting Tags (DBR 16.1+)

```sql
-- On catalogs
SET TAG ON CATALOG sales `environment` = `production`;

-- On schemas
SET TAG ON SCHEMA sales.gold `data_layer` = `curated`;

-- On tables
SET TAG ON TABLE sales.gold.revenue `sensitivity` = `high`;
SET TAG ON TABLE sales.gold.revenue `pii` = `true`;

-- On columns
SET TAG ON COLUMN sales.gold.customers.email `pii_type` = `email`;
SET TAG ON COLUMN sales.gold.customers.ssn `pii_type` = `ssn`;
SET TAG ON COLUMN sales.gold.customers.phone `pii_type` = `phone`;

-- On volumes
SET TAG ON VOLUME sales.raw.documents `retention` = `7years`;

-- Multiple tags on one object (separate statements)
SET TAG ON TABLE sales.gold.revenue `sensitivity` = `high`;
SET TAG ON TABLE sales.gold.revenue `domain` = `finance`;
SET TAG ON TABLE sales.gold.revenue `compliance` = `sox`;
```

### Setting Tags (DBR 13.3+ — legacy syntax)

```sql
-- Table-level tags
ALTER TABLE sales.gold.revenue
SET TAGS ('sensitivity' = 'high', 'pii' = 'true');

-- Column-level tags
ALTER TABLE sales.gold.customers
ALTER COLUMN email
SET TAGS ('pii_type' = 'email');

ALTER TABLE sales.gold.customers
ALTER COLUMN ssn
SET TAGS ('pii_type' = 'ssn');
```

> **Note:** You cannot tag multiple columns in a single ALTER TABLE statement. Each column requires its own command.

### Removing Tags

```sql
-- Remove from objects (DBR 16.1+)
UNSET TAG ON CATALOG sales environment;
UNSET TAG ON TABLE sales.gold.revenue sensitivity;
UNSET TAG ON COLUMN sales.gold.customers.email pii_type;

-- Legacy syntax (DBR 13.3+)
ALTER TABLE sales.gold.revenue UNSET TAGS ('sensitivity', 'pii');
ALTER TABLE sales.gold.customers ALTER COLUMN email UNSET TAGS ('pii_type');
```

> **Important:** You must remove all governed tags from a column before you can drop that column.

---

## 4. Querying Tags via Information Schema

```sql
-- All tags on catalogs
SELECT * FROM system.information_schema.catalog_tags
WHERE catalog_name = 'sales';

-- All tags on schemas
SELECT * FROM system.information_schema.schema_tags
WHERE catalog_name = 'sales';

-- All tags on tables
SELECT * FROM system.information_schema.table_tags
WHERE catalog_name = 'sales';

-- All tags on columns
SELECT * FROM system.information_schema.column_tags
WHERE catalog_name = 'sales';

-- All tags on volumes
SELECT * FROM system.information_schema.volume_tags
WHERE catalog_name = 'sales';
```

### Find All PII-Tagged Columns

```sql
SELECT
    catalog_name,
    schema_name,
    table_name,
    column_name,
    tag_name,
    tag_value
FROM system.information_schema.column_tags
WHERE tag_name = 'pii_type'
ORDER BY catalog_name, schema_name, table_name, column_name;
```

### Find All Tables with a Specific Classification

```sql
SELECT
    catalog_name,
    schema_name,
    table_name,
    tag_name,
    tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'sensitivity' AND tag_value = 'high'
ORDER BY catalog_name, schema_name, table_name;
```

---

## 5. Data Classification Patterns

### Pattern: Sensitivity Levels

```sql
-- Define a consistent sensitivity taxonomy
-- Level 1: Public — no restrictions
-- Level 2: Internal — requires authentication
-- Level 3: Confidential — restricted to specific teams
-- Level 4: Restricted — PII, PHI, financial data

SET TAG ON TABLE sales.gold.aggregate_metrics `sensitivity` = `public`;
SET TAG ON TABLE sales.gold.revenue `sensitivity` = `internal`;
SET TAG ON TABLE sales.gold.customer_segments `sensitivity` = `confidential`;
SET TAG ON TABLE sales.gold.customer_pii `sensitivity` = `restricted`;
```

### Pattern: PII Column Classification

```sql
-- Tag individual columns with PII type for targeted masking
SET TAG ON COLUMN hr.employees.ssn `pii_type` = `ssn`;
SET TAG ON COLUMN hr.employees.email `pii_type` = `email`;
SET TAG ON COLUMN hr.employees.phone `pii_type` = `phone`;
SET TAG ON COLUMN hr.employees.salary `pii_type` = `financial`;
SET TAG ON COLUMN hr.employees.date_of_birth `pii_type` = `dob`;
SET TAG ON COLUMN hr.employees.home_address `pii_type` = `address`;

-- These tags can drive ABAC column masks (see file 5)
```

### Pattern: Compliance Labeling

```sql
-- Tag tables with applicable regulations
SET TAG ON TABLE healthcare.gold.patient_records `compliance` = `hipaa`;
SET TAG ON TABLE finance.gold.transactions `compliance` = `sox`;
SET TAG ON TABLE marketing.gold.eu_customers `compliance` = `gdpr`;
SET TAG ON TABLE sales.gold.ca_customers `compliance` = `ccpa`;

-- Tag with retention requirements
SET TAG ON TABLE finance.gold.transactions `retention` = `7years`;
SET TAG ON TABLE healthcare.gold.patient_records `retention` = `10years`;
```

### Pattern: Data Domain / Ownership

```sql
-- Tag catalogs and schemas with domain ownership
SET TAG ON CATALOG sales `domain` = `revenue`;
SET TAG ON CATALOG sales `data_owner` = `sales-data-team`;
SET TAG ON CATALOG sales `steward` = `alice@company.com`;

SET TAG ON SCHEMA sales.gold `refresh_cadence` = `daily`;
SET TAG ON SCHEMA sales.bronze `refresh_cadence` = `realtime`;
```

---

## 6. Tag Inheritance in ABAC

When used with ABAC policies, tags inherit downward through the hierarchy:

```
catalog (tagged: sensitivity=high)
  └── schema (inherits: sensitivity=high)
        └── table (inherits: sensitivity=high)
              └── column (does NOT inherit tags)
```

> **Important:** Tag inheritance for ABAC stops at the table level. Column-level tags must be applied explicitly.

This means you can tag an entire catalog as `sensitivity=high` and have a single ABAC policy enforce masking on all tables within it — without tagging each table individually.

---

## 7. Common Recipes

### Bulk-Tag All PII Columns in a Schema

```python
# Run via execute_databricks_command or notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Define PII column patterns
pii_columns = {
    "email": "email",
    "ssn": "ssn",
    "social_security": "ssn",
    "phone": "phone",
    "phone_number": "phone",
    "date_of_birth": "dob",
    "dob": "dob",
    "address": "address",
    "home_address": "address",
    "salary": "financial",
    "credit_card": "financial",
}

catalog = "sales"
schema = "gold"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
for table_row in tables:
    table_name = f"{catalog}.{schema}.{table_row.tableName}"
    columns = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
    for col in columns:
        col_lower = col.col_name.lower()
        if col_lower in pii_columns:
            pii_type = pii_columns[col_lower]
            spark.sql(f"SET TAG ON COLUMN {table_name}.{col.col_name} `pii_type` = `{pii_type}`")
            print(f"Tagged {table_name}.{col.col_name} as pii_type={pii_type}")
```

### Generate a Data Classification Report

```sql
-- Comprehensive classification report across all catalogs
SELECT
    ct.catalog_name,
    ct.tag_value AS sensitivity,
    COUNT(DISTINCT tt.schema_name || '.' || tt.table_name) AS table_count,
    COUNT(DISTINCT colt.column_name) AS pii_column_count
FROM system.information_schema.catalog_tags ct
LEFT JOIN system.information_schema.table_tags tt
    ON ct.catalog_name = tt.catalog_name
LEFT JOIN system.information_schema.column_tags colt
    ON tt.catalog_name = colt.catalog_name
    AND tt.schema_name = colt.schema_name
    AND tt.table_name = colt.table_name
    AND colt.tag_name = 'pii_type'
WHERE ct.tag_name = 'sensitivity'
GROUP BY ct.catalog_name, ct.tag_value
ORDER BY ct.catalog_name;
```

---

## Next Steps

- Protect tagged data with masks and filters → [4-row-filters-column-masks.md](4-row-filters-column-masks.md)
- Scale with ABAC policies → [5-abac-policies.md](5-abac-policies.md)
