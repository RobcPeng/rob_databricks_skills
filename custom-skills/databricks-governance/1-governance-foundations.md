# Governance Foundations

The mental model for data governance on Databricks — how Unity Catalog organizes, secures, and audits your data and AI assets.

## Table of Contents

1. [Why Governance Matters](#1-why-governance-matters)
2. [Unity Catalog Object Hierarchy](#2-unity-catalog-object-hierarchy)
3. [Governance Models](#3-governance-models)
4. [Catalog Design Patterns](#4-catalog-design-patterns)
5. [The Three Pillars](#5-the-three-pillars)

---

## 1. Why Governance Matters

Data governance is the oversight that ensures data and AI bring value. Without it, you get:
- Uncontrolled access to sensitive data
- No audit trail for compliance
- Duplicate, inconsistent data across teams
- No visibility into what data exists or who uses it

Databricks governance is built on **Unity Catalog** — a unified catalog that centrally stores all data and analytical artifacts with their metadata, permissions, lineage, and quality constraints.

### Well-Architected Framework (WAF) Governance Principles

| Principle | What it means |
|-----------|---------------|
| **Unify data and AI management** | One catalog for all assets — tables, views, models, functions, volumes |
| **Unify data and AI security** | Central permissions + audit across all data |
| **Establish data quality standards** | Expectations, monitoring, standardized formats |

---

## 2. Unity Catalog Object Hierarchy

Everything in Unity Catalog follows a three-level namespace:

```
┌──────────────────────────────────────────────────────────────┐
│  METASTORE (one per cloud region)                            │
│  ├── CATALOG (logical grouping — domain, env, or team)       │
│  │   ├── SCHEMA (namespace within catalog)                   │
│  │   │   ├── TABLE / VIEW / MATERIALIZED VIEW                │
│  │   │   ├── VOLUME (file storage)                           │
│  │   │   ├── FUNCTION (UDFs, including registered models)    │
│  │   │   └── MODEL (MLflow registered models)                │
│  │   └── SCHEMA ...                                          │
│  ├── CATALOG ...                                             │
│  ├── EXTERNAL LOCATION (cloud storage paths)                 │
│  ├── STORAGE CREDENTIAL (cloud auth for storage)             │
│  ├── SERVICE CREDENTIAL (cloud auth for services)            │
│  └── CONNECTION (Lakehouse Federation to external DBs)       │
└──────────────────────────────────────────────────────────────┘
```

### Addressing Objects

```sql
-- Three-level namespace: catalog.schema.object
SELECT * FROM sales.bronze.transactions;

-- Two-level (uses default catalog): schema.object
SELECT * FROM bronze.transactions;

-- One-level (uses default catalog + schema): object
SELECT * FROM transactions;
```

### Key Rules

- **One metastore per cloud region** — deploy regionally for performance and data residency
- **Privileges inherit downward** from catalog → schema → object (but NOT from metastore)
- **Every object has an owner** who has all privileges and can grant to others
- **Best practice:** Run the lakehouse in a single account with one Unity Catalog

---

## 3. Governance Models

Choose based on your org structure and compliance requirements:

### Centralized Model

```
Central IT owns metastore + all catalogs
    └── All permissions flow from governance team
```

- **Best for:** Strong central IT, strict compliance (finance, healthcare, government)
- **Pros:** Consistent policies, single point of control, easier auditing
- **Cons:** Bottleneck on the governance team, slower to grant access

### Federated (Distributed) Model

```
Central IT owns metastore
    ├── Sales team owns sales catalog
    ├── Marketing team owns marketing catalog
    └── Engineering team owns engineering catalog
```

- **Best for:** Large organizations with autonomous business units
- **Pros:** Teams move fast, domain expertise drives governance
- **Cons:** Potential inconsistency across domains, harder to enforce global policies

### Hybrid Model (Recommended for Most)

```
Central IT owns metastore + sensitive catalogs (PII, finance)
    ├── Regulated catalogs: centrally governed
    └── Operational catalogs: federated to domain teams
```

- **Best for:** Enterprises with a mix of regulated and operational data
- **Pros:** Strict governance where needed, autonomy elsewhere
- **Cons:** Requires clear ownership boundaries

---

## 4. Catalog Design Patterns

### Domain-Based Catalogs (Recommended)

```sql
-- One catalog per business domain
CREATE CATALOG IF NOT EXISTS sales;
CREATE CATALOG IF NOT EXISTS marketing;
CREATE CATALOG IF NOT EXISTS finance;
CREATE CATALOG IF NOT EXISTS hr;

-- Schemas within each domain follow medallion architecture
CREATE SCHEMA IF NOT EXISTS sales.bronze;
CREATE SCHEMA IF NOT EXISTS sales.silver;
CREATE SCHEMA IF NOT EXISTS sales.gold;
```

### Environment-Based Catalogs

```sql
-- Separate environments for dev/staging/prod
CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS staging;
CREATE CATALOG IF NOT EXISTS prod;

-- Same schemas appear in each environment
CREATE SCHEMA IF NOT EXISTS dev.sales;
CREATE SCHEMA IF NOT EXISTS prod.sales;
```

### Combined Pattern (Domain + Environment)

```sql
-- Environment prefix + domain
CREATE CATALOG IF NOT EXISTS prod_sales;
CREATE CATALOG IF NOT EXISTS dev_sales;

-- Or use bundle variables for DAB deployments
-- catalog: ${var.catalog}  →  resolves to dev_sales, prod_sales, etc.
```

### Schema Naming for Medallion Architecture

```sql
-- Within a domain catalog, schemas represent data layers
CREATE SCHEMA sales.01_bronze;    -- raw ingestion
CREATE SCHEMA sales.02_silver;    -- cleaned, conformed
CREATE SCHEMA sales.03_gold;      -- business-ready aggregates
CREATE SCHEMA sales.utilities;    -- UDFs, helpers
CREATE SCHEMA sales.ml_models;    -- registered models
```

---

## 5. The Three Pillars

### Pillar 1: Unified Management

| Capability | How Unity Catalog delivers |
|------------|---------------------------|
| Central metadata | All assets in one catalog with descriptions, comments, tags |
| Data discovery | Catalog Explorer UI — search, browse, see lineage |
| AI asset governance | Models, feature tables, and functions governed alongside data |
| Lineage | Automatic column-level lineage across notebooks, jobs, dashboards |

### Pillar 2: Unified Security

| Capability | How Unity Catalog delivers |
|------------|---------------------------|
| RBAC | GRANT/REVOKE privileges on any securable object |
| ABAC | Tag-driven policies for scalable, centralized control |
| Fine-grained | Row filters and column masks at the table level |
| Audit | system.access.audit captures every data access event |

### Pillar 3: Data Quality

| Capability | How Unity Catalog delivers |
|------------|---------------------------|
| Expectations | Pipeline quality constraints (DLT/SDP) |
| Monitoring | Lakehouse Monitoring for drift and anomaly detection |
| Standards | Standardized formats, naming conventions, data dictionaries |

---

## Next Steps

- Set up access control → [2-access-control-rbac.md](2-access-control-rbac.md)
- Classify and tag data → [3-tagging-classification.md](3-tagging-classification.md)
