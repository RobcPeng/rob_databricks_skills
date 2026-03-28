# Access Control & RBAC

Complete reference for Unity Catalog privileges, grants, ownership, and role-based access control patterns.

## Table of Contents

1. [How Access Control Works](#1-how-access-control-works)
2. [Admin Roles](#2-admin-roles)
3. [Privilege Types Reference](#3-privilege-types-reference)
4. [GRANT and REVOKE Syntax](#4-grant-and-revoke-syntax)
5. [Ownership](#5-ownership)
6. [RBAC Patterns](#6-rbac-patterns)
7. [Service Principals](#7-service-principals)
8. [Common Recipes](#8-common-recipes)

---

## 1. How Access Control Works

Unity Catalog access control has four properties:

```
┌─────────────────────────────────────────────────────────────────┐
│  Access Control Properties                                       │
│                                                                   │
│  EXPLICIT     ── No access unless explicitly granted              │
│  HIERARCHICAL ── Catalog grants cascade to schemas and objects    │
│  DELEGABLE    ── Owners can grant to others                       │
│  ADDITIVE     ── Users get the union of all group permissions     │
│                                                                   │
│  ⚠️  Metastore grants do NOT cascade to catalogs                  │
└─────────────────────────────────────────────────────────────────┘
```

### Who Can Grant Privileges

- Metastore admins
- Object owners (and owners of containing catalog/schema)
- Users with `MANAGE` privilege on the object
- Account admins (on metastores)

### Principal Types

```sql
-- Individual user (email format)
GRANT SELECT ON TABLE sales.gold.revenue TO `alice@company.com`;

-- Group
GRANT SELECT ON SCHEMA sales.gold TO `analytics-team`;

-- Service principal (applicationId)
GRANT SELECT ON CATALOG sales TO `2f631b2a-1b2c-3d4e-5f6a-7b8c9d0e1f2a`;
```

> Names with special characters (including `@` and `-`) must be enclosed in backticks.

---

## 2. Admin Roles

| Role | Scope | Key Capabilities |
|------|-------|------------------|
| **Account admin** | Entire account | Create metastores, manage users/groups, assign workspaces |
| **Metastore admin** | One metastore | All objects in metastore, grant top-level privileges |
| **Workspace admin** | One workspace | Workspace settings, compute, identity management |

**Best practice:** Restrict account admin to 2-3 trusted individuals. Use SSO + SCIM for identity management. Separate admin duties across roles.

---

## 3. Privilege Types Reference

### Data Access Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `SELECT` | Table, View, MV, Share, Catalog, Schema | Read data |
| `MODIFY` | Table, Catalog, Schema | INSERT, UPDATE, DELETE data |
| `READ VOLUME` | Volume, Catalog, Schema | Read files in volumes |
| `WRITE VOLUME` | Volume, Catalog, Schema | Write files to volumes |
| `READ FILES` | External Location | Read from cloud storage directly |
| `WRITE FILES` | External Location | Write to cloud storage directly |
| `REFRESH` | Materialized View, Catalog, Schema | Refresh materialized views |

### Object Creation Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `CREATE CATALOG` | Metastore | Create catalogs |
| `CREATE SCHEMA` | Catalog | Create schemas within catalog |
| `CREATE TABLE` | Schema, Catalog | Create tables or views |
| `CREATE VOLUME` | Schema, Catalog | Create volumes |
| `CREATE FUNCTION` | Schema, Catalog | Create UDFs |
| `CREATE MODEL` | Schema, Catalog | Create MLflow registered models |
| `CREATE MATERIALIZED VIEW` | Schema, Catalog | Create materialized views |
| `CREATE MODEL VERSION` | Model | Register new model versions |

### Navigation Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `USE CATALOG` | Catalog | Required to interact with anything in the catalog |
| `USE SCHEMA` | Schema, Catalog | Required to interact with anything in the schema |
| `BROWSE` | Catalog, External Location | View metadata without USE CATALOG |
| `EXECUTE` | Function, Model, Catalog, Schema | Invoke UDFs or load models |

### Administrative Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `ALL PRIVILEGES` | Most objects | All applicable privileges (excludes MANAGE, EXTERNAL USE) |
| `MANAGE` | Most objects | Grant privileges, transfer ownership, drop/rename |
| `APPLY TAG` | Catalog, Schema, Table, Volume, MV, View, Model | Add/edit tags |

### External Access Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `CREATE EXTERNAL LOCATION` | Metastore, Storage Credential | Register cloud storage paths |
| `CREATE EXTERNAL TABLE` | External Location | Create tables on external storage |
| `CREATE EXTERNAL VOLUME` | External Location | Create volumes on external storage |
| `EXTERNAL USE SCHEMA` | Schema | Access from external engines (Iceberg REST) |
| `EXTERNAL USE LOCATION` | External Location | Access from external engines |
| `CREATE CONNECTION` | Metastore, Service Credential | Lakehouse Federation connections |
| `USE CONNECTION` | Connection | Use a federation connection |
| `ACCESS` | Service Credential | Use credential for external services |

### Delta Sharing Privileges

| Privilege | Applies to | Description |
|-----------|------------|-------------|
| `CREATE SHARE` | Metastore | Create data shares |
| `CREATE RECIPIENT` | Metastore | Create share recipients |
| `CREATE PROVIDER` | Metastore | Create share providers |
| `SET SHARE PERMISSION` | Metastore | Grant recipient access to shares |
| `USE SHARE` | Metastore | Read-only access to all shares |
| `USE RECIPIENT` | Metastore | Read-only access to all recipients |
| `USE PROVIDER` | Metastore | Read-only access to all providers |

---

## 4. GRANT and REVOKE Syntax

### Granting Privileges

```sql
-- Basic syntax
GRANT <privilege> ON <object-type> <object-name> TO <principal>;

-- Grant read access to a table
GRANT SELECT ON TABLE sales.gold.revenue TO `analytics-team`;

-- Grant full schema access (cascades to all tables within)
GRANT SELECT ON SCHEMA sales.gold TO `analytics-team`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `analytics-team`;
GRANT USE CATALOG ON CATALOG sales TO `analytics-team`;

-- Grant catalog-wide read (all current and future tables)
GRANT SELECT ON CATALOG sales TO `data-scientists`;
GRANT USE CATALOG ON CATALOG sales TO `data-scientists`;

-- Grant creation privileges
GRANT CREATE TABLE ON SCHEMA sales.bronze TO `data-engineers`;
GRANT CREATE FUNCTION ON SCHEMA sales.utilities TO `data-engineers`;

-- Grant on metastore (no object name needed)
GRANT CREATE CATALOG ON METASTORE TO `platform-team`;

-- Grant model execution
GRANT EXECUTE ON FUNCTION prod.ml_team.fraud_model TO `ml-inference`;
```

> **Important:** `USE CATALOG` + `USE SCHEMA` are always required for a user to access objects. Think of them as "door keys" — you need them to enter before any other privilege matters.

### Revoking Privileges

```sql
-- Basic syntax
REVOKE <privilege> ON <object-type> <object-name> FROM <principal>;

-- Revoke table access
REVOKE SELECT ON TABLE sales.gold.revenue FROM `analytics-team`;

-- Revoke schema creation
REVOKE CREATE TABLE ON SCHEMA sales.bronze FROM `data-engineers`;

-- Revoke from metastore
REVOKE CREATE CATALOG ON METASTORE FROM `platform-team`;
```

> A `REVOKE` succeeds even if the privilege was never granted — it ensures the privilege is not present.

### Showing Grants

```sql
-- Show all grants on an object
SHOW GRANTS ON TABLE sales.gold.revenue;
SHOW GRANTS ON SCHEMA sales.gold;
SHOW GRANTS ON CATALOG sales;

-- Show grants for a specific principal
SHOW GRANTS `analytics-team` ON SCHEMA sales.gold;

-- Show grants on metastore
SHOW GRANTS ON METASTORE;
```

---

## 5. Ownership

Every securable object has exactly one owner. The owner:
- Has all privileges on the object automatically
- Can grant privileges to others
- Can transfer ownership

```sql
-- Transfer ownership
ALTER TABLE sales.gold.revenue SET OWNER TO `data-governance-team`;
ALTER SCHEMA sales.gold SET OWNER TO `data-governance-team`;
ALTER CATALOG sales SET OWNER TO `data-governance-team`;
```

> **Ownership vs MANAGE:** The `MANAGE` privilege lets a user grant access and modify objects without being the owner. Use this for delegated administration without transferring ownership.

---

## 6. RBAC Patterns

### Pattern: Team-Based Access with Groups

```sql
-- Create the group hierarchy
-- (Groups are managed via SCIM, Workspace Admin UI, or Databricks CLI)

-- Give all data scientists read access to gold layer
GRANT USE CATALOG ON CATALOG sales TO `data-scientists`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `data-scientists`;
GRANT SELECT ON SCHEMA sales.gold TO `data-scientists`;

-- Give data engineers write access to bronze + silver
GRANT USE CATALOG ON CATALOG sales TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA sales.bronze TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA sales.silver TO `data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA sales.bronze TO `data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA sales.silver TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA sales.bronze TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA sales.silver TO `data-engineers`;

-- Give governance team full control via MANAGE
GRANT MANAGE ON CATALOG sales TO `data-governance`;
```

### Pattern: Environment Isolation

```sql
-- Dev: broad access for experimentation
GRANT USE CATALOG ON CATALOG dev TO `all-developers`;
GRANT ALL PRIVILEGES ON CATALOG dev TO `all-developers`;

-- Staging: read for QA, write for CI/CD service principal
GRANT USE CATALOG ON CATALOG staging TO `qa-team`;
GRANT SELECT ON CATALOG staging TO `qa-team`;
GRANT ALL PRIVILEGES ON CATALOG staging TO `cicd-service-principal`;

-- Prod: read-only for most, write only via service principal
GRANT USE CATALOG ON CATALOG prod TO `all-developers`;
GRANT SELECT ON CATALOG prod TO `all-developers`;
GRANT ALL PRIVILEGES ON CATALOG prod TO `prod-deploy-sp`;
```

### Pattern: Least Privilege for Dashboards

```sql
-- Dashboard service account gets only what it needs
GRANT USE CATALOG ON CATALOG prod TO `dashboard-sp`;
GRANT USE SCHEMA ON SCHEMA prod.gold TO `dashboard-sp`;
GRANT SELECT ON SCHEMA prod.gold TO `dashboard-sp`;
-- No MODIFY, no CREATE, no access to bronze/silver
```

---

## 7. Service Principals

Service principals are non-human identities for automated workloads. Use them for:
- Production pipelines and jobs
- CI/CD deployments
- Dashboard data access
- API integrations

```sql
-- Grant to a service principal (use applicationId)
GRANT SELECT ON CATALOG prod TO `2f631b2a-1b2c-3d4e-5f6a-7b8c9d0e1f2a`;

-- Best practice: create a group, add the SP, grant to the group
-- (Manage via SCIM or Databricks CLI, not SQL)
-- Then grant to the group:
GRANT ALL PRIVILEGES ON CATALOG prod TO `prod-automation`;
```

**Best practices:**
- Never use personal accounts for production workloads
- One service principal per workload (pipeline, job, app)
- Add service principals to groups for easier management
- Use OAuth token authentication with enforced token management

---

## 8. Common Recipes

### "New team member needs access to the gold layer"

```sql
-- Add user to the appropriate group (via SCIM/Admin UI)
-- Then the group grants handle everything automatically.
-- If one-off access is needed:
GRANT USE CATALOG ON CATALOG sales TO `newuser@company.com`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `newuser@company.com`;
GRANT SELECT ON SCHEMA sales.gold TO `newuser@company.com`;
```

### "Lock down a sensitive table within an open schema"

```sql
-- Revoke broad schema-level SELECT won't work (it's additive)
-- Instead: use row filters or column masks (see file 4)
-- Or: move sensitive tables to a restricted schema
CREATE SCHEMA sales.restricted;
ALTER TABLE sales.gold.customer_pii SET SCHEMA sales.restricted;
-- Only grant access to the restricted schema to authorized groups
GRANT USE SCHEMA ON SCHEMA sales.restricted TO `pii-authorized`;
GRANT SELECT ON SCHEMA sales.restricted TO `pii-authorized`;
```

### "Audit who has access to what"

```sql
-- List all grants on a catalog
SHOW GRANTS ON CATALOG sales;

-- List all grants for a specific group across objects
SHOW GRANTS `analytics-team` ON CATALOG sales;
SHOW GRANTS `analytics-team` ON SCHEMA sales.gold;

-- For comprehensive audit, query system tables (see file 6)
```

---

## Next Steps

- Classify data with tags → [3-tagging-classification.md](3-tagging-classification.md)
- Protect sensitive columns/rows → [4-row-filters-column-masks.md](4-row-filters-column-masks.md)
