# Attribute-Based Access Control (ABAC)

Centralized, tag-driven governance that scales across your entire lakehouse — define policies once, enforce everywhere.

## Table of Contents

1. [What is ABAC?](#1-what-is-abac)
2. [ABAC vs Manual Filters/Masks](#2-abac-vs-manual-filtersmasks)
3. [How ABAC Works](#3-how-abac-works)
4. [Setting Up ABAC](#4-setting-up-abac)
5. [Row Filter Policies](#5-row-filter-policies)
6. [Column Mask Policies](#6-column-mask-policies)
7. [Policy Hierarchy and Inheritance](#7-policy-hierarchy-and-inheritance)
8. [Limitations](#8-limitations)

---

## 1. What is ABAC?

ABAC uses **governed tags** on data assets to automatically apply access control policies. Instead of manually attaching row filters and column masks to each table, you:

1. Define governed tags (e.g., `sensitivity=high`, `pii_type=ssn`)
2. Create policy functions (UDFs) that check tags and user context
3. Attach policies at the catalog/schema/table level
4. Unity Catalog enforces them automatically on any tagged object

Think of it like this: manual filters/masks are **locks on individual doors**. ABAC is a **security badge system** — define the rules centrally, badge the doors, and access is automatic.

---

## 2. ABAC vs Manual Filters/Masks

| Dimension | Manual Filters/Masks | ABAC Policies |
|-----------|---------------------|---------------|
| **Scale** | Per-table configuration | Define once, apply to all tagged objects |
| **Maintenance** | Must update each table individually | Update the policy, applies everywhere |
| **Override risk** | Table owners can remove | Higher-level admins control, owners can't override |
| **Setup complexity** | Simple for a few tables | More setup, but pays off at scale |
| **Tag requirement** | None — UDF logic is self-contained | Requires governed tags on objects |
| **Runtime requirement** | DBR 12.2+ | DBR 16.4+ or serverless |
| **Best for** | Small number of tables, custom logic | Enterprise-wide governance |

**Recommendation:** Start with manual filters/masks for quick wins. Graduate to ABAC when you have 10+ tables with similar governance needs or need centralized policy management.

---

## 3. How ABAC Works

```
┌──────────────────────────────────────────────────────────────────────┐
│  ABAC Policy Evaluation Flow                                          │
│                                                                        │
│  1. User queries a table                                               │
│     └── SELECT * FROM sales.gold.customers                             │
│                                                                        │
│  2. Unity Catalog checks: does this table (or parent) have policies?   │
│     └── catalog 'sales' has a column mask policy for pii_type tags     │
│                                                                        │
│  3. UC resolves governed tags on the table and columns                 │
│     └── email column tagged: pii_type=email                            │
│     └── ssn column tagged: pii_type=ssn                                │
│                                                                        │
│  4. UC evaluates the policy UDF for each tagged column                 │
│     └── mask_by_pii_type(email, 'email') → a***@company.com           │
│     └── mask_by_pii_type(ssn, 'ssn') → ***-**-6789                    │
│                                                                        │
│  5. Results returned with masks applied                                │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | What it is |
|-----------|------------|
| **Governed tag** | Account-level tag with enforced values (e.g., `pii_type` with values `ssn`, `email`, `phone`) |
| **Policy function (UDF)** | SQL function that implements the filter/mask logic |
| **Policy** | Binding that says "apply this UDF when this tag matches" at a given level |
| **Policy level** | Catalog, schema, or table — with downward inheritance |

---

## 4. Setting Up ABAC

### Step 1: Create Governed Tags

Governed tags are created at the account level by account admins or metastore admins (via UI or API). They define a fixed set of allowed values.

Example governed tags:
- `sensitivity`: values `public`, `internal`, `confidential`, `restricted`
- `pii_type`: values `ssn`, `email`, `phone`, `address`, `dob`, `financial`
- `compliance`: values `hipaa`, `gdpr`, `ccpa`, `sox`

### Step 2: Tag Your Data Assets

```sql
-- Tag at the table level for row filter policies
SET TAG ON TABLE healthcare.gold.patient_records `sensitivity` = `restricted`;
SET TAG ON TABLE healthcare.gold.patient_records `compliance` = `hipaa`;

-- Tag at the column level for column mask policies
SET TAG ON COLUMN healthcare.gold.patient_records.ssn `pii_type` = `ssn`;
SET TAG ON COLUMN healthcare.gold.patient_records.email `pii_type` = `email`;
SET TAG ON COLUMN healthcare.gold.patient_records.phone `pii_type` = `phone`;
SET TAG ON COLUMN healthcare.gold.patient_records.dob `pii_type` = `dob`;
```

### Step 3: Create Policy Functions

```sql
-- Column mask policy function: masks based on PII type tag
CREATE OR REPLACE FUNCTION governance.policies.mask_by_pii_type(
  col_value STRING,
  pii_type_tag STRING
)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii-authorized') THEN col_value
    WHEN pii_type_tag = 'ssn' THEN CONCAT('***-**-', RIGHT(col_value, 4))
    WHEN pii_type_tag = 'email' THEN CONCAT(LEFT(col_value, 1), '***@', SUBSTRING_INDEX(col_value, '@', -1))
    WHEN pii_type_tag = 'phone' THEN CONCAT('(***) ***-', RIGHT(REGEXP_REPLACE(col_value, '[^0-9]', ''), 4))
    WHEN pii_type_tag = 'address' THEN NULL
    WHEN pii_type_tag = 'dob' THEN NULL
    WHEN pii_type_tag = 'financial' THEN '***REDACTED***'
    ELSE col_value
  END;
```

### Step 4: Create and Apply Policies

ABAC policies are created via the Databricks UI (Catalog Explorer → Policies) or the REST API. The policy binds:
- A governed tag (e.g., `pii_type`)
- A policy function (e.g., `governance.policies.mask_by_pii_type`)
- A level (catalog, schema, or table)

> **Note:** As of DBR 16.4, ABAC policy creation is primarily done through the UI and API, not pure SQL DDL. The UDFs are SQL, but the policy binding is managed through the governance UI.

---

## 5. Row Filter Policies

Row filter ABAC policies restrict which rows are visible based on table-level tags.

### Example: Sensitivity-Based Row Filtering

```sql
-- Policy function: restrict rows based on table sensitivity tag
CREATE OR REPLACE FUNCTION governance.policies.sensitivity_row_filter(
  sensitivity_tag STRING
)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('data-admins') THEN TRUE
    WHEN sensitivity_tag = 'public' THEN TRUE
    WHEN sensitivity_tag = 'internal' THEN is_account_group_member('internal-users')
    WHEN sensitivity_tag = 'confidential' THEN is_account_group_member('confidential-authorized')
    WHEN sensitivity_tag = 'restricted' THEN is_account_group_member('restricted-authorized')
    ELSE FALSE
  END;
```

### Example: Region-Based Filtering via Tags

```sql
-- Tag tables with their region
SET TAG ON TABLE sales.gold.emea_revenue `data_region` = `emea`;
SET TAG ON TABLE sales.gold.us_revenue `data_region` = `us`;

-- Policy function checks user's group against table's region tag
CREATE OR REPLACE FUNCTION governance.policies.region_row_filter(
  region_tag STRING
)
RETURNS BOOLEAN
RETURN
  is_account_group_member('data-admins')
  OR is_account_group_member(CONCAT(region_tag, '-team'));
  -- emea-team can access tables tagged data_region=emea
  -- us-team can access tables tagged data_region=us
```

---

## 6. Column Mask Policies

Column mask ABAC policies apply masking functions to columns based on column-level tags.

### Example: Universal PII Masking Policy

The policy function from Step 3 above (`mask_by_pii_type`) can serve as a single universal masking policy. When applied at the catalog level:

- Every column tagged `pii_type=ssn` in the catalog gets SSN masking
- Every column tagged `pii_type=email` gets email masking
- New tables added with the same tags automatically get the same masks
- No per-table configuration needed

### Example: Financial Data Masking

```sql
CREATE OR REPLACE FUNCTION governance.policies.mask_financial(
  col_value DECIMAL(10,2),
  sensitivity_tag STRING
)
RETURNS DECIMAL(10,2)
RETURN
  CASE
    WHEN is_account_group_member('finance-authorized') THEN col_value
    WHEN sensitivity_tag = 'restricted' THEN NULL
    WHEN sensitivity_tag = 'confidential' THEN ROUND(col_value, -3)  -- round to nearest 1000
    ELSE col_value
  END;
```

---

## 7. Policy Hierarchy and Inheritance

Policies inherit downward through the catalog hierarchy:

```
CATALOG-level policy (broadest scope)
  └── Applies to ALL schemas and tables in the catalog
        SCHEMA-level policy
          └── Applies to ALL tables in the schema
                TABLE-level policy (narrowest scope)
                  └── Applies to this table only
```

### Conflict Resolution

- Only **one distinct row filter** or **column mask** can resolve per table/user combination
- If multiple policies at different levels could apply, the most specific (table-level) wins
- **ABAC policies from higher-level admins cannot be overridden by table owners** — this is the key security advantage over manual filters/masks

### Best Practice

Define policies at the **highest applicable level** (usually catalog) to maximize governance efficiency. Use lower-level policies only for exceptions.

```
┌─────────────────────────────────────────────────────────────┐
│  Recommended Policy Hierarchy                                │
│                                                               │
│  Catalog level:  Universal PII masking policy                │
│                  (masks all columns tagged pii_type=*)       │
│                                                               │
│  Schema level:   Financial data masking policy               │
│                  (additional rules for finance.gold.*)       │
│                                                               │
│  Table level:    Exception overrides only                    │
│                  (rare — use sparingly)                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. Limitations

| Limitation | Details |
|------------|---------|
| **Runtime** | Requires DBR 16.4+ or serverless compute |
| **Views** | Cannot apply ABAC directly to views (policies apply when querying underlying tables) |
| **One policy per dimension** | Only one row filter or column mask resolves per table/user |
| **Column conditions** | Max 3 column conditions in `MATCH COLUMNS` clause |
| **Time travel** | Not supported unless user is explicitly excluded from policy |
| **Cloning** | Deep/shallow clone not supported with active ABAC policies |
| **Vector search** | Cannot create indexes on ABAC-protected tables |
| **Information schema** | No INFORMATION_SCHEMA tables for ABAC policies (query via API) |
| **Tag inheritance** | Tags inherit to table level but NOT to column level — columns must be tagged explicitly |

---

## Next Steps

- Set up audit logging and compliance → [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md)
- Review RBAC fundamentals → [2-access-control-rbac.md](2-access-control-rbac.md)
