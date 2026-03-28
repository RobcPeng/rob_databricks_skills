# Audit, Lineage & Compliance

Observe, track, and prove governance — audit logs for who accessed what, lineage for where data flows, and patterns for regulatory compliance.

## Table of Contents

1. [Audit Logging](#1-audit-logging)
2. [Querying Audit Logs](#2-querying-audit-logs)
3. [Data Lineage](#3-data-lineage)
4. [Compliance Patterns](#4-compliance-patterns)
5. [Delta Sharing Audit](#5-delta-sharing-audit)
6. [Security Monitoring](#6-security-monitoring)

---

## 1. Audit Logging

Unity Catalog captures a detailed audit trail of every action against your data assets in the `system.access.audit` system table.

### Audit Log Schema

| Column | Type | Description |
|--------|------|-------------|
| `account_id` | STRING | Databricks account ID |
| `workspace_id` | STRING | Workspace ID (`0` for account-level events) |
| `event_time` | TIMESTAMP | When the event occurred (UTC) |
| `event_date` | DATE | Calendar date of the event |
| `event_id` | STRING | Unique event identifier |
| `service_name` | STRING | Service that generated the event (e.g., `unityCatalog`, `notebook`, `clusters`) |
| `action_name` | STRING | Specific action (e.g., `getTable`, `createTable`, `generateTemporaryTableCredential`) |
| `user_identity` | STRUCT | User info: `email`, `subject_name` |
| `source_ip_address` | STRING | IP address of the request |
| `user_agent` | STRING | Client/tool that made the request |
| `request_params` | MAP<STRING, STRING> | Action-specific parameters (table name, schema, etc.) |
| `response` | STRUCT | Response status: `statusCode`, `errorMessage` |
| `audit_level` | STRING | `WORKSPACE_LEVEL` or `ACCOUNT_LEVEL` |
| `identity_metadata` | STRUCT | Run-by and run-as identities (for service principals) |
| `session_id` | STRING | Session identifier |

### Key Service Names

| Service | Events captured |
|---------|----------------|
| `unityCatalog` | Table/schema/catalog CRUD, grants, tag operations |
| `notebook` | Notebook execution, cell runs |
| `clusters` | Cluster create/start/stop/delete |
| `jobs` | Job create/run/update |
| `sqlPermissions` | Legacy Hive metastore permissions |
| `accounts` | Account-level admin actions |
| `secrets` | Secret scope and secret operations |

---

## 2. Querying Audit Logs

### Who Accessed a Specific Table?

```sql
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    source_ip_address,
    request_params['full_name_arg'] AS table_name
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND request_params['full_name_arg'] = 'sales.gold.customers'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### What Tables Did a User Access?

```sql
SELECT
    event_date,
    request_params['full_name_arg'] AS table_name,
    action_name,
    COUNT(*) AS access_count
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND user_identity.email = 'alice@company.com'
  AND action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY event_date, request_params['full_name_arg'], action_name
ORDER BY event_date DESC, access_count DESC;
```

### Who Changed Permissions?

```sql
SELECT
    event_time,
    user_identity.email AS admin_email,
    action_name,
    request_params['securable_type'] AS object_type,
    request_params['securable_full_name'] AS object_name,
    request_params['principal'] AS grantee,
    request_params['changes'] AS privilege_changes
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('updatePermissions')
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Failed Access Attempts (Potential Security Events)

```sql
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    source_ip_address,
    request_params['full_name_arg'] AS target_object,
    response.errorMessage AS error
FROM system.access.audit
WHERE response.statusCode >= 400
  AND service_name = 'unityCatalog'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### Tag Modification Events

```sql
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params['full_name_arg'] AS object_name,
    request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name LIKE '%Tag%'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Daily Access Summary Dashboard Query

```sql
SELECT
    event_date,
    service_name,
    action_name,
    COUNT(DISTINCT user_identity.email) AS unique_users,
    COUNT(*) AS event_count
FROM system.access.audit
WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY event_date, service_name, action_name
ORDER BY event_date DESC, event_count DESC;
```

### Top Data Consumers (Last 30 Days)

```sql
SELECT
    user_identity.email AS user_email,
    COUNT(DISTINCT request_params['full_name_arg']) AS tables_accessed,
    COUNT(*) AS total_events
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'generateTemporaryTableCredential'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY user_identity.email
ORDER BY tables_accessed DESC
LIMIT 25;
```

---

## 3. Data Lineage

Unity Catalog automatically captures runtime data lineage down to the column level — across notebooks, jobs, and dashboards.

### What Lineage Captures

| Dimension | Tracked |
|-----------|---------|
| Table-to-table dependencies | Which tables feed into which |
| Column-level lineage | Which source columns flow to which target columns |
| Notebook lineage | Which notebooks read/write which tables |
| Job lineage | Which jobs produce which tables |
| Model lineage | Which datasets trained which models |
| Dashboard lineage | Which tables power which dashboards |

### Viewing Lineage

- **Catalog Explorer UI:** Navigate to any table → Lineage tab → see upstream/downstream dependencies
- **Lineage is near real-time:** Updates as queries and jobs run
- **Languages:** Captured across Python, SQL, Scala, and R

### Querying Lineage via System Tables

```sql
-- Table dependencies: which tables does this table depend on?
SELECT *
FROM system.access.table_lineage
WHERE target_table_full_name = 'sales.gold.revenue'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS;

-- Column-level lineage
SELECT *
FROM system.access.column_lineage
WHERE target_table_full_name = 'sales.gold.revenue'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS;
```

### Lineage Use Cases

| Use case | How lineage helps |
|----------|-------------------|
| **Impact analysis** | Before changing a table, see all downstream consumers |
| **Root cause analysis** | Trace bad data back to its source |
| **Compliance audit** | Prove where PII flows for GDPR/CCPA/HIPAA |
| **Data quality** | Understand the full transformation chain |
| **Migration planning** | See all dependencies before moving or deprecating a table |

---

## 4. Compliance Patterns

### GDPR (General Data Protection Regulation)

| Requirement | Databricks implementation |
|-------------|--------------------------|
| **Right to access** | Query audit logs to show what data exists for a user |
| **Right to erasure** | Delta Lake `DELETE` + `VACUUM` to permanently remove PII |
| **Right to portability** | Export user data via `SELECT ... INTO` or Volume files |
| **Data minimization** | Column masks to hide PII from non-authorized users |
| **Consent tracking** | Tag tables/columns with consent basis |
| **Breach notification** | Audit logs for access forensics |
| **Lineage documentation** | Column-level lineage proves data flow |

```sql
-- GDPR: Find all data for a specific user across tables
-- (Requires knowledge of which tables contain user PII)
SELECT 'customers' AS source, * FROM sales.gold.customers
WHERE email = 'user@example.com'
UNION ALL
SELECT 'orders' AS source, customer_email, order_id, order_date, NULL, NULL
FROM sales.gold.orders
WHERE customer_email = 'user@example.com';

-- GDPR: Delete user data (right to erasure)
DELETE FROM sales.gold.customers WHERE email = 'user@example.com';
DELETE FROM sales.gold.orders WHERE customer_email = 'user@example.com';

-- Then vacuum to physically remove from storage
VACUUM sales.gold.customers RETAIN 0 HOURS;
VACUUM sales.gold.orders RETAIN 0 HOURS;
```

> **Warning:** `VACUUM RETAIN 0 HOURS` requires `spark.databricks.delta.retentionDurationCheck.enabled = false`. This permanently removes data — use with extreme caution.

### HIPAA (Health Insurance Portability and Accountability Act)

| Requirement | Databricks implementation |
|-------------|--------------------------|
| **Access controls** | RBAC with least-privilege groups for PHI |
| **Audit trail** | system.access.audit with 6+ year retention |
| **Encryption** | Customer-managed keys (CMK) for storage + managed services |
| **Minimum necessary** | Column masks on PHI columns; row filters by care team |
| **BAA** | Databricks offers BAA for HIPAA-covered workloads |

```sql
-- Tag PHI columns for HIPAA compliance
SET TAG ON TABLE healthcare.gold.patients `compliance` = `hipaa`;
SET TAG ON COLUMN healthcare.gold.patients.ssn `pii_type` = `ssn`;
SET TAG ON COLUMN healthcare.gold.patients.diagnosis `phi_type` = `medical`;
SET TAG ON COLUMN healthcare.gold.patients.medications `phi_type` = `medical`;

-- Apply masks — only care team sees PHI
ALTER TABLE healthcare.gold.patients
ALTER COLUMN diagnosis
SET MASK governance.policies.mask_phi;

ALTER TABLE healthcare.gold.patients
ALTER COLUMN medications
SET MASK governance.policies.mask_phi;
```

### SOX (Sarbanes-Oxley)

| Requirement | Databricks implementation |
|-------------|--------------------------|
| **Financial data integrity** | Delta Lake ACID transactions + time travel |
| **Access controls** | Strict RBAC on financial catalogs |
| **Audit trail** | Audit logs capture every access and modification |
| **Change management** | Git-based notebooks + DAB deployments |
| **Segregation of duties** | Separate service principals for ETL vs reporting |

### CCPA (California Consumer Privacy Act)

Similar to GDPR with California-specific requirements:
- Right to know what data is collected
- Right to delete personal information
- Right to opt-out of data sales
- Tag tables with `compliance=ccpa` and use audit logs to prove compliance

---

## 5. Delta Sharing Audit

When sharing data via Delta Sharing, additional audit events are captured:

```sql
-- Monitor Delta Sharing access
SELECT
    event_time,
    user_identity.email AS recipient,
    action_name,
    request_params['share'] AS share_name,
    request_params['recipient_name'] AS recipient_name,
    request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name LIKE '%Share%'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Tracked Delta Sharing Events

- Share creation, modification, deletion
- Recipient creation, activation, credential rotation
- Data access by recipients
- Share/recipient permission changes

---

## 6. Security Monitoring

### Suspicious Activity Detection

```sql
-- Unusual access patterns: users accessing tables they've never accessed before
WITH user_table_history AS (
    SELECT
        user_identity.email AS user_email,
        request_params['full_name_arg'] AS table_name,
        MIN(event_date) AS first_access
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND action_name = 'generateTemporaryTableCredential'
      AND event_date >= CURRENT_DATE() - INTERVAL 90 DAYS
    GROUP BY user_identity.email, request_params['full_name_arg']
)
SELECT *
FROM user_table_history
WHERE first_access >= CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY first_access DESC;
```

### Off-Hours Access

```sql
-- Access outside business hours (adjust timezone as needed)
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params['full_name_arg'] AS table_name,
    source_ip_address
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND (HOUR(event_time) < 6 OR HOUR(event_time) > 22)
ORDER BY event_time DESC;
```

### Permission Escalation Monitoring

```sql
-- Track all permission changes to sensitive catalogs
SELECT
    event_time,
    user_identity.email AS changed_by,
    request_params['securable_full_name'] AS object,
    request_params['changes'] AS changes
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
  AND (
    request_params['securable_full_name'] LIKE 'finance.%'
    OR request_params['securable_full_name'] LIKE 'healthcare.%'
    OR request_params['securable_full_name'] LIKE 'hr.%'
  )
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Governance Health Dashboard Queries

```sql
-- Untagged tables (potential governance gaps)
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.table_tags tt
    ON t.table_catalog = tt.catalog_name
    AND t.table_schema = tt.schema_name
    AND t.table_name = tt.table_name
    AND tt.tag_name = 'sensitivity'
WHERE tt.tag_value IS NULL
  AND t.table_catalog NOT IN ('system', 'information_schema')
  AND t.table_schema NOT IN ('information_schema', 'default')
ORDER BY t.table_catalog, t.table_schema, t.table_name;
```

```sql
-- Tables with PII columns but no masking
-- (Compare tagged PII columns against tables with active masks)
SELECT
    ct.catalog_name,
    ct.schema_name,
    ct.table_name,
    ct.column_name,
    ct.tag_value AS pii_type
FROM system.information_schema.column_tags ct
WHERE ct.tag_name = 'pii_type'
ORDER BY ct.catalog_name, ct.schema_name, ct.table_name;
-- Cross-reference with active masks via DESCRIBE TABLE EXTENDED
-- to identify unprotected PII columns
```

---

## Related Skills

- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — system tables deep dive, volume operations
- **[databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md)** — build audit dashboards
- **[databricks-bundles](../databricks-bundles/SKILL.md)** — deploy governance config as code
