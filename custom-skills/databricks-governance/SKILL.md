---
name: databricks-governance
description: >
  Comprehensive guide to Databricks data governance with Unity Catalog — access control (RBAC/ABAC),
  tagging, classification, column masking, row filters, audit logging, lineage, and compliance.
  Use when working with permissions, privileges, grants, data protection, PII handling, dynamic
  masking, row-level security, governed tags, audit logs, data quality, or compliance frameworks.
  Triggers on: 'governance', 'access control', 'RBAC', 'ABAC', 'permissions', 'privileges',
  'GRANT', 'REVOKE', 'tagging', 'classification', 'column mask', 'row filter', 'masking',
  'PII', 'data protection', 'audit log', 'lineage', 'compliance', 'GDPR', 'CCPA', 'HIPAA',
  'row-level security', 'column-level security', 'fine-grained access', 'data quality',
  'ownership', 'service principal', 'governed tags', 'system tables', 'Delta Sharing audit',
  'is_account_group_member', 'current_user', 'dynamic view', 'security', 'data classification'.
---

# Databricks Governance with Unity Catalog

A comprehensive guide to governing data and AI assets on Databricks — from catalog design and access control to fine-grained masking, tagging, audit, and compliance.

## How to Use This Skill

**New to governance:** Start with file 1 (foundations) for the mental model, then file 2 (RBAC).

**Setting up access control:** Go to [2-access-control-rbac.md](2-access-control-rbac.md) for privileges and grants.

**Classifying and tagging data:** See [3-tagging-classification.md](3-tagging-classification.md).

**Masking PII or filtering rows:** Jump to [4-row-filters-column-masks.md](4-row-filters-column-masks.md).

**Scaling governance with ABAC:** See [5-abac-policies.md](5-abac-policies.md).

**Audit, lineage, compliance:** Go to [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md).

## Routing Table

| User intent | Start here |
|-------------|------------|
| "Set up governance for my org" | [1-governance-foundations.md](1-governance-foundations.md) |
| "Grant access to a table/schema/catalog" | [2-access-control-rbac.md](2-access-control-rbac.md) |
| "What privileges exist?" | [2-access-control-rbac.md](2-access-control-rbac.md) |
| "Tag data as PII / classify columns" | [3-tagging-classification.md](3-tagging-classification.md) |
| "Mask SSN / email / phone columns" | [4-row-filters-column-masks.md](4-row-filters-column-masks.md) |
| "Filter rows by user group" | [4-row-filters-column-masks.md](4-row-filters-column-masks.md) |
| "Set up tag-based access policies" | [5-abac-policies.md](5-abac-policies.md) |
| "ABAC vs manual masks — which one?" | [5-abac-policies.md](5-abac-policies.md) |
| "Query audit logs" | [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md) |
| "Track data lineage" | [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md) |
| "GDPR / HIPAA compliance patterns" | [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md) |

## Quick Reference: Governance Layers

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Unity Catalog Governance Stack                                         │
│                                                                         │
│  Layer 6: Audit & Compliance    ── system.access.audit, lineage        │
│  Layer 5: ABAC Policies         ── tag-driven, centralized, scalable   │
│  Layer 4: Row Filters & Masks   ── per-table UDF-based data protection │
│  Layer 3: Tags & Classification ── governed tags, PII labels, metadata │
│  Layer 2: RBAC (Privileges)     ── GRANT/REVOKE, ownership, groups     │
│  Layer 1: Catalog Structure     ── metastore → catalog → schema → obj  │
│                                                                         │
│  KEY: Each layer builds on the ones below it.                           │
│  Start at Layer 1, add layers as governance requirements grow.          │
└─────────────────────────────────────────────────────────────────────────┘
```

## MCP Tool Mapping

| Governance action | MCP tool |
|-------------------|----------|
| Run GRANT/REVOKE SQL | `execute_sql` |
| Query audit logs | `execute_sql` |
| Manage UC objects (catalogs, schemas) | `manage_uc_objects` |
| Manage grants programmatically | `manage_uc_grants` |
| Manage tags | `manage_uc_tags` |
| Manage security policies | `manage_uc_security_policies` |
| View lineage | `manage_uc_objects` |

## Related Skills

- **[databricks-unity-catalog](../databricks-unity-catalog/SKILL.md)** — system tables, volumes, file operations
- **[databricks-bundles](../databricks-bundles/SKILL.md)** — deploying governance config via DABs
- **[databricks-python-sdk](../databricks-python-sdk/SKILL.md)** — programmatic governance via SDK
