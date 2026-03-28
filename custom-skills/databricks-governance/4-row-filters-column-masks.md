# Row Filters & Column Masks

Fine-grained data protection at the table level — restrict which rows users see and mask sensitive column values using SQL UDFs.

## Table of Contents

1. [Overview](#1-overview)
2. [Row Filters](#2-row-filters)
3. [Column Masks](#3-column-masks)
4. [PII Masking Recipes](#4-pii-masking-recipes)
5. [Dynamic Views Alternative](#5-dynamic-views-alternative)
6. [Limitations](#6-limitations)

---

## 1. Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Row Filters vs Column Masks                                     │
│                                                                   │
│  ROW FILTER:  "Can this user see this row at all?"               │
│  ┌───────────────────┐    ┌───────────────────┐                  │
│  │ Alice (EMEA team) │    │ Bob (US team)     │                  │
│  │ sees: region=EMEA │    │ sees: region=US   │                  │
│  └───────────────────┘    └───────────────────┘                  │
│                                                                   │
│  COLUMN MASK: "Can this user see this column's real value?"      │
│  ┌───────────────────┐    ┌───────────────────┐                  │
│  │ HR team           │    │ General user      │                  │
│  │ sees: 123-45-6789 │    │ sees: ***-**-6789 │                  │
│  └───────────────────┘    └───────────────────┘                  │
│                                                                   │
│  Both are SQL UDFs evaluated at query time.                      │
│  Original table stays unchanged — filters/masks are overlays.    │
└─────────────────────────────────────────────────────────────────┘
```

**Requirements:**
- Databricks Runtime 12.2 LTS+ or serverless compute
- UDFs must be registered in Unity Catalog
- Table owner or `MANAGE` privilege to apply

---

## 2. Row Filters

A row filter is a SQL UDF that takes the table's columns as arguments and returns a BOOLEAN. Only rows where the function returns TRUE are visible.

### Step 1: Create the Filter Function

```sql
-- Filter: users only see rows for their own region
CREATE OR REPLACE FUNCTION sales.utilities.region_filter(region_col STRING)
RETURNS BOOLEAN
RETURN
  -- Admins see everything
  is_account_group_member('data-admins')
  OR
  -- EMEA team sees EMEA rows
  (is_account_group_member('emea-team') AND region_col = 'EMEA')
  OR
  -- US team sees US rows
  (is_account_group_member('us-team') AND region_col = 'US')
  OR
  -- APAC team sees APAC rows
  (is_account_group_member('apac-team') AND region_col = 'APAC');
```

### Step 2: Apply to Table

```sql
-- Apply the row filter
ALTER TABLE sales.gold.revenue
SET ROW FILTER sales.utilities.region_filter ON (region);
```

### Step 3: Verify

```sql
-- As a member of 'emea-team', this returns only EMEA rows
SELECT * FROM sales.gold.revenue;

-- Admins still see all rows
```

### Remove a Row Filter

```sql
ALTER TABLE sales.gold.revenue DROP ROW FILTER;
```

### Row Filter Using current_user()

```sql
-- Each row has an owner_email column — users see only their own data
CREATE OR REPLACE FUNCTION sales.utilities.owner_filter(owner_email STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('data-admins')
  OR owner_email = current_user();

ALTER TABLE sales.gold.deals
SET ROW FILTER sales.utilities.owner_filter ON (owner_email);
```

### Row Filter with Multiple Columns

```sql
-- Filter on both region and department
CREATE OR REPLACE FUNCTION hr.utilities.dept_region_filter(
  dept STRING,
  region STRING
)
RETURNS BOOLEAN
RETURN
  is_account_group_member('hr-admins')
  OR (
    is_account_group_member('hr-us') AND region = 'US'
  )
  OR (
    is_account_group_member('hr-emea') AND region = 'EMEA'
  );

ALTER TABLE hr.gold.employees
SET ROW FILTER hr.utilities.dept_region_filter ON (department, region);
```

---

## 3. Column Masks

A column mask is a SQL UDF that takes the column value (and optionally other columns) as input and returns the same data type — either the real value or a masked version.

### Step 1: Create the Mask Function

```sql
-- Mask SSN: show full value to HR, last 4 digits to others
CREATE OR REPLACE FUNCTION hr.utilities.mask_ssn(ssn_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('hr-authorized') THEN ssn_val
    ELSE CONCAT('***-**-', RIGHT(ssn_val, 4))
  END;
```

### Step 2: Apply to Column

```sql
ALTER TABLE hr.gold.employees
ALTER COLUMN ssn
SET MASK hr.utilities.mask_ssn;
```

### Step 3: Verify

```sql
-- HR team sees: 123-45-6789
-- Everyone else sees: ***-**-6789
SELECT employee_name, ssn FROM hr.gold.employees;
```

### Remove a Column Mask

```sql
ALTER TABLE hr.gold.employees ALTER COLUMN ssn DROP MASK;
```

### Mask with Additional Context Columns

```sql
-- Mask salary — show to managers of the same department only
CREATE OR REPLACE FUNCTION hr.utilities.mask_salary(
  salary_val DECIMAL(10,2),
  dept STRING
)
RETURNS DECIMAL(10,2)
RETURN
  CASE
    WHEN is_account_group_member('hr-admins') THEN salary_val
    WHEN is_account_group_member('managers') AND
         is_account_group_member(CONCAT(dept, '-managers')) THEN salary_val
    ELSE NULL
  END;

ALTER TABLE hr.gold.employees
ALTER COLUMN salary
SET MASK hr.utilities.mask_salary USING COLUMNS (department);
```

> **`USING COLUMNS`** lets the mask function reference other columns in the same row for context-aware masking.

---

## 4. PII Masking Recipes

### Email Masking

```sql
CREATE OR REPLACE FUNCTION utilities.mask_email(email_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii-authorized') THEN email_val
    ELSE CONCAT(
      LEFT(email_val, 1),
      '***@',
      SUBSTRING_INDEX(email_val, '@', -1)
    )
  END;
-- alice@company.com → a***@company.com
```

### Phone Number Masking

```sql
CREATE OR REPLACE FUNCTION utilities.mask_phone(phone_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii-authorized') THEN phone_val
    ELSE CONCAT('(***) ***-', RIGHT(REGEXP_REPLACE(phone_val, '[^0-9]', ''), 4))
  END;
-- (303) 555-1234 → (***) ***-1234
```

### Credit Card Masking

```sql
CREATE OR REPLACE FUNCTION utilities.mask_credit_card(cc_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('finance-authorized') THEN cc_val
    ELSE CONCAT('****-****-****-', RIGHT(REGEXP_REPLACE(cc_val, '[^0-9]', ''), 4))
  END;
-- 4111-2222-3333-4444 → ****-****-****-4444
```

### Date of Birth → Age Range

```sql
CREATE OR REPLACE FUNCTION utilities.mask_dob(dob_val DATE)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii-authorized') THEN CAST(dob_val AS STRING)
    ELSE CONCAT(
      CAST(FLOOR(DATEDIFF(CURRENT_DATE(), dob_val) / 365.25 / 10) * 10 AS STRING),
      's'
    )
  END;
-- 1985-03-15 → 30s (for someone in their late 30s)
```

### Full Redaction (NULL)

```sql
CREATE OR REPLACE FUNCTION utilities.redact_column(val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('pii-authorized') THEN val
    ELSE NULL
  END;
```

### Apply Multiple Masks to a Table

```sql
-- Apply all PII masks to the customers table
ALTER TABLE sales.gold.customers ALTER COLUMN email SET MASK utilities.mask_email;
ALTER TABLE sales.gold.customers ALTER COLUMN phone SET MASK utilities.mask_phone;
ALTER TABLE sales.gold.customers ALTER COLUMN ssn SET MASK hr.utilities.mask_ssn;
ALTER TABLE sales.gold.customers ALTER COLUMN credit_card SET MASK utilities.mask_credit_card;
ALTER TABLE sales.gold.customers ALTER COLUMN date_of_birth SET MASK utilities.mask_dob;
```

---

## 5. Dynamic Views Alternative

Dynamic views are read-only SQL views that embed access logic directly. They're an alternative to row filters and column masks — especially useful when:
- You need complex multi-table join logic
- You want to share via Delta Sharing (filters/masks can't be shared)
- You need to support time travel on the underlying table

```sql
-- Dynamic view: region-filtered revenue
CREATE OR REPLACE VIEW sales.gold.revenue_filtered AS
SELECT *
FROM sales.gold.revenue
WHERE
  is_account_group_member('data-admins')
  OR (is_account_group_member('emea-team') AND region = 'EMEA')
  OR (is_account_group_member('us-team') AND region = 'US');

-- Dynamic view: masked PII
CREATE OR REPLACE VIEW sales.gold.customers_safe AS
SELECT
  customer_id,
  first_name,
  CASE
    WHEN is_account_group_member('pii-authorized') THEN email
    ELSE CONCAT(LEFT(email, 1), '***@', SUBSTRING_INDEX(email, '@', -1))
  END AS email,
  CASE
    WHEN is_account_group_member('pii-authorized') THEN ssn
    ELSE CONCAT('***-**-', RIGHT(ssn, 4))
  END AS ssn,
  city,
  state
FROM sales.gold.customers;
```

### When to Use What

| Feature | Row Filters / Column Masks | Dynamic Views |
|---------|---------------------------|---------------|
| Transparency | Users query original table name | Users must know to use the view |
| Delta Sharing | Cannot share filtered tables | Can share views |
| Time travel | Not supported with filters/masks | Supported on underlying table |
| Complexity | Simple per-table UDFs | Can embed joins, aggregations |
| ABAC integration | Yes (tag-driven policies) | No |
| Maintenance | One function per pattern | One view per table |

---

## 6. Limitations

- **Views:** Cannot apply row filters or column masks to views (use dynamic views instead)
- **Iceberg REST / Unity REST APIs:** Incompatible
- **Delta Sharing:** Providers cannot share tables with active filters/masks
- **Path-based access:** Filters/masks only apply to name-based access (SQL, DataFrame API)
- **Time travel:** Not supported on tables with active filters/masks
- **MERGE statements:** May have restrictions with complex filter/mask logic
- **Cloning:** DEEP CLONE and SHALLOW CLONE not supported with active filters
- **Vector search:** Cannot create indexes on filtered/masked tables
- **Performance:** Keep filter/mask UDFs simple — prefer SQL over Python UDFs, minimize column references

### Performance Tips

- Use simple CASE expressions, not complex joins within UDFs
- Use `is_account_group_member()` (cached) over custom lookup tables
- Limit the number of `USING COLUMNS` references
- Prefer deterministic expressions (e.g., `try_divide` instead of division that could error)
- Test with realistic query workloads to measure impact

---

## Next Steps

- Scale governance with ABAC policies → [5-abac-policies.md](5-abac-policies.md)
- Audit data access → [6-audit-lineage-compliance.md](6-audit-lineage-compliance.md)
