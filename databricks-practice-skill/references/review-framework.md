# Code Review Framework for Databricks

Use this framework when the user asks to review code or when running "Review It" exercises.

## The 7-Layer Review

Score each layer: ✅ Pass | ⚠️ Issues Found | ❌ Critical Problem

### Layer 1: Syntax & API Validity
- Does every function/method actually exist in the user's Spark/DBR version?
- Is the SQL dialect correct for Databricks SQL (not MySQL/Postgres)?
- Are deprecated APIs being used? (e.g., `dlt.read_stream` → `STREAM(LIVE.table)`)

**Common hallucinations to catch:**
| Fake API | Real Alternative |
|----------|-----------------|
| `dlt.read_stream("table")` | `STREAM(LIVE.table)` in SDP SQL |
| `spark.deltaTable("path")` | `DeltaTable.forPath(spark, "path")` |
| `df.writeStream.format("delta").save(path)` | `.toTable("catalog.schema.table")` |
| `spark.read.format("cloudFiles")` | `spark.readStream.format("cloudFiles")` (requires readStream) |

### Layer 2: Unity Catalog & Governance
- Three-level namespace: `catalog.schema.table` (not just `table_name`)
- GRANT statements for new tables/views
- No direct S3/ADLS writes bypassing UC governance
- Volumes for non-tabular files
- Service credentials instead of hardcoded keys

### Layer 3: Performance & Scalability
- No `.collect()` on unbounded data
- No Python UDFs where built-in functions exist
- Filters applied before joins
- Broadcast hint for small dimension tables (< 10MB)
- Appropriate shuffle partition count
- Liquid Clustering on frequently queried columns

### Layer 4: Data Correctness
- Join types correct (inner vs left — silent data loss?)
- NULL handling explicit (coalesce, isNull checks)
- Aggregation logic verified
- Date/timezone assumptions documented
- Type casting explicit (not relying on implicit conversion)

### Layer 5: Error Handling & Resilience
- Streaming: checkpoint locations set
- SDP: expectations with ON VIOLATION actions
- Jobs: retry policies configured
- Writes: atomic/idempotent (MERGE or partition overwrite)

### Layer 6: Security & Secrets
- No hardcoded passwords, tokens, API keys
- Secrets via `dbutils.secrets.get(scope, key)`
- No credentials in version control
- SQL injection prevention (parameterized queries)

### Layer 7: Naming, Style & Maintainability
- Meaningful names (not df1, df2, temp)
- Comments explain WHY, not WHAT
- Consistent naming conventions
- No dead code or unused imports

## Scoring Template

```
Code Review Results:
━━━━━━━━━━━━━━━━━━
1. Syntax & API:        [✅/⚠️/❌] — notes
2. UC & Governance:     [✅/⚠️/❌] — notes
3. Performance:         [✅/⚠️/❌] — notes
4. Data Correctness:    [✅/⚠️/❌] — notes
5. Error Handling:      [✅/⚠️/❌] — notes
6. Security:            [✅/⚠️/❌] — notes
7. Naming & Style:      [✅/⚠️/❌] — notes

Overall: X/7 layers passed
Priority fix: [most critical issue]
```
