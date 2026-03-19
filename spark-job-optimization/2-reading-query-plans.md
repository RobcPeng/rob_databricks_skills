# Reading Spark Query Plans

A deep-dive reference for interpreting `explain()` output, understanding logical and physical plans, and using query plans to drive optimization decisions. This file covers every plan mode, every common operator, and shows real before/after optimization examples.

> **Key Takeaway:** The query plan is your single most powerful diagnostic tool. Before tuning configs or resizing clusters, read the plan first. It tells you exactly what Spark intends to do.

---

## Table of Contents

1. [Understanding explain() Modes](#1-understanding-explain-modes)
2. [Logical Plan](#2-logical-plan)
3. [Physical Plan](#3-physical-plan)
4. [Reading Plan Output Like a Pro](#4-reading-plan-output-like-a-pro)
5. [Before/After Optimization Examples](#5-beforeafter-optimization-examples)
6. [Catalyst Optimization Rules in Practice](#6-catalyst-optimization-rules-in-practice)
7. [SQL Tab Query Plans](#7-sql-tab-query-plans)

---

## 1. Understanding explain() Modes

Spark provides several modes for `explain()`. Each shows a different level of detail about how your query will execute.

### Sample Query Used Throughout

```python
from pyspark.sql import functions as F

orders = spark.table("sales.orders")          # partitioned by order_date
customers = spark.table("sales.customers")    # ~50K rows, small dimension table

result = (
    orders
    .filter(F.col("order_date") >= "2025-01-01")
    .join(customers, "customer_id")
    .groupBy("region")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy(F.desc("total_amount"))
)
```

---

### explain(True) / explain("extended")

These are equivalent. Shows the full plan lifecycle: parsed logical plan, analyzed logical plan, optimized logical plan, and physical plan.

```python
result.explain(True)
# or equivalently:
result.explain("extended")
```

**Output:**

```
== Parsed Logical Plan ==
'Sort ['total_amount DESC NULLS LAST], true
+- 'Aggregate ['region], ['region, 'sum('amount) AS total_amount#45]
   +- 'Join Inner, ('customer_id = 'customer_id)
      :- 'Filter ('order_date >= 2025-01-01)
      :  +- 'UnresolvedRelation [sales, orders]
      +- 'UnresolvedRelation [sales, customers]

== Analyzed Logical Plan ==
region: string, total_amount: decimal(20,2)
Sort [total_amount#45 DESC NULLS LAST], true
+- Aggregate [region#12], [region#12, sum(amount#8) AS total_amount#45]
   +- Join Inner, (customer_id#3 = customer_id#20)
      :- Filter (order_date#5 >= 2025-01-01)
      :  +- SubqueryAlias sales.orders
      :     +- Relation sales.orders[order_id#1,customer_id#3,...] parquet
      +- SubqueryAlias sales.customers
         +- Relation sales.customers[customer_id#20,name#21,...] parquet

== Optimized Logical Plan ==
Sort [total_amount#45 DESC NULLS LAST], true
+- Aggregate [region#12], [region#12, sum(amount#8) AS total_amount#45]
   +- Project [amount#8, region#12]
      +- Join Inner, (customer_id#3 = customer_id#20)
         :- Project [customer_id#3, amount#8]
         :  +- Filter (isnotnull(customer_id#3) AND (order_date#5 >= 2025-01-01))
         :     +- Relation sales.orders[customer_id#3,amount#8,order_date#5] parquet
         +- Project [customer_id#20, region#12]
            +- Filter isnotnull(customer_id#20)
               +- Relation sales.customers[customer_id#20,region#12] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [total_amount#45 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(total_amount#45 DESC NULLS LAST, 200)
      +- HashAggregate(keys=[region#12], functions=[sum(amount#8)])
         +- Exchange hashpartitioning(region#12, 200)
            +- HashAggregate(keys=[region#12], functions=[partial_sum(amount#8)])
               +- Project [amount#8, region#12]
                  +- BroadcastHashJoin [customer_id#3], [customer_id#20], Inner, BuildRight
                     :- Project [customer_id#3, amount#8]
                     :  +- Filter (isnotnull(customer_id#3) AND (order_date#5 >= 2025-01-01))
                     :     +- FileScan parquet sales.orders[customer_id#3,amount#8,order_date#5]
                     :        Batched: true, DataFilters: [isnotnull(customer_id#3)],
                     :        PartitionFilters: [(order_date#5 >= 2025-01-01)],
                     :        PushedFilters: [IsNotNull(customer_id)],
                     :        ReadSchema: struct<customer_id:bigint,amount:decimal(10,2)>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(customer_id#20))
                        +- Filter isnotnull(customer_id#20)
                           +- FileScan parquet sales.customers[customer_id#20,region#12]
                              Batched: true, DataFilters: [isnotnull(customer_id#20)],
                              PushedFilters: [IsNotNull(customer_id)],
                              ReadSchema: struct<customer_id:bigint,region:string>
```

**When to use:** When you need the full picture -- seeing how the query evolves from parsed SQL/DataFrame code all the way to the physical execution strategy.

---

### explain("simple") / explain() (no arguments)

Shows only the physical plan. This is the default when you call `explain()` with no arguments.

```python
result.explain("simple")
# or equivalently:
result.explain()
```

**Output:**

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [total_amount#45 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(total_amount#45 DESC NULLS LAST, 200)
      +- HashAggregate(keys=[region#12], functions=[sum(amount#8)])
         +- Exchange hashpartitioning(region#12, 200)
            +- HashAggregate(keys=[region#12], functions=[partial_sum(amount#8)])
               +- Project [amount#8, region#12]
                  +- BroadcastHashJoin [customer_id#3], [customer_id#20], Inner, BuildRight
                     :- ...
                     +- ...
```

**When to use:** Quick check of the physical strategy when you already understand the logical plan.

---

### explain("codegen")

Shows the generated Java code that Spark compiles and runs via WholeStageCodegen. Useful for advanced debugging of codegen performance issues.

```python
result.explain("codegen")
```

**Output (abbreviated):**

```
Found 4 WholeStageCodegen subtrees.
== Subtree 1 / 4 (maxMethodCodeSize: 282; maxConstantPoolSize: 192) ==
Generated code:
/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ final class GeneratedIteratorForCodegenStage1 extends ...BufferedRowIterator {
/* 006 */   private Object[] references;
/* 007 */   private UnsafeRow result;
/* 008 */   private ...UnsafeRowWriter writer;
/* 009 */
/* 010 */   // Filter: (isnotnull(customer_id#3) AND (order_date#5 >= 2025-01-01))
/* 011 */   // Project: [customer_id#3, amount#8]
...
```

**When to use:** Rarely. Useful when you suspect codegen issues (e.g., methods exceeding the 8000-byte JVM limit causing fallback to interpreted execution). Also helpful when understanding why a particular operator is slower than expected.

---

### explain("cost")

Shows the optimized logical plan with statistics annotations (estimated row counts and sizes) that the cost-based optimizer (CBO) uses to make decisions.

```python
result.explain("cost")
```

**Output:**

```
== Optimized Logical Plan ==
Sort [total_amount#45 DESC NULLS LAST], true, Statistics(sizeInBytes=1200.0 B)
+- Aggregate [region#12], [region#12, sum(amount#8) AS total_amount#45],
   Statistics(sizeInBytes=1200.0 B)
   +- Project [amount#8, region#12], Statistics(sizeInBytes=450.6 MB)
      +- Join Inner, (customer_id#3 = customer_id#20),
         Statistics(sizeInBytes=667.5 MB)
         :- Project [customer_id#3, amount#8],
         :  Statistics(sizeInBytes=600.0 MB)
         :  +- Filter (isnotnull(customer_id#3) AND (order_date#5 >= 2025-01-01)),
         :     Statistics(sizeInBytes=800.0 MB)
         :     +- Relation sales.orders[...] parquet,
         :        Statistics(sizeInBytes=5.0 GB, rowCount=1.2E8)
         +- Project [customer_id#20, region#12],
            Statistics(sizeInBytes=1.2 MB)
            +- Filter isnotnull(customer_id#20),
               Statistics(sizeInBytes=2.0 MB)
               +- Relation sales.customers[...] parquet,
                  Statistics(sizeInBytes=2.0 MB, rowCount=5.0E4)
```

**When to use:** When you suspect the CBO is making wrong decisions (e.g., choosing SortMergeJoin when BroadcastHashJoin would be better because statistics are stale). Check if the `sizeInBytes` and `rowCount` estimates look reasonable. If they are wildly off, run `ANALYZE TABLE ... COMPUTE STATISTICS` to refresh them.

---

### explain("formatted") -- Recommended Default

The most readable mode. Separates the physical plan from the node details, and shows each operator's attributes in a clean table format.

```python
result.explain("formatted")
```

**Output:**

```
== Physical Plan ==
AdaptiveSparkPlan (12)
+- Sort (11)
   +- Exchange (10)
      +- HashAggregate (9)
         +- Exchange (8)
            +- HashAggregate (7)
               +- Project (6)
                  +- BroadcastHashJoin Inner BuildRight (5)
                     :- Project (2)
                     :  +- Filter (1)
                     :     +- Scan parquet sales.orders (0)
                     +- BroadcastExchange (4)
                        +- Filter (3)
                           +- Scan parquet sales.customers (0)


(0) Scan parquet sales.orders
Output [3]: [customer_id#3L, amount#8, order_date#5]
Batched: true
Location: PreparedDeltaFileIndex [dbfs:/user/hive/warehouse/sales.db/orders]
PartitionFilters: [(order_date#5 >= 2025-01-01)]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:bigint,amount:decimal(10,2)>

(1) Filter
Input [3]: [customer_id#3L, amount#8, order_date#5]
Condition : isnotnull(customer_id#3L)

(2) Project
Output [2]: [customer_id#3L, amount#8]

(3) Filter
Input [2]: [customer_id#20L, region#12]
Condition : isnotnull(customer_id#20L)

(4) BroadcastExchange
Input [2]: [customer_id#20L, region#12]

(5) BroadcastHashJoin
Left keys [1]: [customer_id#3L]
Right keys [1]: [customer_id#20L]
Join type: Inner
Join condition: None

(7) HashAggregate
Input [2]: [amount#8, region#12]
Keys [1]: [region#12]
Functions [1]: [partial_sum(amount#8)]

(8) Exchange
Input [3]: [region#12, sum#50, isEmpty#51]
Arguments: hashpartitioning(region#12, 200)

(9) HashAggregate
Input [3]: [region#12, sum#50, isEmpty#51]
Keys [1]: [region#12]
Functions [1]: [sum(amount#8)]
Aggregate Attributes [1]: [sum(amount#8)#45]
Results [2]: [region#12, sum(amount#8)#45 AS total_amount#45]

(10) Exchange
Input [2]: [region#12, total_amount#45]
Arguments: rangepartitioning(total_amount#45 DESC NULLS LAST, 200)

(11) Sort
Input [2]: [region#12, total_amount#45]
Sort order: [total_amount#45 DESC NULLS LAST]
```

> **Recommendation:** Use `explain("formatted")` as your default. It gives you the tree structure at a glance and then the detail for each node in isolation. This is the fastest way to spot problems.

> **Photon:** On Photon-enabled clusters, `explain("formatted")` output looks structurally the same but you may see `PhotonGroupingAgg` instead of `HashAggregate`, `PhotonBroadcastHashJoin` instead of `BroadcastHashJoin`, and `PhotonShuffleExchangeSink`/`PhotonShuffleExchangeSource` instead of `Exchange`. The plan semantics are identical -- the same optimization strategies apply.

---

## 2. Logical Plan

The logical plan describes *what* the query does, without specifying *how*. It passes through three stages before becoming a physical plan.

### Stage 1: Unresolved (Parsed) Logical Plan

Created immediately when you write a DataFrame transformation or SQL query. Column names and table names are raw strings -- nothing has been validated against the catalog.

```
'Sort ['total_amount DESC NULLS LAST], true
+- 'Aggregate ['region], ['region, 'sum('amount) AS total_amount#45]
   +- 'Join Inner, ('customer_id = 'customer_id)
      :- 'Filter ('order_date >= 2025-01-01)
      :  +- 'UnresolvedRelation [sales, orders]
      +- 'UnresolvedRelation [sales, customers]
```

Notice the single-quote prefix (`'region`, `'sum`, `'UnresolvedRelation`). This means these references are unresolved -- Spark hasn't yet checked if these tables and columns actually exist.

**What happens here:** The parser converts your code into an abstract syntax tree (AST). No validation occurs.

### Stage 2: Analyzed Logical Plan

The Analyzer resolves all references against the catalog (Unity Catalog, Hive Metastore, or temp views). Columns get unique IDs (e.g., `region#12`), data types are assigned, and ambiguous references are resolved.

```
region: string, total_amount: decimal(20,2)
Sort [total_amount#45 DESC NULLS LAST], true
+- Aggregate [region#12], [region#12, sum(amount#8) AS total_amount#45]
   +- Join Inner, (customer_id#3 = customer_id#20)
      :- Filter (order_date#5 >= 2025-01-01)
      :  +- SubqueryAlias sales.orders
      :     +- Relation sales.orders[order_id#1,customer_id#3,...] parquet
      +- SubqueryAlias sales.customers
         +- Relation sales.customers[customer_id#20,name#21,...] parquet
```

**What happens here:**
- `'UnresolvedRelation` becomes `Relation sales.orders[...] parquet` -- the table exists and its schema is known.
- `'region` becomes `region#12` (type: `string`) -- the column is resolved with a unique expression ID.
- `'sum('amount)` becomes `sum(amount#8)` -- the function is resolved and the argument type is validated.

If a column or table doesn't exist, the error is thrown at this stage.

### Stage 3: Optimized Logical Plan

The Catalyst optimizer applies rule-based and cost-based optimization. This is where significant transformations happen.

```
Sort [total_amount#45 DESC NULLS LAST], true
+- Aggregate [region#12], [region#12, sum(amount#8) AS total_amount#45]
   +- Project [amount#8, region#12]
      +- Join Inner, (customer_id#3 = customer_id#20)
         :- Project [customer_id#3, amount#8]
         :  +- Filter (isnotnull(customer_id#3) AND (order_date#5 >= 2025-01-01))
         :     +- Relation sales.orders[customer_id#3,amount#8,order_date#5] parquet
         +- Project [customer_id#20, region#12]
            +- Filter isnotnull(customer_id#20)
               +- Relation sales.customers[customer_id#20,region#12] parquet
```

**What changed from analyzed to optimized:**
- **Column pruning:** The `Relation` nodes now read only the columns actually needed (`customer_id`, `amount`, `order_date` from orders; `customer_id`, `region` from customers), not all columns.
- **Predicate pushdown:** The `isnotnull(customer_id)` filters were injected on both sides of the join (inner join requires non-null keys).
- **Project insertion:** `Project` nodes were added to drop unnecessary columns as early as possible, reducing data flowing through the plan.

### Reading the Tree Structure

Plans are printed as trees with indentation showing the parent-child relationship:

```
Parent Operator              <-- executes last (top of the tree = final output)
+- Child Operator            <-- executes before parent
   +- Grandchild Operator    <-- executes before child
```

For joins and other binary operators, the two branches are shown with `:- ` (left/first child) and `+- ` (right/second child):

```
Join Inner
:- Left Branch (build side or stream side depends on join type)
:  +- ...
+- Right Branch
   +- ...
```

> **Key Takeaway:** Read plans **bottom-up**. The leaves (scans) are where data enters the plan. The root (top) is where the final result is produced. Data flows upward.

### Common Logical Operators

| Operator | Meaning |
|----------|---------|
| `Project [col1, col2]` | Column selection / computed expressions (like SQL `SELECT`) |
| `Filter (condition)` | Row filtering (like SQL `WHERE`) |
| `Join Inner/Left/Right/...` | Join two datasets on a condition |
| `Aggregate [keys], [functions]` | Group-by aggregation |
| `Sort [expressions]` | Order the output |
| `Window [partition] [order] [frame]` | Window function evaluation |
| `Union` | Combine rows from two datasets (UNION ALL) |
| `Distinct` | Remove duplicate rows |
| `Limit n` | Take first n rows |
| `SubqueryAlias name` | Names a subquery or table reference |
| `Expand [projections]` | Generates rows for CUBE, ROLLUP, GROUPING SETS |

---

## 3. Physical Plan

The physical plan specifies the concrete execution strategy. Spark's planner takes the optimized logical plan and chooses specific algorithms and data movement patterns.

### Scan Operators

| Operator | Meaning |
|----------|---------|
| `FileScan parquet` | Reads Parquet files from disk/cloud storage. Shows `DataFilters`, `PartitionFilters`, `PushedFilters`, and `ReadSchema`. |
| `FileScan delta` | Reads Delta Lake table (Parquet under the hood, with transaction log metadata). |
| `FileScan csv` / `FileScan json` | Reads CSV or JSON files. No pushdown for these formats. |
| `InMemoryTableScan` | Reads from a cached/persisted DataFrame. Shows `StorageLevel` used. |
| `ExternalRDDScanExec` | Reads from an RDD (usually means you're mixing RDD and DataFrame APIs). |

**Reading FileScan details:**

```
FileScan parquet sales.orders[customer_id#3,amount#8,order_date#5]
  Batched: true                                           <-- columnar batch reading enabled
  DataFilters: [isnotnull(customer_id#3)]                 <-- filters applied during scan (after partition pruning)
  PartitionFilters: [(order_date#5 >= 2025-01-01)]        <-- filters on partition columns (pruning entire directories)
  PushedFilters: [IsNotNull(customer_id)]                 <-- filters pushed to the data source (Parquet reader)
  ReadSchema: struct<customer_id:bigint,amount:decimal>   <-- only these columns are read from files
```

> **Key Takeaway:** If a filter column is in `PartitionFilters`, Spark skips entire directories. If it's only in `DataFilters`, Spark still opens every file but filters rows during scan. Check that your most selective filters appear in `PartitionFilters` when possible.

> **Photon:** On Photon clusters, the scan operator may appear as `PhotonScan` or `PhotonGroupScan`. Photon reads Parquet files using native C++ code with SIMD vectorization, which is significantly faster for wide tables. The filter and projection pushdown semantics are the same.

---

### Join Operators

Spark has five physical join strategies, chosen based on data size, join type, and hints:

#### BroadcastHashJoin

```
BroadcastHashJoin [customer_id#3], [customer_id#20], Inner, BuildRight
:- [stream side - large table]
+- BroadcastExchange HashedRelationBroadcastMode(List(customer_id#20))
   +- [build side - small table, broadcast to all executors]
```

- **When chosen:** One side is smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10MB), or a broadcast hint is used.
- **Performance:** Fastest join. No shuffle on the stream (large) side. The small table is broadcast to every executor and built into a hash table in memory.
- **Supports:** All join types (inner, left, right, full outer, semi, anti).
- `BuildRight` / `BuildLeft` indicates which side is broadcast.

#### SortMergeJoin

```
SortMergeJoin [customer_id#3], [customer_id#20], Inner
:- Sort [customer_id#3 ASC], false, 0
:  +- Exchange hashpartitioning(customer_id#3, 200)
:     +- [left side]
+- Sort [customer_id#20 ASC], false, 0
   +- Exchange hashpartitioning(customer_id#20, 200)
      +- [right side]
```

- **When chosen:** Default for equi-joins when both sides are too large to broadcast.
- **Performance:** Requires shuffle + sort on both sides. Scales well to very large datasets.
- **Supports:** All join types.
- **Signature:** Always preceded by `Exchange` (shuffle) and `Sort` on both sides.

#### ShuffledHashJoin

```
ShuffledHashJoin [customer_id#3], [customer_id#20], Inner, BuildRight
:- Exchange hashpartitioning(customer_id#3, 200)
:  +- [left side]
+- Exchange hashpartitioning(customer_id#20, 200)
   +- [right side]
```

- **When chosen:** When `spark.sql.join.preferSortMergeJoin` is `false`, or the build side fits in memory per-partition. Less common.
- **Performance:** Requires shuffle but not sort. Faster than SortMergeJoin when one side is substantially smaller per-partition.
- **Supports:** All join types except full outer.

#### CartesianProduct

```
CartesianProduct
:- [left side]
+- [right side]
```

- **When chosen:** Non-equi joins (no equality condition) with inner join type, or a missing join condition.
- **Performance:** O(n * m) -- produces the cross product. Almost always a bug or a sign of a missing join condition.

> **Red Flag:** If you see `CartesianProduct` in your plan, stop and check your join conditions immediately. This is the number one cause of unexpectedly slow or OOM queries.

#### BroadcastNestedLoopJoin

```
BroadcastNestedLoopJoin BuildRight, Inner, (a.value > b.threshold)
:- [stream side]
+- BroadcastExchange IdentityBroadcastMode
   +- [build side - broadcast]
```

- **When chosen:** Non-equi joins when one side is small enough to broadcast.
- **Performance:** Better than CartesianProduct (only one side is broadcast), but still O(n * m) comparisons. Acceptable for small datasets or theta joins.
- **Supports:** All join types, any join condition.

---

### Shuffle (Exchange) Operators

Shuffles redistribute data across partitions. They are the most expensive operation in Spark (network I/O + disk I/O + serialization).

| Exchange Type | When Used | Example |
|---------------|-----------|---------|
| `hashpartitioning(col, N)` | Join or groupBy on `col` | `Exchange hashpartitioning(customer_id#3, 200)` |
| `rangepartitioning(col, N)` | `orderBy` / `sortBy` on `col` | `Exchange rangepartitioning(total#45 DESC, 200)` |
| `SinglePartition` | Global aggregation or `coalesce(1)` | `Exchange SinglePartition` |
| `RoundRobinPartitioning(N)` | `repartition(N)` without columns | `Exchange RoundRobinPartitioning(200)` |

```
Exchange hashpartitioning(region#12, 200)     <-- shuffle to 200 partitions by hash of region
+- HashAggregate(keys=[region#12], functions=[partial_sum(amount#8)])
```

> **Key Takeaway:** Count the `Exchange` nodes in your plan. Each one is a shuffle boundary (a stage boundary). Fewer shuffles = faster job. If you see the same column shuffled multiple times, consider restructuring your query or persisting an intermediate result.

> **Photon:** Photon replaces the JVM shuffle with a native C++ shuffle (`PhotonShuffleExchangeSink` / `PhotonShuffleExchangeSource`). It is typically 2-3x faster due to zero-copy serialization and columnar data transfer. However, the same optimization principles apply -- fewer shuffles is still better.

---

### Aggregation Operators

Spark performs aggregation in two phases (partial + final) to minimize data shuffled:

```
HashAggregate(keys=[region#12], functions=[sum(amount#8)])                   <-- FINAL agg
+- Exchange hashpartitioning(region#12, 200)                                 <-- shuffle
   +- HashAggregate(keys=[region#12], functions=[partial_sum(amount#8)])      <-- PARTIAL agg
```

| Operator | Meaning |
|----------|---------|
| `HashAggregate` | Hash-based aggregation. Fast, in-memory. Used for most built-in aggregate functions. Appears twice: partial (pre-shuffle) and final (post-shuffle). |
| `SortAggregate` | Sort-based aggregation. Used when aggregation expressions are not compatible with hash aggregation (e.g., certain complex types). Slower, requires sorted input. |
| `ObjectHashAggregate` | Used for typed aggregators (e.g., `collect_list`, `collect_set`, Pandas UDFs, custom aggregators). Cannot benefit from partial aggregation in the same way. |

> **Photon:** Photon replaces `HashAggregate` with `PhotonGroupingAgg`, which uses vectorized, SIMD-optimized aggregation. You will still see the two-phase pattern (partial + final) in the plan.

---

### Other Important Operators

#### WholeStageCodegen

```
*(1) HashAggregate(keys=[region#12], functions=[sum(amount#8)])
```

The `*(1)` prefix (or `WholeStageCodegen (1)` in formatted output) means this operator and its children have been fused into a single generated Java method. This eliminates virtual function call overhead between operators.

Multiple codegen stages are numbered: `*(1)`, `*(2)`, `*(3)`, etc. Stage boundaries occur at shuffle (Exchange) and broadcast points.

#### ColumnarToRow / RowToColumnar

```
ColumnarToRow
+- FileScan parquet ...
```

Converts between columnar batch format (used by Parquet reader) and row format (used by most Spark operators). If you see many of these, it may indicate format mismatches.

#### AdaptiveSparkPlan

```
AdaptiveSparkPlan isFinalPlan=false
+- ...
```

Wraps the entire plan when Adaptive Query Execution (AQE) is enabled. `isFinalPlan=false` means the plan may change at runtime based on actual shuffle statistics. After execution, you can call `explain()` again to see `isFinalPlan=true` with the actual plan that ran.

```python
# To see the final adaptive plan after execution:
result.collect()                  # force execution
result.explain("formatted")      # now shows isFinalPlan=true with actual strategies
```

#### Subquery / SubqueryBroadcast

```
Subquery subquery#123, [id=#456]
+- ...
```

Appears for scalar subqueries or `IN` clauses. The subquery is executed once and the result is broadcast.

---

## 4. Reading Plan Output Like a Pro

### Step-by-Step Walkthrough

Here is a complete plan from `explain("formatted")`. We will read it bottom-up, annotating every decision.

```python
# The query: top 10 products by revenue in Q1 2025, for customers in the US
top_products = (
    spark.table("sales.order_items").alias("oi")
    .join(spark.table("sales.orders").alias("o"), "order_id")
    .join(spark.table("sales.customers").alias("c"), "customer_id")
    .filter(F.col("o.order_date").between("2025-01-01", "2025-03-31"))
    .filter(F.col("c.country") == "US")
    .groupBy("oi.product_id")
    .agg(
        F.sum(F.col("oi.quantity") * F.col("oi.unit_price")).alias("revenue"),
        F.countDistinct("o.order_id").alias("order_count")
    )
    .orderBy(F.desc("revenue"))
    .limit(10)
)

top_products.explain("formatted")
```

**Plan output (annotated):**

```
== Physical Plan ==
AdaptiveSparkPlan (20)                                    # [20] AQE wraps everything
+- TakeOrderedAndProject (19)                             # [19] Combined sort+limit+project (optimization!)
   +- HashAggregate (18)                                  # [18] FINAL aggregation (post-shuffle)
      +- Exchange (17)                                    # [17] SHUFFLE by product_id for final agg
         +- HashAggregate (16)                            # [16] PARTIAL aggregation (pre-shuffle)
            +- Project (15)                               # [15] Compute revenue expression
               +- BroadcastHashJoin Inner BuildRight (14) # [14] JOIN order_items+orders WITH customers
                  :- Project (9)                          # [9]  Project after first join
                  :  +- SortMergeJoin Inner (8)           # [8]  JOIN order_items WITH orders
                  :     :- Sort (4)                       # [4]  Sort left side for SMJ
                  :     :  +- Exchange (3)                # [3]  SHUFFLE order_items by order_id
                  :     :     +- Filter (2)               # [2]  Filter nulls on order_id
                  :     :        +- Scan parquet (1)      # [1]  SCAN order_items table
                  :     +- Sort (7)                       # [7]  Sort right side for SMJ
                  :        +- Exchange (6)                # [6]  SHUFFLE orders by order_id
                  :           +- Filter (5)               # [5]  Filter: date range + not null
                  :              +- Scan parquet (0)      # [0]  SCAN orders table
                  +- BroadcastExchange (13)               # [13] BROADCAST customers table
                     +- Filter (12)                       # [12] Filter: country = 'US' + not null
                        +- Scan parquet (11)              # [11] SCAN customers table
```

**Reading bottom-up:**

1. **Nodes (0), (1), (11) -- Scans:** Three tables are read. Check `ReadSchema` to verify column pruning. Check `PartitionFilters` for partition pruning.

2. **Node (5) -- Filter on orders:** The date range filter `(order_date BETWEEN '2025-01-01' AND '2025-03-31')` is pushed down here, before the shuffle. If `order_date` is a partition column, it appears in `PartitionFilters` -- major win.

3. **Nodes (3), (6) -- Shuffles:** Both `order_items` and `orders` are shuffled by `order_id` for the SortMergeJoin. This is expected -- both tables are large.

4. **Nodes (4), (7) -- Sorts:** Required by SortMergeJoin. These run after the shuffle.

5. **Node (8) -- SortMergeJoin:** Joins `order_items` with `orders` on `order_id`. Both sides are too large for broadcast -- correct strategy.

6. **Node (12) -- Filter on customers:** `country = 'US'` is applied before broadcast -- filters reduce the broadcast payload.

7. **Node (13) -- BroadcastExchange:** The filtered `customers` table (US only) is small enough to broadcast. This avoids a third shuffle.

8. **Node (14) -- BroadcastHashJoin:** Joins the result of (order_items JOIN orders) with customers, using the broadcast. `BuildRight` means customers is the build side.

9. **Nodes (16), (17), (18) -- Two-phase aggregation:** Partial aggregation happens before the shuffle (node 16), reducing the data sent across the network. Final aggregation happens after (node 18).

10. **Node (19) -- TakeOrderedAndProject:** Catalyst merged `Sort` + `Limit 10` + `Project` into a single optimized operator. Instead of sorting all data and then taking 10, each partition keeps only its top-10, reducing final sort cost dramatically.

### Understanding the `*` Prefix (WholeStageCodegen)

In `explain()` (non-formatted) output, operators with a `*` prefix and a number in parentheses are fused together into a single codegen stage:

```
*(2) HashAggregate(keys=[product_id#5], functions=[sum(...)])
+- Exchange hashpartitioning(product_id#5, 200)            <-- no *, this is a shuffle boundary
   +- *(1) HashAggregate(keys=[product_id#5], functions=[partial_sum(...)])
      +- *(1) Project [...]
         +- *(1) BroadcastHashJoin [...]
            :- *(1) Project [...]
            :  +- *(1) Filter [...]
            :     +- *(1) FileScan parquet [...]
            +- BroadcastExchange [...]                     <-- no *, this is a broadcast boundary
               +- *(3) Filter [...]
                  +- *(3) FileScan parquet [...]
```

All operators with `*(1)` are compiled into a single Java method. Same for `*(2)` and `*(3)`. The boundaries are where data must materialize (shuffles, broadcasts).

> **Photon:** Photon does not use WholeStageCodegen. Instead, it uses vectorized interpretation with native C++ code. You will not see `*` prefixes on Photon-executed operators. This is expected and not a concern.

### Plan Node Metrics

After execution, `explain()` can show runtime metrics per node (if using `EXPLAIN ANALYZE` in SQL or examining the SQL tab):

```sql
EXPLAIN ANALYZE
SELECT region, sum(amount)
FROM sales.orders
WHERE order_date >= '2025-01-01'
GROUP BY region
```

Or from the Spark UI SQL tab, each node shows:

- **number of output rows:** Actual row count produced by this operator
- **data size:** Bytes produced
- **time:** Wall-clock time spent in this operator
- **spill size:** If the operator spilled to disk (memory pressure)

### Spotting Red Flags

| Red Flag | What to Look For | Fix |
|----------|------------------|-----|
| CartesianProduct | `CartesianProduct` in the plan | Add proper join conditions |
| BroadcastNestedLoopJoin | `BroadcastNestedLoopJoin` with large data | Convert to equi-join or add equality conditions |
| No partition pruning | `PartitionFilters: []` when you expect pruning | Filter on partition columns directly, avoid wrapping them in functions |
| No filter pushdown | `PushedFilters: []` with `DataFilters` containing your filter | Simplify filter expressions; avoid UDFs in filter conditions |
| Missing column pruning | `ReadSchema` includes columns not used downstream | Select only needed columns; check for `SELECT *` |
| Excessive shuffles | Many `Exchange` nodes (more than 2-3) | Restructure query; pre-aggregate; persist intermediate results |
| Redundant sorts | Multiple `Sort` nodes on the same column | Check for unnecessary `orderBy` in the middle of transformations |
| SinglePartition exchange | `Exchange SinglePartition` on large data | Avoid `coalesce(1)` or global aggregation on large datasets |
| ObjectHashAggregate | `ObjectHashAggregate` instead of `HashAggregate` | Replace `collect_list`/`collect_set` with alternatives if possible; replace Python UDFs with built-in functions |
| SortMergeJoin on small table | `SortMergeJoin` when one side is < 100MB | Add broadcast hint or increase `autoBroadcastJoinThreshold` |

---

## 5. Before/After Optimization Examples

### Example 1: Adding a Filter That Enables Partition Pruning

**Before (no partition pruning):**

```python
# BAD: wrapping partition column in a function prevents pruning
df = spark.table("sales.orders").filter(F.year("order_date") == 2025)
df.explain("formatted")
```

```
Scan parquet sales.orders
  PartitionFilters: []                              <-- NO partition pruning!
  DataFilters: [year(order_date#5) = 2025]          <-- filter applied row-by-row on ALL files
  ReadSchema: struct<order_id:bigint,...,order_date:date>
```

**After (partition pruning enabled):**

```python
# GOOD: direct comparison on partition column enables pruning
df = spark.table("sales.orders").filter(
    (F.col("order_date") >= "2025-01-01") &
    (F.col("order_date") < "2026-01-01")
)
df.explain("formatted")
```

```
Scan parquet sales.orders
  PartitionFilters: [(order_date#5 >= 2025-01-01), (order_date#5 < 2026-01-01)]  <-- PRUNING!
  DataFilters: []                                  <-- no row-level filter needed
  ReadSchema: struct<order_id:bigint,...>           <-- order_date not even in ReadSchema (it's a partition col)
```

> **Key Takeaway:** Never wrap partition columns in functions (`year()`, `month()`, `date_format()`). Use direct range comparisons instead. The difference can be reading 365 partitions vs 100,000+ partitions.

---

### Example 2: Replacing a UDF with Built-in Functions

**Before (Python UDF blocks optimization):**

```python
from pyspark.sql.types import StringType

@udf(StringType())
def categorize_amount(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    else:
        return "low"

df = (spark.table("sales.orders")
      .withColumn("category", categorize_amount("amount"))
      .filter(F.col("category") == "high")
      .select("order_id", "category", "amount"))
df.explain("formatted")
```

```
Project [order_id#1, category#50, amount#8]
+- Filter (category#50 = high)                     <-- filter AFTER UDF (can't push down)
   +- Project [order_id#1, amount#8, pythonUDF0#51 AS category#50]
      +- BatchEvalPython [categorize_amount(amount#8)], [pythonUDF0#51]    <-- PYTHON UDF!
         +- Scan parquet sales.orders
            ReadSchema: struct<order_id:bigint,amount:decimal(10,2)>
            PushedFilters: []                       <-- no pushdown possible
```

Notice `BatchEvalPython` -- this means data is serialized to Python, processed row-by-row, and serialized back. The filter on `category` cannot be pushed below the UDF.

**After (built-in functions, Catalyst optimizes everything):**

```python
df = (spark.table("sales.orders")
      .withColumn("category",
          F.when(F.col("amount") > 1000, "high")
           .when(F.col("amount") > 100, "medium")
           .otherwise("low"))
      .filter(F.col("category") == "high")
      .select("order_id", "category", "amount"))
df.explain("formatted")
```

```
Project [order_id#1, high AS category#50, amount#8]    <-- constant folding! category is always "high"
+- Filter (amount#8 > 1000)                             <-- Catalyst simplified: "high" means amount > 1000
   +- Scan parquet sales.orders
      PushedFilters: [GreaterThan(amount, 1000)]        <-- PUSHED DOWN to Parquet reader!
      ReadSchema: struct<order_id:bigint,amount:decimal(10,2)>
```

Catalyst recognized that filtering for `category == "high"` is equivalent to `amount > 1000`, eliminated the `CASE WHEN` entirely, and pushed the filter down to the Parquet reader.

> **Photon:** Photon accelerates built-in expressions with native vectorized execution but cannot accelerate Python UDFs at all. The performance gap between UDF and built-in is even larger on Photon clusters (often 10-50x).

---

### Example 3: Broadcast Hint to Convert SortMergeJoin to BroadcastHashJoin

**Before (SortMergeJoin with expensive double shuffle):**

```python
# dim_products is 200MB -- just above the 10MB auto-broadcast threshold
orders = spark.table("sales.order_items")       # 500M rows
products = spark.table("catalog.dim_products")  # 500K rows, 200MB

result = orders.join(products, "product_id")
result.explain("formatted")
```

```
SortMergeJoin [product_id#3], [product_id#30], Inner
:- Sort [product_id#3 ASC]
:  +- Exchange hashpartitioning(product_id#3, 200)       <-- shuffle 1: order_items (huge!)
:     +- Scan parquet sales.order_items
+- Sort [product_id#30 ASC]
   +- Exchange hashpartitioning(product_id#30, 200)      <-- shuffle 2: dim_products
      +- Scan parquet catalog.dim_products
```

Two shuffles, including one on the 500M-row table.

**After (broadcast hint eliminates both shuffles):**

```python
result = orders.join(F.broadcast(products), "product_id")
result.explain("formatted")
```

```
BroadcastHashJoin [product_id#3], [product_id#30], Inner, BuildRight
:- Scan parquet sales.order_items                        <-- NO shuffle on the big table!
   ReadSchema: struct<...>
+- BroadcastExchange HashedRelationBroadcastMode(List(product_id#30))
   +- Scan parquet catalog.dim_products                  <-- broadcast instead of shuffle
```

Zero shuffles on the large table. The 200MB dimension table is broadcast to all executors.

```python
# Alternatively, increase the threshold:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "250m")
# Now Spark will auto-broadcast tables up to 250MB without hints.
```

> **Key Takeaway:** A 200MB broadcast is almost always cheaper than shuffling a 500M-row table. The default 10MB threshold is conservative. For dimension tables up to ~500MB-1GB (depending on executor memory), broadcast joins are typically faster.

---

### Example 4: Column Pruning (Selecting Only Needed Columns)

**Before (selecting all columns):**

```python
# BAD: reading a 100-column wide table when you only need 3 columns
df = spark.table("analytics.user_events")  # 100 columns, 2B rows
result = df.groupBy("event_type").agg(F.count("*").alias("cnt"))
result.explain("formatted")
```

```
Scan parquet analytics.user_events
  ReadSchema: struct<event_type:string>             <-- Catalyst already pruned columns!
```

Actually, Catalyst handles this well -- it only reads the columns you reference. But where column pruning matters is in intermediate DataFrames:

```python
# BAD: persisting all columns when downstream only needs a few
all_events = spark.table("analytics.user_events")   # 100 columns
all_events.cache()                                   # caches ALL 100 columns
result = all_events.groupBy("event_type").count()
```

**After (explicit column selection before caching):**

```python
# GOOD: select needed columns before caching
events = spark.table("analytics.user_events").select("event_type", "user_id", "timestamp")
events.cache()                                       # caches only 3 columns -- much smaller!
result = events.groupBy("event_type").count()
```

The cache reduction can be 10-30x. For a 2B row table with 100 columns vs. 3 columns, you might go from 500GB cached to 15GB cached.

Also watch for unnecessary columns in joins:

```python
# BAD: join brings in all columns from both sides
result = orders.join(customers, "customer_id")  # carries all customer columns

# GOOD: select only what you need before or after the join
result = (orders
    .join(customers.select("customer_id", "region"), "customer_id"))
```

The plan difference shows fewer columns flowing through Exchange operators, reducing shuffle data volume.

---

### Example 5: Fixing a Cartesian Product with Proper Join Conditions

**Before (accidental Cartesian product):**

```python
# BAD: join condition references wrong column -- effectively no join condition
orders = spark.table("sales.orders")
returns = spark.table("sales.returns")

# Typo: 'order_id' vs 'return_order_id' -- this produces a cross join condition
result = orders.join(returns, orders.order_id == returns.order_id)
# If returns table doesn't have 'order_id' column, you might get an error
# But a subtler bug: joining on a always-true condition or non-equi condition:

result = orders.join(returns, orders["amount"] > returns["refund_amount"])
result.explain("formatted")
```

```
BroadcastNestedLoopJoin BuildRight, Inner, (amount#8 > refund_amount#40)
:- Scan parquet sales.orders
+- BroadcastExchange IdentityBroadcastMode
   +- Scan parquet sales.returns
```

If `returns` is large, this becomes:

```
CartesianProduct                                   <-- O(n*m) disaster
:- Scan parquet sales.orders                       <-- 10M rows
+- Scan parquet sales.returns                      <-- 1M rows = 10 TRILLION row pairs
```

**After (proper equi-join condition):**

```python
# GOOD: use the correct join key, then apply the inequality as a filter
result = (orders.join(returns, orders["order_id"] == returns["return_order_id"])
          .filter(orders["amount"] > returns["refund_amount"]))
result.explain("formatted")
```

```
Filter (amount#8 > refund_amount#40)
+- SortMergeJoin [order_id#1], [return_order_id#35], Inner
   :- Sort [order_id#1 ASC]
   :  +- Exchange hashpartitioning(order_id#1, 200)
   :     +- Scan parquet sales.orders
   +- Sort [return_order_id#35 ASC]
      +- Exchange hashpartitioning(return_order_id#35, 200)
         +- Scan parquet sales.returns
```

The equi-join condition enables SortMergeJoin (or BroadcastHashJoin if one side is small), and the inequality is applied as a post-join filter.

> **Key Takeaway:** Always include at least one equality condition in your joins. Move inequality conditions to a `filter()` after the join. The plan difference is CartesianProduct (hours or OOM) vs SortMergeJoin (seconds to minutes).

---

## 6. Catalyst Optimization Rules in Practice

The Catalyst optimizer applies dozens of rules in multiple passes. Here are the most impactful ones and how they appear in practice.

### Rules That Transform Your Query

| Rule | What It Does | Before/After in Plan |
|------|-------------|---------------------|
| **PredicatePushDown** | Moves filters closer to the scan (below joins and projections) | Filter moves from above Join to below it, or into FileScan `PushedFilters` |
| **ColumnPruning** | Removes columns not needed by any downstream operator | `ReadSchema` shows fewer columns; `Project` nodes appear to drop unused columns early |
| **ConstantFolding** | Evaluates constant expressions at plan time | `1 + 2` becomes `3`; `CASE WHEN true THEN 'x'` becomes `'x'` |
| **BooleanSimplification** | Simplifies boolean logic | `(a AND true)` becomes `a`; `(a OR false)` becomes `a` |
| **CombineFilters** | Merges adjacent Filter nodes | Two `Filter` nodes become one with `AND` |
| **CombineProjections** | Merges adjacent Project nodes | Reduces materialization overhead |
| **PushDownPredicatesThoughJoin** | Pushes single-table predicates through join to the correct side | Filter on `orders.date` moves below the join to the orders branch |
| **EliminateSorts** | Removes unnecessary Sort operators | If data is already sorted or sort result is not consumed |
| **ReplaceExceptWithAntiJoin** | Converts `EXCEPT` to left anti join | More efficient than set-based subtraction |
| **RewriteCorrelatedScalarSubquery** | Converts correlated scalar subqueries to left outer joins | Avoids per-row subquery execution |

### Seeing Rules in Action

```python
# Predicate pushdown through a join:
# The filter on 'region' only applies to the customers table.
# Catalyst pushes it below the join.

result = (orders
    .join(customers, "customer_id")
    .filter(F.col("region") == "EMEA"))

# In the plan, the filter appears in the customers branch, BELOW the join:
#   BroadcastHashJoin [customer_id]
#   :- Scan parquet orders
#   +- BroadcastExchange
#      +- Filter (region = 'EMEA')            <-- pushed below the join
#         +- Scan parquet customers
```

```python
# Constant folding + filter simplification:
result = orders.filter(
    (F.lit(1) == F.lit(1)) & (F.col("status") == "active")
)
# Catalyst simplifies (1 = 1) to true, then (true AND status = 'active') to just (status = 'active')
# Plan shows only: Filter (status#7 = active)
```

### When Catalyst CANNOT Optimize

Catalyst treats Python UDFs, complex expressions, and certain patterns as opaque black boxes.

**Python UDFs block all optimization:**

```python
@udf(StringType())
def normalize(s):
    return s.strip().lower()

# Catalyst cannot push filters below UDFs, cannot fold constants inside them,
# and cannot prune columns that are UDF inputs even if the UDF output is later filtered away.
df = orders.withColumn("norm_status", normalize("status"))

# Plan shows BatchEvalPython -- Catalyst cannot look inside
```

**Complex expressions that prevent pushdown:**

```python
# These expressions PREVENT filter pushdown to the Parquet reader:
df.filter(F.col("name").contains("test"))          # LIKE '%test%' -- no pushdown (not prefix match)
df.filter(F.upper("status") == "ACTIVE")           # function wrapping column -- no pushdown
df.filter(F.col("data").getItem("key") == "val")   # nested field access -- limited pushdown

# These expressions ALLOW pushdown:
df.filter(F.col("name").startsWith("test"))        # LIKE 'test%' -- pushdown supported
df.filter(F.col("status") == "active")             # simple equality -- pushdown supported
df.filter(F.col("amount").between(100, 500))       # range -- pushdown supported
```

**Non-equi join conditions prevent hash/sort-merge joins:**

```python
# Catalyst CANNOT convert this to a SortMergeJoin:
df1.join(df2, df1["timestamp"].between(df2["start"], df2["end"]))
# Result: BroadcastNestedLoopJoin or CartesianProduct

# Workaround: bucket by a coarser key, then filter:
df1.join(df2, (df1["hour_bucket"] == df2["hour_bucket"]))  # equi-join on bucket
   .filter(df1["timestamp"].between(df2["start"], df2["end"]))  # then fine-grained filter
```

### CBO (Cost-Based Optimizer) in Practice

The CBO uses table and column statistics to make decisions like join ordering and join strategy selection.

```sql
-- Compute statistics for the CBO
ANALYZE TABLE sales.orders COMPUTE STATISTICS;
ANALYZE TABLE sales.orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date, amount;
```

```python
# Check if stats exist:
spark.table("sales.orders").explain("cost")
# Look for: Statistics(sizeInBytes=X, rowCount=Y)
# If you see: Statistics(sizeInBytes=8.0 EiB) -- that's the default "unknown" value
# It means stats haven't been computed and the CBO is guessing
```

> **Key Takeaway:** If Spark is choosing SortMergeJoin when the table is small enough for broadcast, check the statistics with `explain("cost")`. Stale or missing statistics are a common cause of suboptimal join strategy selection.

---

## 7. SQL Tab Query Plans

The SQL tab in the Spark UI shows the physical plan as an interactive graphical diagram. It is the richest source of runtime information about your query.

### Accessing the SQL Tab

1. Open the Spark UI (in Databricks: cluster page -> Spark UI, or the "View Spark UI" link on a completed command).
2. Click the **SQL / DataFrame** tab.
3. Find your query in the list (sorted by submission time, most recent first).
4. Click the query description to open its detail page.

### What the SQL Tab Shows

The SQL tab displays a **DAG (directed acyclic graph)** of physical operators, flowing top-to-bottom (opposite of `explain()` output which flows bottom-to-top).

Each node in the graph shows:
- **Operator name** (e.g., `Scan parquet`, `BroadcastHashJoin`, `HashAggregate`)
- **Runtime metrics** after execution completes:
  - `number of output rows`: actual rows produced
  - `data size` or `size of files read`: bytes processed
  - `time to compute`: wall-clock time
  - `spill size (memory)` / `spill size (disk)`: if the operator exceeded its memory budget
  - `shuffle records written` / `shuffle bytes written`: for Exchange operators
  - `rows output`: for each codegen stage

### Correlating SQL Tab Nodes with explain() Output

The node numbers in `explain("formatted")` directly correspond to the SQL tab node IDs:

```
explain("formatted") output:          SQL Tab DAG:

HashAggregate (9)            <-->      Node 9: HashAggregate
+- Exchange (8)              <-->      Node 8: Exchange
   +- HashAggregate (7)      <-->      Node 7: HashAggregate
```

To find a specific operator:
1. Note the node number from `explain("formatted")` (e.g., node 8).
2. In the SQL tab DAG, look for the node labeled with that same number.
3. Click on it to expand its runtime metrics.

### What to Look for in the SQL Tab

**Skew detection:** Compare `number of output rows` across Exchange nodes. If the input to a join is heavily skewed, one task will process far more rows than others. The SQL tab shows aggregate metrics, but the Stages tab (Tasks table) shows per-task breakdown.

**Spill detection:** Look for `spill size (memory)` and `spill size (disk)` on any node. Non-zero spill means the operator ran out of memory and fell back to disk I/O. Common on large shuffles, sorts, and aggregations.

**Broadcast size:** On `BroadcastExchange` nodes, check `data size`. If it's close to or above 8GB, you're at risk of broadcast failures (Spark has a hard 8GB limit per broadcast). Consider switching to SortMergeJoin.

**Scan efficiency:** On `Scan` nodes, compare `number of output rows` (rows surviving filters) vs. `number of files read` and `size of files read`. If you read 100GB of files but only output 1000 rows, your filters are not being pushed down effectively.

**Exchange volume:** On `Exchange` nodes, check `shuffle bytes written`. This is the data that crosses the network. Compare it to the input size to gauge how well partial aggregation is working. If shuffle output is almost as large as scan input, partial aggregation may not be reducing data volume (possible with high-cardinality group-by keys).

### SQL Tab vs. explain() -- When to Use Each

| Need | Use |
|------|-----|
| Quick check of the planned strategy before execution | `explain("formatted")` |
| Understanding the full plan lifecycle (logical -> physical) | `explain("extended")` |
| Checking CBO statistics | `explain("cost")` |
| Runtime performance analysis (actual row counts, timing, spill) | SQL tab in Spark UI |
| Identifying which stage/task is slow | Stages tab in Spark UI (linked from SQL tab) |
| Sharing a plan with a colleague | `explain("formatted")` output (text) or SQL tab screenshot |

> **Photon:** On Photon-enabled clusters, the SQL tab shows Photon-specific operators (`PhotonGroupingAgg`, `PhotonBroadcastHashJoin`, etc.) with the same metric structure. Additionally, Photon adds its own metrics like `photon CPUs time` and `photon rows processed`. These appear alongside the standard Spark metrics and are useful for understanding Photon utilization. If an operator falls back from Photon to JVM (e.g., due to an unsupported expression), the SQL tab shows the JVM operator name instead, which is your signal to investigate.

---

## Quick Reference Card

```
# Your go-to commands for query plan analysis:

df.explain("formatted")          # Default: clean, readable plan with node details
df.explain("extended")           # Full lifecycle: parsed -> analyzed -> optimized -> physical
df.explain("cost")               # Check CBO statistics (sizeInBytes, rowCount)

# After execution, see runtime metrics:
df.collect()                     # Execute the query
df.explain("formatted")         # Shows isFinalPlan=true with AQE adjustments

# In SQL:
EXPLAIN FORMATTED SELECT ...     # Same as df.explain("formatted")
EXPLAIN EXTENDED SELECT ...      # Same as df.explain("extended")
EXPLAIN COST SELECT ...          # Same as df.explain("cost")
```

> **Final Takeaway:** Make `explain("formatted")` a habit. Run it before every performance investigation. Read the plan bottom-up, count the Exchange nodes, verify filter pushdown, and check join strategies. The plan tells you what Spark will do -- and what it won't.
