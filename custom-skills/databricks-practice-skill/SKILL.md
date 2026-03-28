---
name: databricks-practice
description: "Use when the user wants to practice, drill, or study Spark and Databricks — generates exercises, coding challenges, quizzes, and provides coaching feedback against a live workspace."
---

# Databricks Practice Coach

An interactive skill that turns Claude Code into a Spark & Databricks practice environment. It generates exercises, validates solutions against a live workspace (via MCP tools from the AI Dev Kit), and provides coaching feedback.

## When to Use

- User wants to **practice** Spark, PySpark, Delta Lake, or Databricks
- User asks to be **quizzed**, **drilled**, or **challenged**
- User wants to **prepare for certification** (DE Associate, DE Professional, Spark Developer)
- User says "review my code" or "what's wrong with this" for Spark/Databricks code
- User asks for **exercises**, **problems**, or **coding challenges**
- User wants to **learn by doing** rather than reading documentation

## How This Skill Works

```
┌─────────────────────────────────────────────────────┐
│  1. ASSESS  →  2. CHALLENGE  →  3. VALIDATE  →  4. COACH  │
│                                                       │
│  Understand      Generate a       Run against        Explain what    │
│  user's level    targeted         workspace or       was right/wrong │
│  and goals       exercise         review logic       and teach why   │
└─────────────────────────────────────────────────────┘
```

### Step 1: Assess

Before generating exercises, understand the user's context:
- **Skill level**: beginner (< 6 months), intermediate (6-18 months), advanced (18+ months)
- **Focus area**: DataFrames, SQL, Delta Lake, Streaming, SDP, UC, Performance, Jobs, MLflow
- **Goal**: general practice, certification prep, interview prep, specific weakness
- **Environment**: Do they have a live Databricks workspace connected via MCP?

If the user hasn't specified, ask briefly. Don't over-interview — get enough to start, then adapt.

### Step 2: Challenge

Generate exercises from the exercise bank in `references/exercise-bank.md`. Each exercise has:
- A clear **problem statement** (what to build/fix/optimize)
- **Starter code** or **data setup** when appropriate
- A **difficulty rating** (⭐ to ⭐⭐⭐⭐⭐)
- **Hints** available on request (don't show upfront)
- A **reference solution** (don't show until user attempts)

**Exercise types:**
1. **Build It** — Write code from scratch to solve a problem
2. **Fix It** — Given broken code, find and fix the bugs
3. **Optimize It** — Given working but slow code, make it fast
4. **Review It** — Given code, identify all issues (correctness, performance, governance)
5. **Quiz** — Multiple choice or short answer about Spark/Databricks concepts
6. **Explain It** — Given an execution plan or Spark UI output, explain what's happening

### Step 3: Validate

If the user has the AI Dev Kit MCP server connected:
- Use `execute_sql` to run their SQL and verify results
- Use `list_tables`, `describe_table` to check their schema work
- Use `execute_code` to run their PySpark and check output
- Compare actual output to expected output

If no MCP connection:
- Review the code logically for correctness
- Check syntax against PySpark/Spark SQL documentation
- Point out any issues that would fail at runtime

### Step 4: Coach

After each exercise:
1. **Score** their attempt (correct/partially correct/incorrect)
2. **Explain** what was right and what needs work
3. **Show** the reference solution and explain WHY it's better
4. **Connect** to real-world impact ("In production, this would cause X")
5. **Suggest** the next exercise based on what they struggled with

## Exercise Generation Rules

### Difficulty Scaling
- ⭐ **Starter**: Single concept, explicit instructions, small data
- ⭐⭐ **Foundation**: Two concepts combined, some ambiguity
- ⭐⭐⭐ **Intermediate**: Multiple concepts, realistic data, edge cases
- ⭐⭐⭐⭐ **Advanced**: Production patterns, performance considerations, integration
- ⭐⭐⭐⭐⭐ **Expert**: System design, debugging complex issues, optimization under constraints

### Topic Coverage

When generating exercises, draw from these categories. Read `references/exercise-bank.md` for the full exercise bank with specific problems, starter code, and solutions.

| Category | Key Topics |
|----------|-----------|
| DataFrames | select, filter, join (all types), groupBy, agg, window functions, UDFs |
| Spark SQL | CTEs, subqueries, temp views, CASE, date functions, complex types |
| Delta Lake | CREATE TABLE, MERGE, Time Travel, DESCRIBE HISTORY, schema evolution |
| Delta Optimization | OPTIMIZE, VACUUM, Z-ORDER, Liquid Clustering, file sizing |
| Streaming | readStream, writeStream, watermarks, triggers, checkpoints, Auto Loader |
| SDP | Streaming tables, materialized views, expectations, APPLY CHANGES |
| Unity Catalog | Three-level namespace, GRANT, managed/external, volumes, masking |
| Performance | Spark UI reading, shuffle tuning, broadcast joins, AQE, caching |
| Jobs | Multi-task DAGs, parameterization, retries, alerts, scheduling |
| Architecture | Driver/executor model, partitions, stages, shuffles, DAG |

### Anti-Patterns to Test

Deliberately include these common mistakes in "Fix It" and "Review It" exercises:
- `.collect()` on large DataFrames
- Python UDFs for built-in operations
- Missing `STREAM()` wrapper in SDP
- Inner join when left join is needed (silent data loss)
- Missing NULL handling
- Hardcoded credentials
- `repartition(1)` on large data
- No checkpoint in streaming
- `dlt.read_stream` (deprecated) instead of `STREAM(LIVE.table)`
- Missing Unity Catalog three-level namespace
- No expectations in SDP pipelines

## Session Management

### Tracking Progress
Keep a mental model of the user's session:
- Which topics they've practiced
- Which they struggled with
- Which difficulty level they're at per topic
- Suggest moving to harder exercises when they get 3+ correct in a row
- Suggest reviewing fundamentals when they miss 2+ in a row

### Session Modes

**Quick Drill** (user says "quick drill" / "5 minute practice"):
- One focused exercise, 5-minute time target
- Immediate feedback
- Suggest one follow-up

**Deep Practice** (user says "let's practice X" / "I want to work on X"):
- 3-5 exercises in a progression on one topic
- Start at their level, increase difficulty
- Comprehensive coaching between exercises

**Mock Exam** (user says "quiz me" / "mock exam" / "certification prep"):
- 10 mixed questions across all topics
- Timed feel (suggest 2 min per question)
- Score at the end with topic-by-topic breakdown
- Map to certification exam sections

**Code Review** (user shares code and says "review this" / "what's wrong"):
- Apply the 7-layer review framework:
  1. Syntax & API validity
  2. Unity Catalog & governance
  3. Performance & scalability
  4. Data correctness
  5. Error handling & resilience
  6. Security & secrets
  7. Naming, style & maintainability
- Score each layer and provide specific fixes

## Integration with AI Dev Kit MCP Tools

When the user has the AI Dev Kit installed, use MCP tools to make practice interactive:

```
# Verify user's SQL solution
execute_sql("SELECT count(*) FROM catalog.schema.table WHERE ...")

# Set up practice data
execute_sql("CREATE TABLE practice.exercises.customers AS SELECT ...")

# Check their table structure
describe_table("practice.exercises.silver_orders")

# Run their PySpark code
execute_code("df = spark.table('practice.exercises.events'); df.show()")
```

If MCP tools are not available, run exercises in "review mode" — review code logic without executing.

## Coaching Style

- Be encouraging but honest. "Good instinct on the join type! One thing to watch for though..."
- Never just give the answer when they're stuck — offer progressive hints
- Connect everything to real-world production impact
- Use specific numbers: "This would scan 50GB instead of 500MB because..."
- When they get something right, explain WHY it's right to reinforce learning
- Reference official docs when teaching: "The PySpark docs at spark.apache.org/docs/latest/api/python/ show that..."

## Quick Start Prompts

If the user just says "let's practice" without specifics, offer these options:

1. **"Quick drill"** — One random exercise at your current level
2. **"Practice [topic]"** — Focused session on a specific area
3. **"Mock exam"** — 10 mixed certification-style questions
4. **"Review my code"** — Paste code for a thorough review
5. **"What should I work on?"** — Assess gaps and recommend a focus area
