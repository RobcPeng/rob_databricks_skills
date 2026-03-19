# 🏋️ Databricks Practice Skill for Claude Code

An interactive coaching skill that turns Claude Code into a Spark & Databricks practice environment. Works standalone or alongside the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) for live workspace validation.

## What It Does

| Mode | Trigger Phrases | What Happens |
|------|----------------|--------------|
| **Quick Drill** | "quick drill", "5 min practice" | One focused exercise with immediate feedback |
| **Deep Practice** | "practice window functions", "help me learn Delta Lake" | 3-5 progressive exercises on one topic |
| **Mock Exam** | "quiz me", "mock exam", "certification prep" | 10 mixed questions with scoring |
| **Code Review** | "review my code", "what's wrong with this" | 7-layer production review framework |
| **Gap Assessment** | "what should I work on?" | Identifies weak areas, suggests exercises |

## Exercise Types

- **Build It** — Write code from scratch to solve a problem
- **Fix It** — Find and fix bugs in broken Spark/Databricks code
- **Optimize It** — Make slow code fast (with Spark UI analysis)
- **Review It** — Spot all issues in code (correctness, performance, governance)
- **Quiz** — Certification-style multiple choice questions
- **Explain It** — Interpret execution plans and Spark UI output

## Topics Covered

DataFrames, Spark SQL, Joins, Window Functions, Delta Lake (MERGE, Time Travel, OPTIMIZE), Structured Streaming, Auto Loader, Spark Declarative Pipelines (SDP/DLT), Unity Catalog, Performance Tuning, Databricks Jobs, MLflow, and more.

## Installation

### Option A: If you already have the AI Dev Kit installed

```bash
# From your project directory (where .claude/ exists)
cd /path/to/your/project

# Copy the skill files
mkdir -p .claude/skills/databricks-practice/references
cp SKILL.md .claude/skills/databricks-practice/
cp references/* .claude/skills/databricks-practice/references/
```

### Option B: Fresh install alongside AI Dev Kit

```bash
# 1. Install the AI Dev Kit first
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)

# 2. Then install this practice skill
bash scripts/install-practice-skill.sh
```

### Option C: Standalone (no AI Dev Kit)

```bash
# Create the Claude Code skill directory
mkdir -p .claude/skills/databricks-practice/references
cp SKILL.md .claude/skills/databricks-practice/
cp references/* .claude/skills/databricks-practice/references/
```

The skill works without the AI Dev Kit but in "review mode" only (code review without live execution). With the AI Dev Kit MCP server, exercises can run against your real Databricks workspace.

## Usage

Once installed, open Claude Code and try:

```
> Let's practice Spark

> Give me a Delta Lake MERGE challenge

> Quiz me — I'm preparing for the DE Associate exam

> Review this code:
  df = spark.table("events")
  result = df.join(users, "user_id").collect()

> I'm weak on window functions, help me practice

> Quick drill — something intermediate level
```

## Skill Structure

```
databricks-practice/
├── SKILL.md                          # Main skill — coaching logic and exercise generation
├── references/
│   ├── exercise-bank.md              # 20+ curated exercises with solutions
│   └── review-framework.md           # 7-layer code review scoring framework
├── scripts/
│   └── install-practice-skill.sh     # Installation helper
└── README.md                         # This file
```

## How It Works with the AI Dev Kit

```
┌──────────────────────────────────────────────────────────────┐
│  databricks-practice skill          AI Dev Kit               │
│  (Knowledge + Coaching)      (Actions + Patterns)            │
│                                                              │
│  ┌─────────────┐            ┌─────────────────────┐         │
│  │ SKILL.md    │            │ databricks-skills/   │         │
│  │ (coaching   │            │ (19 pattern skills)  │         │
│  │  logic)     │            │                      │         │
│  ├─────────────┤            ├─────────────────────┤         │
│  │ exercise-   │            │ databricks-mcp-      │         │
│  │ bank.md     │───────────▶│ server/              │         │
│  │ (problems)  │  validate  │ (50+ live tools)     │         │
│  ├─────────────┤  against   │ • execute_sql        │         │
│  │ review-     │  workspace │ • execute_code       │         │
│  │ framework   │            │ • describe_table     │         │
│  └─────────────┘            └─────────────────────┘         │
└──────────────────────────────────────────────────────────────┘
```

The practice skill generates exercises and coaching. The AI Dev Kit's MCP server validates solutions against your live workspace. The AI Dev Kit's other skills provide the correct patterns that exercises test against.

## Adding Your Own Exercises

Edit `references/exercise-bank.md` to add exercises. Follow this template:

```markdown
### TOPIC-N: Exercise Title ⭐⭐⭐
**Type:** Build It | Fix It | Optimize It | Review It | Quiz

**Problem:** Clear description of what the user needs to do.

**Starter code (if applicable):**
\```python
# Code the user starts with
\```

**Hints:**
1. First hint (don't reveal the answer)
2. Second hint (more specific)

**Reference solution:**
\```python
# The correct solution
\```

**Teaching points:**
- Why the solution works
- Real-world impact
- Related concepts to explore
```

## License

This skill is provided for educational use. The exercise patterns reference the Databricks AI Dev Kit (Databricks License) and Apache Spark (Apache 2.0 License).
