---
name: update-skills-from-lessons
description: "Review lessons learned from practice sessions and suggest updates to custom skills only. Use on session start or when the user says 'check lessons', 'update skills from lessons', 'any new lessons', or 'skill updates'. Requires a lessons_path provided by the caller (CLAUDE.md)."
---

# Update Skills from Lessons Learned

Scan practice session lessons and propose targeted updates to the custom skills in this repository.

## Lessons Directory

The caller (CLAUDE.md) must provide the `lessons_path`. If no path was provided, ask the user for it before proceeding.

Each file in this directory documents a real issue encountered during hands-on practice — root cause, fix applied, and takeaway.

## Workflow

### 1. Read All Lesson Files

Read every `.md` file in the lessons directory. For each lesson, extract:
- **What went wrong** (the problem)
- **Why** (root cause)
- **What was fixed** (the actual change)
- **Takeaway** (the general principle)

### 2. Match Lessons to Skills

For each lesson, determine which custom skill(s) it applies to:

| Lesson topic | Likely skill |
|-------------|--------------|
| Overture Maps, S3 paths, spatial SQL, ST_ functions, H3 | `databricks-geospatial` |
| Spark performance, query plans, skew, OOM, shuffle | `spark-job-optimization` |
| dbldatagen, Faker, synthetic data, data generation | `dbldatagen` |
| Free tier, serverless, Spark Connect, banned APIs | `databricks-free-tier-guardrails` |
| Practice exercises, coaching, drills | `databricks-practice-skill` |
| POC builder, Kafka, PostgreSQL, Neo4j | `data-api-poc-builder` |

If a lesson doesn't match any custom skill, note it as a potential new skill topic or an upstream AI Dev Kit issue.

### 3. Classify the Update Type

For each match, classify what kind of update is needed:

- **Content fix**: A code example, path, or API usage in the skill is wrong or outdated
- **New warning**: The skill should warn about a gotcha that tripped someone up
- **New recipe/example**: The lesson reveals a pattern worth adding as a recipe
- **Documentation gap**: The skill covers the topic but doesn't mention this specific scenario

### 4. Present Suggestions

For each suggested update, present:

```
Skill: <skill-name>
File: <specific .md file in the skill>
Type: <content fix | new warning | new recipe | documentation gap>
Lesson: <which lesson file>
Summary: <1-2 sentences: what to change and why>
```

Group by skill. Present all suggestions, then wait for approval before making any changes.

### 5. Apply Approved Updates

When the user approves:
1. Edit the skill file in `custom-skills/<skill-name>/`
2. Keep changes minimal and targeted — don't restructure existing content
3. Add warnings near the relevant existing content (don't create new sections unless necessary)
4. For code fixes, update the example in-place
5. After editing, confirm what was changed

## Rules

- **Custom skills only** — only edit files in `custom-skills/`. Never modify AI Dev Kit skills (in `ai-dev-kit/`), installed copies (in `.claude/skills/`), or any upstream content. If a lesson applies to an AI Dev Kit skill, note it as an upstream issue but do not make changes.
- Only suggest updates backed by a specific lesson — no speculative improvements
- If a lesson contradicts existing skill content, flag it clearly and explain the conflict
- Prefer adding a warning or note near existing content over restructuring
- If no lessons produce actionable updates, say so and move on
