# CLAUDE.md — rob_skills

## Project Overview

This repository manages custom Databricks skills that extend the AI Dev Kit. It wraps the `ai-dev-kit` git submodule with custom skills in `custom-skills/` and an installer (`update.sh`) that deploys everything to Claude Code and optionally to Genie Code in a Databricks workspace.

## On Session Start

Invoke the `update-skills-from-lessons` skill with the lessons directory:

```
lessons_path: ~/Documents/dbx/databricks_sandbox/ai_kit_dev_practice/artifacts/lessons_learned/custom_skill_updates/
```

This checks for new lessons and suggests updates to **custom skills only** (in `custom-skills/`). Never modify AI Dev Kit skills in `ai-dev-kit/` or installed copies in `.claude/skills/`.

## Repository Structure

```
rob_skills/
  ai-dev-kit/              # Git submodule — upstream AI Dev Kit (do not edit directly)
  custom-skills/           # Custom skills owned by this repo
  update.sh                # Installer: submodule pull + AI Dev Kit install + custom skill deploy
  .claude/skills/          # Installed skills (populated by update.sh, gitignored)
```

## Key Files

- `update.sh` — Master installer. Steps: pull submodule, run AI Dev Kit installer, patch MCP config with Databricks profile, optional Builder App setup, interactive custom skill selection, install skills, optional Genie Code deploy.
- `custom-skills/*/SKILL.md` — Custom skill definitions with frontmatter (name, description) and content.
- `ai-dev-kit/.mcp.json` — MCP server config for the Databricks plugin.

## Custom Skills

| Skill | Default | Purpose |
|-------|---------|---------|
| `databricks-geospatial` | ON | Spatial SQL, H3, Overture Maps |
| `dbldatagen` | ON | Synthetic data generation (dbldatagen + Faker) |
| `spark-job-optimization` | ON | Spark performance tuning and diagnostics |
| `databricks-practice-skill` | OFF | Interactive practice coach |
| `databricks-free-tier-guardrails` | OFF | Serverless/free tier compatibility filter |
| `data-api-poc-builder` | OFF | POC builder for external data sources (requires Kafka/PG/Neo4j) |
| `databricks-governance` | ON | Unity Catalog governance: RBAC, ABAC, tagging, masking, audit, compliance |
| `databricks-pipeline-guardrails` | ON | Qualified names, table existence checks, schema contracts, broadcast hints, union alignment |
| `databricks-screenshot-docs` | ON | Playwright screenshots for docs, blogs, presentations — viewports, maps, readability |
| `databricks-noaa-storm-events` | ON | NOAA Storm Events: severe weather CSVs, damage parsing, spatial analysis |
| `update-skills-from-lessons` | OFF | Scan practice lessons and suggest updates to custom skills |

## Working With This Repo

- Edit custom skills in `custom-skills/`, not in `.claude/skills/` (installed copies get overwritten by `update.sh`)
- `update.sh` is bash 3.2 compatible (macOS default)
- The AI Dev Kit submodule should not be edited directly — changes go upstream
- Skills use markdown links to reference related skills; these links serve as navigation for both humans and AI assistants
