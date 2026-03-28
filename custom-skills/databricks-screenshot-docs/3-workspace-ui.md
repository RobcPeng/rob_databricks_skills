# Databricks Workspace UI — Pipelines, Jobs, and Notebooks

The workspace UI has consistent chrome (top navbar, left sidebar) across all pages. These patterns help you capture clean screenshots of operational views.

## General Workspace Prep

```
# Use wide viewport — workspace sidebar eats ~250px
browser_resize -> width: 1920, height: 1080
browser_navigate -> url: "https://your-workspace.databricks.com/..."
browser_wait_for -> time: 3

# Optional: collapse the left sidebar for more content area
browser_run_code -> code: "async (page) => {
  await page.evaluate(() => {
    const sidebar = document.querySelector('[data-testid=\"left-sidebar\"]') ||
                    document.querySelector('.left-sidebar');
    if (sidebar) sidebar.style.display = 'none';
  });
}"
```

---

## Pipelines (SDP / DLT)

**Pipeline DAG view** — the lineage graph showing table dependencies:

```
# Navigate to the pipeline
browser_navigate -> url: "https://your-workspace.databricks.com/pipelines/{pipeline_id}"
browser_wait_for -> time: 5                    # DAG renders async

# Wait for tables to appear in the graph
browser_wait_for -> text: "bronze_"            # or any known table name

# The DAG can be wide — use fullPage or zoom out
browser_run_code -> code: "async (page) => {
  await page.evaluate(() => { document.body.style.zoom = '80%'; });
  await page.waitForTimeout(1000);
}"
browser_take_screenshot -> filename: "pipeline-dag-overview.png"
```

**Pipeline event log** — for showing update status, errors, or data quality:

```
# Click the "Events" tab first (use snapshot to find ref)
browser_snapshot
browser_click -> ref: "{events_tab_ref}", element: "Events tab"
browser_wait_for -> time: 2
browser_take_screenshot -> filename: "pipeline-events.png"
```

**What makes a good pipeline screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| DAG view | All tables visible, connections clear, zoom fits the graph | Truncated graph, scroll needed, overlapping labels |
| Update status | Green checkmarks on all tables, timestamp visible | Mid-update with spinners (unless showing progress) |
| Event log | Filtered to relevant events, error highlighted | Unfiltered log with hundreds of INFO lines |
| Data quality | DQ tab open, expectations visible with pass/fail | Raw metrics without context |

---

## Jobs

**Job run list** — showing execution history:

```
browser_navigate -> url: "https://your-workspace.databricks.com/jobs/{job_id}"
browser_wait_for -> time: 3

# Show recent runs — wait for the run history table
browser_wait_for -> text: "Succeeded"         # or "Failed" if showing an error
browser_take_screenshot -> filename: "job-run-history.png"
```

**Job run detail** — task DAG for multi-task jobs:

```
browser_navigate -> url: "https://your-workspace.databricks.com/jobs/{job_id}/runs/{run_id}"
browser_wait_for -> time: 4                    # task graph renders

# The task DAG is the hero visual for job screenshots
browser_take_screenshot -> filename: "job-task-dag.png"
```

**Job run output / logs** — for debugging or showing results:

```
# Navigate to a specific task's output
browser_navigate -> url: "https://your-workspace.databricks.com/jobs/{job_id}/runs/{run_id}/tasks/{task_key}"
browser_wait_for -> time: 3

# Scroll to output if needed
browser_run_code -> code: "async (page) => {
  const output = document.querySelector('[data-testid=\"task-output\"]') ||
                 document.querySelector('.run-output');
  if (output) output.scrollIntoView({ behavior: 'instant', block: 'start' });
  await page.waitForTimeout(500);
}"
browser_take_screenshot -> filename: "job-task-output.png"
```

**What makes a good job screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| Run history | 5-10 recent runs visible, mix of succeeded/failed tells a story | Single run with no context |
| Task DAG | All tasks visible with dependency arrows, status colors clear | Zoomed too tight on one task |
| Duration | Run duration column visible — readers care about performance | Duration cropped out |
| Errors | Error message visible, red status highlighted | Error truncated, needs scroll |

---

## Notebooks

**Notebook cells** — showing code + output:

```
browser_navigate -> url: "https://your-workspace.databricks.com/#notebook/{notebook_id}"
browser_wait_for -> time: 4                    # cells render

# Hide the notebook toolbar for cleaner shots
browser_run_code -> code: "async (page) => {
  await page.evaluate(() => {
    document.querySelectorAll('.notebook-toolbar, [data-testid=\"notebook-toolbar\"]').forEach(
      el => el.style.display = 'none'
    );
  });
}"
browser_take_screenshot -> filename: "notebook-overview.png"
```

**Individual cell + output** — the most common notebook screenshot:

```
# Use snapshot to find the cell element ref
browser_snapshot

# Screenshot just the target cell
browser_take_screenshot -> element: "Cell with spark.read output", ref: "{cell_ref}", filename: "notebook-cell-read.png"
```

**Notebook with visualizations** — charts rendered in cell output:

```
# Navigate to notebook, wait for cell execution
browser_wait_for -> time: 5

# Scroll to the visualization cell
browser_run_code -> code: "async (page) => {
  // Find cell output with a chart (canvas or SVG)
  const charts = document.querySelectorAll('.cell-output canvas, .cell-output svg');
  if (charts.length > 0) {
    charts[charts.length - 1].scrollIntoView({ behavior: 'instant', block: 'center' });
  }
  await page.waitForTimeout(1000);
}"
browser_take_screenshot -> filename: "notebook-chart-output.png"
```

**What makes a good notebook screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| Code cells | Syntax highlighted, readable font size, complete logic visible | Truncated code, scroll needed |
| Cell output | Table or chart fully rendered, no "Loading..." | Partial output, spinner visible |
| Narrative flow | 2-3 cells showing setup -> transform -> result | 15 cells crammed into one shot |
| Error screenshots | Error cell highlighted, traceback readable | Error message cropped |
| Comments | Markdown cells visible to provide context | Code-only with no explanation |

---

## Workspace URL Patterns

Quick reference for constructing direct URLs:

| View | URL Pattern |
|------|-------------|
| Pipeline overview | `/pipelines/{pipeline_id}` |
| Pipeline update | `/pipelines/{pipeline_id}/updates/{update_id}` |
| Job overview | `/jobs/{job_id}` |
| Job run detail | `/jobs/{job_id}/runs/{run_id}` |
| Job task output | `/jobs/{job_id}/runs/{run_id}/tasks/{task_key}` |
| Notebook | `/#notebook/{notebook_id}` |
| SQL editor | `/sql/editor` |
| Dashboard (published) | `/sql/dashboardsv3/{dashboard_id}` |
| Cluster | `/compute/clusters/{cluster_id}` |
| Unity Catalog table | `/explore/data/{catalog}/{schema}/{table}` |

---

## Navigation

- [SKILL.md](SKILL.md) — Overview and quick reference
- [1-content-readiness.md](1-content-readiness.md) — Wait strategies and readability
- [2-maps-and-dashboards.md](2-maps-and-dashboards.md) — Maps and dashboard composition
- [4-common-app-patterns.md](4-common-app-patterns.md) — Common Databricks app patterns
- [5-output-and-annotation.md](5-output-and-annotation.md) — Output format and annotation
