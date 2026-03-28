---
name: databricks-screenshot-docs
description: "Use when capturing screenshots of Databricks apps for documentation, blogs, demos, or presentations. Covers viewport sizing, map readability, async wait, and annotation."
---

# Databricks Screenshot Documentation

Capture publication-quality screenshots of Databricks applications using the Playwright MCP tools. This skill covers viewport sizing, content readiness, map/chart optimization, and output conventions for docs, blogs, and presentations.

---

## Quick Reference — Viewport Presets

Resize the browser **before** navigating or after full load. All dimensions in pixels.

| Use Case | Width | Height | Aspect | When to Use |
|----------|-------|--------|--------|-------------|
| Blog post hero | 1280 | 720 | 16:9 | Lead image, full-width embed |
| Blog post inline | 1024 | 768 | 4:3 | Mid-article screenshots |
| Documentation | 1440 | 900 | 16:10 | Product docs, tutorials |
| Dashboard (wide) | 1920 | 1080 | 16:9 | Full dashboard with sidebar |
| Dashboard (content only) | 1600 | 900 | 16:9 | Dashboard after hiding sidebar |
| Mobile preview | 390 | 844 | ~9:19 | Responsive app testing |
| Social / thumbnail | 1200 | 630 | ~1.9:1 | LinkedIn, Twitter cards |
| Presentation slide | 1920 | 1080 | 16:9 | Keynote / PowerPoint embed |

```
# Set viewport before capturing
browser_resize → width: 1280, height: 720
browser_navigate → url: "https://your-app.databricks.app/"
browser_wait_for → time: 3          # let content render
browser_take_screenshot → filename: "hero-dashboard.png", type: "png"
```

---

## 1. Content Readiness — Wait Before You Shoot

Databricks apps render asynchronously. A screenshot taken too early captures spinners, empty charts, or half-loaded maps.

### Wait Strategies

| Content Type | Wait Strategy | Tool |
|-------------|--------------|------|
| Static page | Wait 2-3 seconds after navigate | `browser_wait_for` → `time: 3` |
| Data tables | Wait for row text to appear | `browser_wait_for` → `text: "showing 1-25"` |
| Charts (Plotly, matplotlib) | Wait for axis label or legend | `browser_wait_for` → `text: "Revenue"` |
| Maps (kepler, folium) | Wait 5-8 seconds for tile load | `browser_wait_for` → `time: 8` |
| Loading spinners | Wait for spinner to disappear | `browser_wait_for` → `textGone: "Loading..."` |
| Streamlit apps | Wait for Streamlit loaded marker | `browser_wait_for` → `textGone: "Please wait..."` |
| Dash apps | Wait for callback completion | `browser_wait_for` → `time: 4` |

### Map-Specific Waits

Maps are the hardest to screenshot well. Tile layers load asynchronously from external CDNs.

```
# 1. Navigate to the page with the map
browser_navigate → url: "https://your-app.databricks.app/map"

# 2. Wait for the map framework to initialize
browser_wait_for → text: "Leaflet" or time: 5

# 3. Wait extra for tile layers to fully render
browser_wait_for → time: 8

# 4. THEN screenshot
browser_take_screenshot → filename: "map-overview.png"
```

**If tiles are still loading:** Use `browser_run_code` to force a longer wait:
```
browser_run_code → code: "async (page) => { await page.waitForTimeout(10000); }"
```

---

## 2. Maps — Making Them Look Good

### Zoom Level Guidelines

| Data Scope | Recommended Zoom | Example |
|-----------|-----------------|---------|
| Continental / country | 4-5 | US-wide coverage map |
| State / region | 6-7 | Colorado service areas |
| Metro area | 9-11 | Denver neighborhoods |
| Neighborhood / campus | 13-15 | Single building cluster |
| Street level | 16-18 | Individual addresses |

### Map Screenshot Checklist

- [ ] **Zoom fits the data** — all relevant points/polygons visible with ~10% padding
- [ ] **Tiles fully loaded** — no gray placeholder squares
- [ ] **Legend visible** — if using color scales, legend must be in the viewport
- [ ] **No UI clutter** — hide zoom controls, layer pickers if they distract from the data
- [ ] **Basemap contrast** — dark basemap for light-colored data, light basemap for dark data
- [ ] **Point density readable** — if points overlap heavily, zoom in or use clustering/H3 aggregation

### Adjusting Map Viewport via Code

For folium/Leaflet maps, you can programmatically set the view:
```
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    // Find the Leaflet map instance and set view
    const maps = Object.values(window).filter(v => v && v._leaflet_id);
    if (maps.length > 0) {
      maps[0].setView([39.7392, -104.9903], 11);  // Denver, zoom 11
      maps[0].invalidateSize();
    }
  });
  await page.waitForTimeout(3000);  // wait for tiles
}"
```

For KeplerGL maps, zoom/pan isn't easily scriptable — position the map manually first, then screenshot.

---

## 3. Dashboards — Layout and Composition

### Hiding Distracting UI Elements

Databricks apps often have navbars, sidebars, or debug panels that clutter screenshots:

```
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    // Hide Streamlit hamburger menu and footer
    document.querySelectorAll('[data-testid=\"stHeader\"], footer, #MainMenu').forEach(
      el => el.style.display = 'none'
    );
    // Hide Dash debug button
    document.querySelectorAll('._dash-debug-menu').forEach(
      el => el.style.display = 'none'
    );
  });
}"
```

### Screenshot Composition Rules

| Rule | Why |
|------|-----|
| **One story per screenshot** | Don't cram 6 charts into one image — the reader can't parse it |
| **Crop to the content** | Use element-level screenshots for individual widgets |
| **Consistent sizing across a post** | All inline screenshots should share the same width |
| **Light mode for docs** | Dark mode screenshots look jarring in white-background docs |
| **Dark mode for presentations** | Dark backgrounds project better in conference rooms |
| **Include just enough context** | A KPI number means nothing without its label and title |

### Element-Level Screenshots

Instead of full-page captures, screenshot specific components:

```
# 1. Take a snapshot to find element refs
browser_snapshot

# 2. Screenshot just the chart element
browser_take_screenshot → element: "Revenue trend chart", ref: "e42", filename: "revenue-chart.png"
```

This produces tighter, more focused images — better for inline documentation.

### Full-Page Screenshots

For long scrolling pages (e.g., a multi-section Streamlit app):

```
browser_take_screenshot → fullPage: true, filename: "full-app.png"
```

**Warning:** Full-page screenshots can be very tall. Use sparingly — they're good for architecture docs but bad for blog posts.

---

## 4. Readability and Zoom

### Font Size and Zoom

If text is too small to read at the screenshot's display size, zoom the page before capturing:

```
browser_run_code → code: "async (page) => {
  await page.evaluate(() => { document.body.style.zoom = '125%'; });
  await page.waitForTimeout(1000);
}"
```

| Display Context | Recommended Zoom | Why |
|----------------|-----------------|-----|
| Blog post (800px display width) | 100-110% | Content renders at roughly 1:1 |
| Documentation (max 720px) | 110-125% | Docs are narrower, text shrinks more |
| Presentation slides | 125-150% | Projected screens lose detail |
| Social media cards | 125-150% | Thumbnails compress aggressively |

### Data Table Readability

| Rows Visible | Action |
|-------------|--------|
| < 10 rows | Show the full table — it's readable |
| 10-25 rows | Acceptable, but consider cropping to key rows |
| 25+ rows | Screenshot is unreadable — filter or paginate first |

**Tip:** For wide tables, scroll horizontally to show the most important columns before capturing, or use element-level screenshot on a specific section.

---

## 5. Output Format and Naming

### Format Selection

| Format | When to Use |
|--------|-------------|
| **PNG** | Default. Lossless, crisp text, transparent backgrounds. Best for docs and blogs. |
| **JPEG** | Photos, map screenshots with many tile colors. Smaller file size, lossy compression. |

**Rule of thumb:** If the screenshot has text or UI elements, use PNG. If it's mostly a map with minimal UI, JPEG is fine.

### File Naming Convention

```
{context}-{subject}-{detail}.png

# Examples:
dashboard-revenue-kpi-row.png
map-denver-neighborhoods-h3.png
app-citizen-services-form.png
table-silver-enriched-preview.png
chart-response-time-by-district.png
```

- Lowercase, hyphens only
- Context first (dashboard, map, app, table, chart)
- Subject matches what the reader sees
- No timestamps in filenames (use git for versioning)

### Output Directory

Save all screenshots to a consistent location:
```
screenshots/
  blog-{post-name}/
    hero-dashboard.png
    map-overview.png
    chart-detail.png
  docs/
    setup-wizard-step1.png
    setup-wizard-step2.png
```

---

## 6. Common Databricks App Patterns

### Streamlit Apps

```
# 1. Set viewport
browser_resize → width: 1280, height: 720

# 2. Navigate
browser_navigate → url: "https://your-app.databricks.app/"

# 3. Wait for Streamlit to finish loading
browser_wait_for → textGone: "Please wait..."
browser_wait_for → time: 2

# 4. Hide Streamlit chrome
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    document.querySelectorAll('[data-testid=\"stHeader\"], footer, [data-testid=\"stToolbar\"]').forEach(
      el => el.style.display = 'none'
    );
  });
}"

# 5. Screenshot
browser_take_screenshot → filename: "streamlit-app.png"
```

### Dash Apps

```
# Dash apps load faster but callbacks can be slow
browser_resize → width: 1280, height: 720
browser_navigate → url: "https://your-app.databricks.app/"
browser_wait_for → time: 4                    # callbacks complete
browser_take_screenshot → filename: "dash-app.png"
```

### Gradio Apps

```
# Gradio has its own loading indicator
browser_resize → width: 1280, height: 720
browser_navigate → url: "https://your-app.databricks.app/"
browser_wait_for → textGone: "Loading..."
browser_wait_for → time: 2
browser_take_screenshot → filename: "gradio-app.png"
```

### AI/BI Dashboards (Lakeview)

Lakeview dashboards are embedded in the Databricks workspace UI — they have workspace chrome (header, sidebar) that you usually want to hide:

```
# Navigate to the published dashboard URL (cleaner than workspace embed)
browser_resize → width: 1920, height: 1080
browser_navigate → url: "https://your-workspace.databricks.com/sql/dashboardsv3/..."
browser_wait_for → time: 5                    # charts render
browser_take_screenshot → filename: "lakeview-dashboard.png"

# For individual widgets, use element screenshot after snapshot
browser_snapshot
browser_take_screenshot → element: "Map widget", ref: "e15", filename: "lakeview-map.png"
```

---

## 7. Databricks Workspace UI — Pipelines, Jobs, and Notebooks

The workspace UI has consistent chrome (top navbar, left sidebar) across all pages. These patterns help you capture clean screenshots of operational views.

### General Workspace Prep

```
# Use wide viewport — workspace sidebar eats ~250px
browser_resize → width: 1920, height: 1080
browser_navigate → url: "https://your-workspace.databricks.com/..."
browser_wait_for → time: 3

# Optional: collapse the left sidebar for more content area
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    const sidebar = document.querySelector('[data-testid=\"left-sidebar\"]') ||
                    document.querySelector('.left-sidebar');
    if (sidebar) sidebar.style.display = 'none';
  });
}"
```

### Pipelines (SDP / DLT)

**Pipeline DAG view** — the lineage graph showing table dependencies:

```
# Navigate to the pipeline
browser_navigate → url: "https://your-workspace.databricks.com/pipelines/{pipeline_id}"
browser_wait_for → time: 5                    # DAG renders async

# Wait for tables to appear in the graph
browser_wait_for → text: "bronze_"            # or any known table name

# The DAG can be wide — use fullPage or zoom out
browser_run_code → code: "async (page) => {
  await page.evaluate(() => { document.body.style.zoom = '80%'; });
  await page.waitForTimeout(1000);
}"
browser_take_screenshot → filename: "pipeline-dag-overview.png"
```

**Pipeline event log** — for showing update status, errors, or data quality:

```
# Click the "Events" tab first (use snapshot to find ref)
browser_snapshot
browser_click → ref: "{events_tab_ref}", element: "Events tab"
browser_wait_for → time: 2
browser_take_screenshot → filename: "pipeline-events.png"
```

**What makes a good pipeline screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| DAG view | All tables visible, connections clear, zoom fits the graph | Truncated graph, scroll needed, overlapping labels |
| Update status | Green checkmarks on all tables, timestamp visible | Mid-update with spinners (unless showing progress) |
| Event log | Filtered to relevant events, error highlighted | Unfiltered log with hundreds of INFO lines |
| Data quality | DQ tab open, expectations visible with pass/fail | Raw metrics without context |

### Jobs

**Job run list** — showing execution history:

```
browser_navigate → url: "https://your-workspace.databricks.com/jobs/{job_id}"
browser_wait_for → time: 3

# Show recent runs — wait for the run history table
browser_wait_for → text: "Succeeded"         # or "Failed" if showing an error
browser_take_screenshot → filename: "job-run-history.png"
```

**Job run detail** — task DAG for multi-task jobs:

```
browser_navigate → url: "https://your-workspace.databricks.com/jobs/{job_id}/runs/{run_id}"
browser_wait_for → time: 4                    # task graph renders

# The task DAG is the hero visual for job screenshots
browser_take_screenshot → filename: "job-task-dag.png"
```

**Job run output / logs** — for debugging or showing results:

```
# Navigate to a specific task's output
browser_navigate → url: "https://your-workspace.databricks.com/jobs/{job_id}/runs/{run_id}/tasks/{task_key}"
browser_wait_for → time: 3

# Scroll to output if needed
browser_run_code → code: "async (page) => {
  const output = document.querySelector('[data-testid=\"task-output\"]') ||
                 document.querySelector('.run-output');
  if (output) output.scrollIntoView({ behavior: 'instant', block: 'start' });
  await page.waitForTimeout(500);
}"
browser_take_screenshot → filename: "job-task-output.png"
```

**What makes a good job screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| Run history | 5-10 recent runs visible, mix of succeeded/failed tells a story | Single run with no context |
| Task DAG | All tasks visible with dependency arrows, status colors clear | Zoomed too tight on one task |
| Duration | Run duration column visible — readers care about performance | Duration cropped out |
| Errors | Error message visible, red status highlighted | Error truncated, needs scroll |

### Notebooks

**Notebook cells** — showing code + output:

```
browser_navigate → url: "https://your-workspace.databricks.com/#notebook/{notebook_id}"
browser_wait_for → time: 4                    # cells render

# Hide the notebook toolbar for cleaner shots
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    document.querySelectorAll('.notebook-toolbar, [data-testid=\"notebook-toolbar\"]').forEach(
      el => el.style.display = 'none'
    );
  });
}"
browser_take_screenshot → filename: "notebook-overview.png"
```

**Individual cell + output** — the most common notebook screenshot:

```
# Use snapshot to find the cell element ref
browser_snapshot

# Screenshot just the target cell
browser_take_screenshot → element: "Cell with spark.read output", ref: "{cell_ref}", filename: "notebook-cell-read.png"
```

**Notebook with visualizations** — charts rendered in cell output:

```
# Navigate to notebook, wait for cell execution
browser_wait_for → time: 5

# Scroll to the visualization cell
browser_run_code → code: "async (page) => {
  // Find cell output with a chart (canvas or SVG)
  const charts = document.querySelectorAll('.cell-output canvas, .cell-output svg');
  if (charts.length > 0) {
    charts[charts.length - 1].scrollIntoView({ behavior: 'instant', block: 'center' });
  }
  await page.waitForTimeout(1000);
}"
browser_take_screenshot → filename: "notebook-chart-output.png"
```

**What makes a good notebook screenshot:**

| Element | Good | Bad |
|---------|------|-----|
| Code cells | Syntax highlighted, readable font size, complete logic visible | Truncated code, scroll needed |
| Cell output | Table or chart fully rendered, no "Loading..." | Partial output, spinner visible |
| Narrative flow | 2-3 cells showing setup → transform → result | 15 cells crammed into one shot |
| Error screenshots | Error cell highlighted, traceback readable | Error message cropped |
| Comments | Markdown cells visible to provide context | Code-only with no explanation |

### Workspace URL Patterns

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

## 8. Annotation and Highlights

After capturing, you may need to annotate screenshots (arrows, callouts, highlights). Playwright doesn't do this natively, but you can add visual highlights before capture:

```
browser_run_code → code: "async (page) => {
  await page.evaluate(() => {
    // Add a red border highlight to a specific element
    const el = document.querySelector('.target-widget');
    if (el) {
      el.style.outline = '3px solid #FF4444';
      el.style.outlineOffset = '4px';
    }
  });
}"
```

For post-capture annotation (arrows, numbered callouts), use an external tool:
- **macOS Preview** — basic arrows and text
- **Skitch / Snagit** — richer annotation
- **Figma** — for polished blog graphics

---

## Pre-Screenshot Checklist

Before capturing any screenshot for publication:

- [ ] Viewport set to appropriate size for target medium
- [ ] Page fully loaded — no spinners, no placeholder tiles, no "Loading..."
- [ ] Map tiles fully rendered (waited 5-8 seconds after map appears)
- [ ] Unnecessary UI chrome hidden (debug panels, hamburger menus, footers)
- [ ] Text is readable at the display size (zoomed if needed)
- [ ] No PII or sensitive data visible (use sample/synthetic data)
- [ ] Light mode for documentation, dark mode for presentations
- [ ] Filename follows naming convention
- [ ] Consistent sizing with other screenshots in the same post/doc
- [ ] Pipeline DAGs show all tables with clear dependency arrows
- [ ] Job screenshots include run duration and status
- [ ] Notebook screenshots show 2-3 cells max (setup → result), not the entire notebook
