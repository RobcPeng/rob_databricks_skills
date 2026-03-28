# Common Databricks App Patterns

## Streamlit Apps

```
# 1. Set viewport
browser_resize -> width: 1280, height: 720

# 2. Navigate
browser_navigate -> url: "https://your-app.databricks.app/"

# 3. Wait for Streamlit to finish loading
browser_wait_for -> textGone: "Please wait..."
browser_wait_for -> time: 2

# 4. Hide Streamlit chrome
browser_run_code -> code: "async (page) => {
  await page.evaluate(() => {
    document.querySelectorAll('[data-testid=\"stHeader\"], footer, [data-testid=\"stToolbar\"]').forEach(
      el => el.style.display = 'none'
    );
  });
}"

# 5. Screenshot
browser_take_screenshot -> filename: "streamlit-app.png"
```

---

## Dash Apps

```
# Dash apps load faster but callbacks can be slow
browser_resize -> width: 1280, height: 720
browser_navigate -> url: "https://your-app.databricks.app/"
browser_wait_for -> time: 4                    # callbacks complete
browser_take_screenshot -> filename: "dash-app.png"
```

---

## Gradio Apps

```
# Gradio has its own loading indicator
browser_resize -> width: 1280, height: 720
browser_navigate -> url: "https://your-app.databricks.app/"
browser_wait_for -> textGone: "Loading..."
browser_wait_for -> time: 2
browser_take_screenshot -> filename: "gradio-app.png"
```

---

## AI/BI Dashboards (Lakeview)

Lakeview dashboards are embedded in the Databricks workspace UI — they have workspace chrome (header, sidebar) that you usually want to hide:

```
# Navigate to the published dashboard URL (cleaner than workspace embed)
browser_resize -> width: 1920, height: 1080
browser_navigate -> url: "https://your-workspace.databricks.com/sql/dashboardsv3/..."
browser_wait_for -> time: 5                    # charts render
browser_take_screenshot -> filename: "lakeview-dashboard.png"

# For individual widgets, use element screenshot after snapshot
browser_snapshot
browser_take_screenshot -> element: "Map widget", ref: "e15", filename: "lakeview-map.png"
```

---

## Navigation

- [SKILL.md](SKILL.md) — Overview and quick reference
- [1-content-readiness.md](1-content-readiness.md) — Wait strategies and readability
- [2-maps-and-dashboards.md](2-maps-and-dashboards.md) — Maps and dashboard composition
- [3-workspace-ui.md](3-workspace-ui.md) — Workspace UI patterns
- [5-output-and-annotation.md](5-output-and-annotation.md) — Output format and annotation
