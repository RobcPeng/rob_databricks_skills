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
browser_resize -> width: 1280, height: 720
browser_navigate -> url: "https://your-app.databricks.app/"
browser_wait_for -> time: 3          # let content render
browser_take_screenshot -> filename: "hero-dashboard.png", type: "png"
```

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
- [ ] Notebook screenshots show 2-3 cells max (setup -> result), not the entire notebook

---

## Detailed Guides

| Guide | File | Contents |
|-------|------|----------|
| Content Readiness | [1-content-readiness.md](1-content-readiness.md) | Wait strategies, map-specific waits, readability/zoom, data table tips |
| Maps and Dashboards | [2-maps-and-dashboards.md](2-maps-and-dashboards.md) | Map zoom levels, viewport scripting, dashboard composition, element screenshots |
| Workspace UI | [3-workspace-ui.md](3-workspace-ui.md) | Pipelines (DLT), jobs, notebooks, workspace URL patterns |
| Common App Patterns | [4-common-app-patterns.md](4-common-app-patterns.md) | Streamlit, Dash, Gradio, AI/BI Lakeview dashboard recipes |
| Output and Annotation | [5-output-and-annotation.md](5-output-and-annotation.md) | Format selection, file naming, output directories, annotation techniques |

---

## Related Skills

- [databricks-app-python](../databricks-app-python/SKILL.md) — Building Python Databricks apps (Dash, Streamlit, Gradio, Flask, FastAPI)
- [databricks-aibi-dashboards](../databricks-aibi-dashboards/SKILL.md) — Creating and deploying AI/BI Lakeview dashboards
