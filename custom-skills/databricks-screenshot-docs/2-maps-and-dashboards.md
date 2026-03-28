# Maps and Dashboards

## Maps — Making Them Look Good

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
browser_run_code -> code: "async (page) => {
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

## Dashboards — Layout and Composition

### Hiding Distracting UI Elements

Databricks apps often have navbars, sidebars, or debug panels that clutter screenshots:

```
browser_run_code -> code: "async (page) => {
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
browser_take_screenshot -> element: "Revenue trend chart", ref: "e42", filename: "revenue-chart.png"
```

This produces tighter, more focused images — better for inline documentation.

### Full-Page Screenshots

For long scrolling pages (e.g., a multi-section Streamlit app):

```
browser_take_screenshot -> fullPage: true, filename: "full-app.png"
```

**Warning:** Full-page screenshots can be very tall. Use sparingly — they're good for architecture docs but bad for blog posts.

---

## Navigation

- [SKILL.md](SKILL.md) — Overview and quick reference
- [1-content-readiness.md](1-content-readiness.md) — Wait strategies and readability
- [3-workspace-ui.md](3-workspace-ui.md) — Workspace UI patterns
- [4-common-app-patterns.md](4-common-app-patterns.md) — Common Databricks app patterns
- [5-output-and-annotation.md](5-output-and-annotation.md) — Output format and annotation
