# Content Readiness — Wait Before You Shoot

Databricks apps render asynchronously. A screenshot taken too early captures spinners, empty charts, or half-loaded maps.

## Wait Strategies

| Content Type | Wait Strategy | Tool |
|-------------|--------------|------|
| Static page | Wait 2-3 seconds after navigate | `browser_wait_for` -> `time: 3` |
| Data tables | Wait for row text to appear | `browser_wait_for` -> `text: "showing 1-25"` |
| Charts (Plotly, matplotlib) | Wait for axis label or legend | `browser_wait_for` -> `text: "Revenue"` |
| Maps (kepler, folium) | Wait 5-8 seconds for tile load | `browser_wait_for` -> `time: 8` |
| Loading spinners | Wait for spinner to disappear | `browser_wait_for` -> `textGone: "Loading..."` |
| Streamlit apps | Wait for Streamlit loaded marker | `browser_wait_for` -> `textGone: "Please wait..."` |
| Dash apps | Wait for callback completion | `browser_wait_for` -> `time: 4` |

## Map-Specific Waits

Maps are the hardest to screenshot well. Tile layers load asynchronously from external CDNs.

```
# 1. Navigate to the page with the map
browser_navigate -> url: "https://your-app.databricks.app/map"

# 2. Wait for the map framework to initialize
browser_wait_for -> text: "Leaflet" or time: 5

# 3. Wait extra for tile layers to fully render
browser_wait_for -> time: 8

# 4. THEN screenshot
browser_take_screenshot -> filename: "map-overview.png"
```

**If tiles are still loading:** Use `browser_run_code` to force a longer wait:
```
browser_run_code -> code: "async (page) => { await page.waitForTimeout(10000); }"
```

---

## Readability and Zoom

### Font Size and Zoom

If text is too small to read at the screenshot's display size, zoom the page before capturing:

```
browser_run_code -> code: "async (page) => {
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

## Navigation

- [SKILL.md](SKILL.md) — Overview and quick reference
- [2-maps-and-dashboards.md](2-maps-and-dashboards.md) — Maps and dashboard composition
- [3-workspace-ui.md](3-workspace-ui.md) — Workspace UI patterns
- [4-common-app-patterns.md](4-common-app-patterns.md) — Common Databricks app patterns
- [5-output-and-annotation.md](5-output-and-annotation.md) — Output format and annotation
