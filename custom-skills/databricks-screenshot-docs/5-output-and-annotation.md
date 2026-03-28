# Output Format and Annotation

## Format Selection

| Format | When to Use |
|--------|-------------|
| **PNG** | Default. Lossless, crisp text, transparent backgrounds. Best for docs and blogs. |
| **JPEG** | Photos, map screenshots with many tile colors. Smaller file size, lossy compression. |

**Rule of thumb:** If the screenshot has text or UI elements, use PNG. If it's mostly a map with minimal UI, JPEG is fine.

---

## File Naming Convention

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

---

## Output Directory

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

## Annotation and Highlights

After capturing, you may need to annotate screenshots (arrows, callouts, highlights). Playwright doesn't do this natively, but you can add visual highlights before capture:

```
browser_run_code -> code: "async (page) => {
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

## Navigation

- [SKILL.md](SKILL.md) — Overview and quick reference
- [1-content-readiness.md](1-content-readiness.md) — Wait strategies and readability
- [2-maps-and-dashboards.md](2-maps-and-dashboards.md) — Maps and dashboard composition
- [3-workspace-ui.md](3-workspace-ui.md) — Workspace UI patterns
- [4-common-app-patterns.md](4-common-app-patterns.md) — Common Databricks app patterns
