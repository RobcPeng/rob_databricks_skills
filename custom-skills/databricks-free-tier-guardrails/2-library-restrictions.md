# Library Restrictions

Free tier supports a **fixed set of pre-installed libraries**. Treat every `%pip install` as a risk.

| Library | Available? | Notes |
|---------|------------|-------|
| `pyspark` | Pre-installed | Core, always available |
| `pandas` | Pre-installed | Available, but avoid for large data |
| `numpy` | Pre-installed | Always available |
| `mlflow` | Pre-installed | Always available |
| `scikit-learn` | Pre-installed | Available for ML |
| `matplotlib` / `seaborn` | Pre-installed | Available for visualization |
| `requests` | Pre-installed | HTTP client |
| `faker` | Requires `%pip install` | Works fine on serverless |
| `dbldatagen` | Requires `%pip install` | Pin `>=0.4.0` (older versions use `sparkContext`) |
| `geopandas` | Requires `%pip install` | Works but pulls heavy deps (GDAL) |
| `arcgis` | Requires `%pip install` | May fail due to system deps |
| `tensorflow` / `torch` | Requires `%pip install` | CPU only, very slow, not recommended |
| `holidays` | Requires `%pip install` | Works fine |
| `scipy` | Sometimes available | Don't rely on it being pre-installed |
| Custom wheels / private packages | Not supported | Requires classic cluster |
| Packages needing C compilation | Risky | May fail without system libraries |

**Rule:** If a library requires `%pip install`, flag it. Prefer native PySpark SQL functions (`rand()`, `randn()`, `expr()`, `sequence()`, `date_add()`, etc.) for data generation tasks.

## Safe Package Installation Pattern

```python
# Always use %pip (not pip or !pip) in notebooks
%pip install faker dbldatagen>=0.4.0

# In scripts executed via MCP, install first via execute_databricks_command:
# Tool: execute_databricks_command
# code: "%pip install faker dbldatagen>=0.4.0"
```

---

# Native PySpark SQL — Always Safe

These require zero external dependencies and work under Spark Connect:

```python
from pyspark.sql import functions as F
from pyspark.sql import Window

# Random numbers
F.rand(seed=42)          # uniform [0, 1)
F.randn(seed=42)         # standard normal N(0,1)

# Categorical assignment without loops
F.when(F.rand() < 0.2, "AAPL").when(F.rand() < 0.5, "GOOG").otherwise("MSFT")

# Expr for SQL-style expressions
F.expr("round(900 + randn() * 4.5, 4)")

# Date/time manipulation
F.date_add("col", 5)
F.to_timestamp("col", "yyyy-MM-dd HH:mm:ss")
F.unix_timestamp("col")
F.current_timestamp()
F.date_format("col", "yyyy-MM-dd")
F.months_between("end_date", "start_date")

# Sequences (use spark.range instead of Python range)
spark.range(100_000)     # distributed; never use Python range() + collect

# String functions
F.concat_ws(" ", "first", "last")
F.regexp_replace("col", "pattern", "replacement")
F.substring("col", 1, 5)

# Array and struct operations
F.array("col1", "col2", "col3")
F.struct("col1", "col2")
F.explode("array_col")
F.size("array_col")

# Window functions
window = Window.partitionBy("group").orderBy("date")
F.row_number().over(window)
F.lag("col", 1).over(window)
F.sum("amount").over(window)

# Type conversions
F.col("str_col").cast("int")
F.col("epoch").cast("timestamp")

# Geospatial (DBR 17.1+ serverless)
F.expr("ST_POINT(lon, lat)")
F.expr("H3_LONGLATASH3STRING(lon, lat, 9)")
F.expr("ST_DISTANCESPHERE(ST_POINT(a.lon, a.lat), ST_POINT(b.lon, b.lat))")
```

---

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-banned-apis.md](1-banned-apis.md) — Banned APIs and detection regex
- [3-resource-config.md](3-resource-config.md) — Resource configuration (Pipelines, Jobs, Notebooks, etc.)
- [4-common-fixes.md](4-common-fixes.md) — Common patterns to fix
- [5-full-checklist.md](5-full-checklist.md) — Full checklist
