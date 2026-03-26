# Distributions, Text Templates, and Date Ranges

## Non-Linear Distributions

**Never use uniform** — real data is rarely uniform. Use these distributions:

```python
import dbldatagen.distributions as dist

# Gamma — good for positive-skewed values (order amounts, response times)
.withColumn("amount", "double", minValue=0, maxValue=10000,
            distribution=dist.Gamma(shape=1.5, scale=200.0), random=True)

# Normal / Gaussian — good for measurements around a mean
.withColumn("latency_ms", "double", minValue=0, maxValue=2000,
            distribution=dist.Normal(mean=120, stddev=40), random=True)

# Exponential — good for inter-arrival times, resolution durations
# ⚠️ Exponential accepts `rate` (NOT `scale`). scale = 1/rate.
# rate=1/24 ≈ 0.0417 → mean ~24 hours
.withColumn("resolution_hours", "double", minValue=0, maxValue=500,
            distribution=dist.Exponential(rate=1/24), random=True)

# Beta — good for rates, probabilities (0-1 range)
.withColumn("error_rate", "double", minValue=0.0, maxValue=1.0,
            distribution=dist.Beta(alpha=2.0, beta=5.0), random=True)

# Weighted categorical (power-law-like effect)
.withColumn("tier", "string",
            values=["Free", "Pro", "Enterprise"], weights=[60, 30, 10],
            random=True)
```

### Distribution Selection Guide

| Distribution | Shape | Use Case | Parameters |
|-------------|-------|----------|------------|
| `Gamma(shape, scale)` | Right-skewed | Revenue, order amounts, wait times | shape: peak sharpness, scale: spread |
| `Normal(mean, stddev)` | Bell curve | Measurements, scores, physical values | mean: center, stddev: spread |
| `Exponential(rate)` | Steep decay | Inter-arrival times, durations, TTL | rate: 1/mean_value |
| `Beta(alpha, beta)` | Flexible 0-1 | Rates, probabilities, percentages | alpha>beta: right-skew, alpha<beta: left-skew |
| Weighted `values` | Categorical | Status, tier, category, region | weights: relative frequencies |

---

## Text Templates

Template characters: `d`=digit, `a`=lowercase alpha, `A`=uppercase, `\w`=random word, `\n`=number 0-255, `|`=OR alternative:

```python
.withColumn("email",    "string", template=r'\w.\w@\w.com|\w@\w.co.uk')
.withColumn("phone",    "string", template=r'(ddd)-ddd-dddd|1(ddd) ddd-dddd')
.withColumn("ip_addr",  "string", template=r'\n.\n.\n.\n')
.withColumn("order_id", "string", template=r'ORD-dddddd')
.withColumn("cust_id",  "string", template=r'CUST-ddddd')
.withColumn("sku",      "string", template=r'SKU-AAA-dddd')
.withColumn("mac_addr", "string", template=r'AA:AA:AA:AA:AA:AA')
.withColumn("uuid",     "string", template=r'AAAAdddd-dddd-dddd-dddd-AAAAdddddddd')
```

### Full Template Character Reference

| Char | Output | Example |
|------|--------|---------|
| `d` | Random digit (0-9) | `ddd` → `472` |
| `a` | Random lowercase letter | `aaa` → `qxm` |
| `A` | Random uppercase letter | `AAA` → `QXM` |
| `\w` | Random word from ipsum list | `\w` → `lorem` |
| `\n` | Random number 0-255 | `\n.\n.\n.\n` → `192.168.1.42` |
| `x` | Hex digit (lowercase) | `xxxx` → `a3f1` |
| `X` | Hex digit (uppercase) | `XXXX` → `A3F1` |
| `k` | Alphanumeric (lowercase) | `kkkk` → `a2b3` |
| `K` | Alphanumeric (uppercase) | `KKKK` → `A2B3` |
| `\|` | OR alternative | `\w.com\|\w.org` → either `.com` or `.org` |

Custom word list for realistic domain-specific names:

```python
products = ['widget', 'gadget', 'module', 'device', 'sensor']
.withColumn("product_name", "string",
            text=dg.TemplateGenerator(r'\w Pro|\w Plus|\w Enterprise',
                                      extendedWordList=products))
```

### ILText — Lorem Ipsum Text Generation

Generate multi-paragraph text for description, comment, or body fields:

```python
# Generate 1-4 paragraphs, each with 2-6 sentences
.withColumn("description", "string",
            text=dg.ILText(paragraphs=(1, 4), sentences=(2, 6)))

# Short descriptions (single paragraph, few sentences)
.withColumn("summary", "string",
            text=dg.ILText(paragraphs=(1, 1), sentences=(1, 3)))

# Long content (articles, documents)
.withColumn("body", "string",
            text=dg.ILText(paragraphs=(3, 8), sentences=(4, 10)))
```

---

## Faker Integration

Use Faker for realistic names, addresses, and locale-specific text. Requires `%pip install faker`.

```python
from dbldatagen import FakerTextFactory, fakerText

# Default Faker (en_US)
.withColumn("full_name", "string", text=fakerText("name"))
.withColumn("address", "string", text=fakerText("address"))
.withColumn("company", "string", text=fakerText("company"))
.withColumn("credit_card", "string", text=fakerText("credit_card_number"))

# Locale-specific Faker
from faker.providers import internet

FakerTextIT = FakerTextFactory(locale=['it_IT'], providers=[internet])

.withColumn("italian_name", "string", text=FakerTextIT("name"))
.withColumn("italian_email", "string", text=FakerTextIT("email"))
```

**Caveats:**
- Faker-based text is **not repeatable** across runs (no seed integration)
- Performance is slower than native templates -- fine up to ~100M rows on medium clusters
- Requires `%pip install faker` (install via `execute_databricks_command` MCP tool)

### Custom Python Text Functions (PyfuncText)

For arbitrary text generation logic using any Python code:

```python
from dbldatagen import PyfuncText

def init_context(context):
    context.cities = ["New York", "London", "Tokyo", "Paris", "Sydney"]

def generate_text(context, v):
    return context.cities[abs(hash(str(v))) % len(context.cities)]

.withColumn("city", "string",
            text=PyfuncText(generate_text, initFn=init_context))
```

Each worker node gets its own context instance -- no serialization needed. Context persists across Pandas UDF calls for efficiency.

---

## Date and Time Ranges

Use `dg.DateRange` for controlled date generation:

```python
from datetime import datetime, timedelta

end_dt   = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_dt = end_dt - timedelta(days=180)

START = start_dt.strftime("%Y-%m-%d 00:00:00")
END   = end_dt.strftime("%Y-%m-%d 23:59:59")

# Date column — random day within range
.withColumn("event_date", "date",
            data_range=dg.DateRange(START, END, "days=1"),
            random=True)

# Timestamp column — random minute within range
.withColumn("event_ts", "timestamp",
            data_range=dg.DateRange(START, END, "minutes=1"),
            random=True)

# Derived date (e.g., resolved_at = created_at + delay)
.withColumn("delay_days", "int", minValue=1, maxValue=30, random=True, omit=True)
.withColumn("resolved_at", "date",
            expr="date_add(event_date, delay_days)",
            baseColumn=["event_date", "delay_days"])
```
