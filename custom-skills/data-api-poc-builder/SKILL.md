---
name: data-api-poc-builder
description: "ONLY invoke when the user EXPLICITLY asks for 'data-api-poc-builder', 'data-api-collector POC', or '/data-api-poc-builder'. This skill requires external services (Kafka, PostgreSQL, Neo4j via data-api-collector-python) and must NOT be triggered by general mentions of POCs, pipelines, dashboards, streaming, or SLED use cases. Do NOT auto-trigger on ambient keywords."
---

# Data API POC Builder

Build proof-of-concept Databricks artifacts — pipelines, dashboards, AI/ML models — on top of external Kafka, PostgreSQL, and Neo4j data sources. This skill bridges external data offerings with Databricks AI Dev Kit skills.

## Assumptions

- Data sources are already running and populated (Kafka topics have data, PG tables exist, Neo4j graphs are loaded)
- Databricks secrets are configured in a `data-api` scope (or user-specified scope)
- User has a Unity Catalog catalog and schema for output tables
- AI Dev Kit skills are installed and available

## Workflow

```
1. Identify use case → pre-built (9 available) or custom
2. Identify what to build → pipeline, dashboard, AI/ML, or combination
3. Provide blueprint → schemas, connection patterns, recommended approach
4. Delegate to AI Dev Kit skill → with pre-filled context
```

### Step 1: Identify the Use Case

Ask the user which use case they want to build on:

**Core streaming use cases:**
| Use Case | Kafka Topic | Domain |
|----------|-------------|--------|
| Fraud Detection | `streaming-fraud-transactions` | Financial crime detection |
| Telemetry | `streaming-device-telemetry` | IoT monitoring & anomaly detection |
| Web Traffic | `streaming-web-traffic` | Clickstream analytics |

**SLED use cases** (State/Local/Education — available across Kafka, PostgreSQL, and Neo4j):
| Use Case | Kafka Topic | Domain |
|----------|-------------|--------|
| Student Enrollment | `streaming-student-enrollment` | Academic records & course management |
| Grant & Budget | `streaming-grant-budget` | Public funding & vendor management |
| Citizen Services | `streaming-citizen-services` | 311 municipal service requests |
| K-12 Early Warning | `streaming-k12-early-warning` | Student risk indicators & interventions |
| Procurement | `streaming-procurement` | Contract & vendor management |
| Case Management | `streaming-case-management` | HHS client cases & benefits |

If the user describes something not in this list, follow the **Custom Use Cases** section below.

### Step 2: Identify What to Build

Ask what they want to build. Common patterns:
- **Pipeline only** — Ingest, transform, persist to Delta
- **Pipeline + Dashboard** — End-to-end from stream to visualization
- **Pipeline + AI/ML** — Ingest then apply models or AI functions
- **Full stack** — Pipeline + Dashboard + AI/ML + Job orchestration
- **Single component** — Just a dashboard on existing tables, or just an ML model

### Step 3: Provide Blueprint

Use the use-case blueprints in the **Use Case Catalog** section below to provide:
- Exact schemas for the data sources involved
- Connection patterns from the **Connection Patterns** section
- Recommended POC components and approach
- Specific SQL queries, model suggestions, or pipeline structures

### Step 4: Delegate to AI Dev Kit Skill

Route to the appropriate skill with use-case-specific context:

| Component | Delegate To | Context to Provide |
|-----------|-------------|-------------------|
| Streaming pipeline (Kafka) | `databricks-spark-declarative-pipelines` | Kafka topic, parsed schema, bronze/silver/gold table definitions |
| Batch ingestion (PostgreSQL) | `databricks-spark-declarative-pipelines` | JDBC config, table name, column schema |
| Graph ingestion (Neo4j) | Connection patterns (no dedicated skill) | Bolt config, node labels, Cypher queries |
| AI/BI Dashboard | `databricks-aibi-dashboards` | SQL queries against Delta tables |
| ML model / agent | `databricks-model-serving` | Feature columns, label, model type |
| AI Functions on data | `databricks-ai-functions` | Column names, classification/extraction prompts |
| Agent evaluation | `databricks-mlflow-evaluation` | Scorer suggestions, eval dataset shape |
| Job orchestration | `databricks-jobs` | Task dependency graph |
| Web app | `databricks-app-python` | Data source connection, viz framework |
| CI/CD bundle | `databricks-bundles` | Resource definitions |

When delegating, always provide the use-case-specific schemas, table names, and suggested queries so the downstream skill can generate the right artifact.

---

## Connection Patterns

All blueprints reference these shared connection configurations.

### Secrets Setup

All credentials are stored in a Databricks secret scope. Reference pattern:

```python
# Assumes a 'data-api' secret scope with these keys:
HOST = "your-hostname"  # or from secrets/widgets
API_KEY = dbutils.secrets.get(scope="data-api", key="api-key")
KAFKA_USER = dbutils.secrets.get(scope="data-api", key="kafka-user")
KAFKA_PASSWORD = dbutils.secrets.get(scope="data-api", key="kafka-password")
NEO4J_PASSWORD = dbutils.secrets.get(scope="data-api", key="neo4j-password")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="data-api", key="postgres-password")

HEADERS = {"X-Api-Key": API_KEY}
```

### Unity Catalog Target

All outputs go to a user-specified catalog and schema:

```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
```

### Kafka Structured Streaming

```python
KAFKA_JAAS = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", f"{HOST}:9093")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_JAAS)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse JSON payload
from pyspark.sql.functions import from_json, col
parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```

### PostgreSQL JDBC

```python
df = (
    spark.read
    .format("jdbc")
    .option("url", f"jdbc:postgresql://{HOST}:15433/datacollector?ssl=true")
    .option("dbtable", TABLE_NAME)
    .option("user", "dataapi")
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)
```

**Lakehouse Federation alternative (recommended for repeated access):**
```sql
CREATE CONNECTION postgres_conn
TYPE postgresql
OPTIONS (
    host '{HOST}',
    port '15433',
    user 'dataapi',
    password secret('data-api', 'postgres-password')
);

CREATE FOREIGN CATALOG postgres_data
USING CONNECTION postgres_conn
OPTIONS (database 'datacollector');

SELECT * FROM postgres_data.public.sled_students LIMIT 10;
```

### Neo4j Spark Connector

Requires cluster library: `org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3`

```python
# Read nodes
df = (
    spark.read
    .format("org.neo4j.spark.DataSource")
    .option("url", f"bolt+s://{HOST}:7688")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("labels", NODE_LABEL)
    .load()
)

# Read with Cypher query
df = (
    spark.read
    .format("org.neo4j.spark.DataSource")
    .option("url", f"bolt+s://{HOST}:7688")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", NEO4J_PASSWORD)
    .option("query", "MATCH (n)-[r]->(m) RETURN n, type(r) as rel, m LIMIT 1000")
    .load()
)
```

---

## Use Case Catalog

### Fraud Detection

**Data sources:**

Kafka topic `streaming-fraud-transactions`:
```
transaction_id  STRING
user_id         STRING
merchant_id     STRING
amount          DECIMAL(10,2)
currency        STRING
merchant_category STRING
payment_method  STRING
ip_address      STRING
device_id       STRING
latitude        DECIMAL(9,6)
longitude       DECIMAL(9,6)
card_type       STRING
event_timestamp TIMESTAMP
```

**Pipeline POC:**
- Bronze: Raw stream from Kafka → Delta table `bronze_fraud_transactions`
- Silver: Parse, deduplicate, cast types, add `is_high_value` flag (amount > threshold) → `silver_fraud_transactions`
- Gold: Aggregate by merchant_category, time window, card_type → `gold_fraud_summary`
- Delegate to `databricks-spark-declarative-pipelines` with this schema

**Dashboard POC:**
- Transaction volume over time (5-min windows)
- High-value transaction heatmap by latitude/longitude
- Top merchant categories by transaction count
- Amount distribution by payment method and card type
- Delegate to `databricks-aibi-dashboards` with SQL queries against gold tables

**AI/ML POC options:**
- `ai_classify` to flag suspicious transactions based on amount + merchant_category + time patterns
- `ai_extract` to identify risk signals from transaction metadata
- Anomaly detection model: train on amount, latitude/longitude, merchant_category features; label = high-risk threshold
- Delegate to `databricks-ai-functions` or `databricks-model-serving`

---

### Telemetry

**Data sources:**

Kafka topic `streaming-device-telemetry`:
```
device_id       STRING
device_type     STRING
reading         DECIMAL(10,2)
unit            STRING
battery_pct     INT
signal_dbm      INT
firmware        STRING
anomaly         BOOLEAN
latitude        DECIMAL(9,6)
longitude       DECIMAL(9,6)
event_timestamp TIMESTAMP
```

**Pipeline POC:**
- Bronze: Raw telemetry stream → `bronze_device_telemetry`
- Silver: Filter noise, normalize readings by device_type, flag low battery (< 20%) → `silver_device_telemetry`
- Gold: Average readings per device_type per hour, anomaly rate trends → `gold_telemetry_summary`

**Dashboard POC:**
- Device health overview (battery levels, signal strength distribution)
- Anomaly rate over time by device_type
- Geographic device distribution map
- Firmware version inventory

**AI/ML POC options:**
- `ai_classify` on reading + battery_pct + signal_dbm to predict device failure
- `ai_forecast` on aggregated readings for predictive maintenance
- Anomaly detection model on reading patterns by device_type

---

### Web Traffic

**Data sources:**

Kafka topic `streaming-web-traffic`:
```
session_id      STRING
user_id         STRING
page            STRING
action          STRING
referrer        STRING
http_status     INT
device          STRING
browser         STRING
duration_ms     INT
event_timestamp TIMESTAMP
```

**Pipeline POC:**
- Bronze: Raw clickstream → `bronze_web_traffic`
- Silver: Filter bot traffic (duration_ms < 100), sessionize, add page_category → `silver_web_traffic`
- Gold: Page views per hour, bounce rate by referrer, conversion funnel → `gold_web_analytics`

**Dashboard POC:**
- Page views and unique sessions over time
- Top pages by traffic volume
- Referrer breakdown (organic, direct, social)
- Device and browser distribution
- HTTP error rate (4xx, 5xx) trends

**AI/ML POC options:**
- `ai_classify` on session patterns to identify user intent (browsing, buying, support)
- `ai_extract` to categorize pages into content types
- Session-based conversion prediction model

---

### Student Enrollment

**Data sources:**

Kafka topic `streaming-student-enrollment`:
```
event_id        STRING
student_id      STRING
course_id       STRING
action          STRING
semester        STRING
department      STRING
grade           STRING
credits         INT
campus          STRING
gpa_impact      DECIMAL(3,2)
event_timestamp TIMESTAMP
```

PostgreSQL tables: `sled_students`, `sled_courses`, `sled_enrollment_events`

Neo4j nodes: `Student`, `Course`, `Department`, `DegreeProgram`
Neo4j relationships: `ENROLLED_IN`, `BELONGS_TO`, `PART_OF`

**Pipeline POC:**
- Bronze: Stream enrollment events → `bronze_enrollment`
- Silver: Join with student/course dimension tables (from PG via JDBC), validate credits → `silver_enrollment`
- Gold: Department enrollment trends, average GPA by department, credit load distribution → `gold_enrollment_analytics`

**Dashboard POC:**
- Enrollment counts by semester and department
- GPA distribution by campus
- Course popularity rankings
- Credit load histogram

**AI/ML POC options:**
- `ai_classify` to predict at-risk students based on GPA impact trends
- `ai_forecast` on enrollment counts by department for capacity planning
- Graph analytics: shortest path to degree completion, course prerequisite analysis

---

### Grant & Budget

**Data sources:**

Kafka topic `streaming-grant-budget`:
```
transaction_id    STRING
fund_source_id    STRING
agency_id         STRING
program_id        STRING
vendor_id         STRING
transaction_type  STRING
amount            DECIMAL(12,2)
fund_category     STRING
fiscal_year       INT
quarter           INT
cost_center       STRING
account_code      STRING
description       STRING
event_timestamp   TIMESTAMP
```

PostgreSQL tables: `sled_budget_transactions`, `sled_agencies`, `sled_vendors`

Neo4j nodes: `FundingSource`, `Agency`, `Program`, `Vendor`, `LineItem`
Neo4j relationships: `FUNDS`, `MANAGES`, `RECEIVES`, `SUPPLIES`

**Pipeline POC:**
- Bronze: Stream budget transactions → `bronze_budget`
- Silver: Enrich with agency/vendor names from PG, categorize by fund_category → `silver_budget`
- Gold: Spend by agency and quarter, vendor concentration, budget vs. actuals → `gold_budget_analytics`

**Dashboard POC:**
- Quarterly spend by agency
- Top vendors by total amount
- Fund category breakdown
- Year-over-year budget trends

**AI/ML POC options:**
- `ai_classify` transactions as routine vs. anomalous based on amount + vendor + category
- `ai_summarize` transaction descriptions for audit reporting
- Spend anomaly detection model by cost center

---

### Citizen Services

**Data sources:**

Kafka topic `streaming-citizen-services`:
```
request_id          STRING
citizen_id          STRING
request_type        STRING
department          STRING
status              STRING
priority            STRING
district            STRING
asset_id            STRING
latitude            DECIMAL(9,6)
longitude           DECIMAL(9,6)
response_time_hours DECIMAL(6,2)
satisfaction_rating INT
event_timestamp     TIMESTAMP
```

PostgreSQL tables: `sled_service_requests`, `sled_citizens`, `sled_assets`

Neo4j nodes: `Citizen`, `ServiceRequest`, `ServiceDepartment`, `Asset`, `District`
Neo4j relationships: `SUBMITTED`, `ASSIGNED_TO`, `LOCATED_IN`, `MAINTAINS`

**Pipeline POC:**
- Bronze: Stream 311 requests → `bronze_citizen_services`
- Silver: Enrich with asset details, geocode districts, calculate SLA compliance → `silver_citizen_services`
- Gold: Response time by department, satisfaction by district, request type trends → `gold_service_analytics`

**Dashboard POC:**
- Request volume heatmap by district (geo)
- Average response time by department and priority
- Satisfaction rating distribution
- Open vs. resolved request trends

**AI/ML POC options:**
- `ai_classify` requests by urgency based on request_type + description
- `ai_extract` to auto-categorize service requests
- `ai_forecast` request volume by district for resource planning
- Response time prediction model

---

### K-12 Early Warning

**Data sources:**

Kafka topic `streaming-k12-early-warning`:
```
event_id              STRING
student_id            STRING
school_id             STRING
event_type            STRING
grade_level           INT
teacher_id            STRING
risk_score            DECIMAL(4,2)
attendance_rate       DECIMAL(5,2)
gpa                   DECIMAL(3,2)
behavior_incidents_ytd INT
intervention_type     STRING
school_type           STRING
free_reduced_lunch    BOOLEAN
english_learner       BOOLEAN
special_education     BOOLEAN
event_timestamp       TIMESTAMP
```

PostgreSQL tables: `sled_k12_events`, `sled_k12_students`, `sled_schools`

Neo4j nodes: `K12Student`, `School`, `Teacher`, `RiskIndicator`, `Intervention`
Neo4j relationships: `ATTENDS`, `TEACHES`, `HAS_RISK`, `RECEIVED_INTERVENTION`

**Pipeline POC:**
- Bronze: Stream early warning events → `bronze_k12_warnings`
- Silver: Calculate rolling averages for attendance/GPA, flag declining trends → `silver_k12_warnings`
- Gold: Risk score distribution by school, intervention effectiveness, demographic breakdowns → `gold_k12_analytics`

**Dashboard POC:**
- Student risk score distribution by school and grade level
- Attendance rate trends over time
- Intervention type frequency and correlation with improved outcomes
- Demographic risk factor analysis (free/reduced lunch, EL, SPED)

**AI/ML POC options:**
- `ai_classify` students into risk tiers based on attendance + GPA + behavior
- Risk score prediction model using attendance_rate, gpa, behavior_incidents_ytd as features
- `ai_forecast` attendance trends by school for proactive intervention
- Intervention recommendation model

---

### Procurement

**Data sources:**

Kafka topic `streaming-procurement`:
```
event_id              STRING
agency_id             STRING
vendor_id             STRING
event_type            STRING
contract_id           STRING
amount                DECIMAL(12,2)
procurement_method    STRING
commodity_code        STRING
category              STRING
minority_owned        BOOLEAN
small_business        BOOLEAN
local_vendor          BOOLEAN
contract_duration_months INT
payment_terms         STRING
event_timestamp       TIMESTAMP
```

PostgreSQL tables: `sled_procurement_events`, `sled_contracts`

Neo4j nodes: `ProcAgency`, `ProcVendor`, `Contract`, `Lobbyist`
Neo4j relationships: `AWARDED`, `SUPPLIES`, `LOBBIES_FOR`, `MANAGES`

**Pipeline POC:**
- Bronze: Stream procurement events → `bronze_procurement`
- Silver: Enrich with contract details, categorize by procurement method → `silver_procurement`
- Gold: Spend by agency/vendor, diversity metrics (minority/small/local), contract duration analysis → `gold_procurement_analytics`

**Dashboard POC:**
- Total spend by agency and procurement method
- Diversity vendor metrics (% minority, small business, local)
- Contract duration and value distribution
- Top vendors by total awarded amount

**AI/ML POC options:**
- `ai_classify` contracts as high-risk based on amount + duration + vendor history
- `ai_extract` commodity codes and categories for spend taxonomy
- Vendor recommendation model based on agency + category + past performance
- Graph analytics: vendor-lobbyist relationship networks

---

### Case Management

**Data sources:**

Kafka topic `streaming-case-management`:
```
event_id        STRING
client_id       STRING
case_id         STRING
caseworker_id   STRING
event_type      STRING
program         STRING
agency_id       STRING
benefit_amount  DECIMAL(10,2)
household_size  INT
income_bracket  STRING
county          STRING
determination   STRING
referral_source STRING
priority        STRING
event_timestamp TIMESTAMP
```

PostgreSQL tables: `sled_case_events`, `sled_clients`, `sled_cases`

Neo4j nodes: `Client`, `Case`, `Caseworker`, `HHSAgency`, `HHSProgram`
Neo4j relationships: `HAS_CASE`, `ASSIGNED_TO`, `ENROLLED_IN`, `REFERRED_BY`

**Pipeline POC:**
- Bronze: Stream case events → `bronze_case_management`
- Silver: Enrich with client demographics, calculate time-to-determination → `silver_case_management`
- Gold: Caseload by worker, benefit distribution by program, determination rates → `gold_case_analytics`

**Dashboard POC:**
- Caseload distribution by caseworker and agency
- Benefit amount distribution by program and county
- Determination rates (approved/denied) by income bracket
- Referral source effectiveness

**AI/ML POC options:**
- `ai_classify` cases by urgency based on household_size + income_bracket + program
- `ai_forecast` caseload by county for staffing optimization
- Determination prediction model using household_size, income_bracket, program as features
- `ai_summarize` case event histories for caseworker handoff reports

---

## Custom Use Cases

When the user describes a scenario not in the catalog above:

1. **Ask about data sources** — Which systems are they working with? Kafka topic name, PostgreSQL table, Neo4j graph, or combination?
2. **Ask for schema** — What fields/columns are available? Or ask them to provide the topic/table name so you can reference it.
3. **Map to a blueprint template** — Structure the POC the same way as the pre-built use cases:
   - Data available (sources + schemas)
   - Pipeline POC (bronze/silver/gold)
   - Dashboard POC (key queries)
   - AI/ML POC (domain-appropriate suggestions)
4. **Delegate to AI Dev Kit skills** — Same routing table as pre-built use cases.

Reference the data-api-collector's **custom generator** capabilities for context on what data shapes are possible:
- **Custom Kafka generators** support: expressions, weighted categoricals, numeric ranges, string templates, null injection, date ranges, column derivation
- **Custom Neo4j generators** support: 14 property generator types (uuid, sequence, choice, range_int, range_float, bool, date, timestamp, name, email, phone, address, constant, null_or) with configurable relationships
- **Custom PostgreSQL generators** support: same 14 generator types with SQL type mapping and primary key designation

---

## Adapting for Audience

**Demo builders** (stakeholders, customer meetings):
- Prioritize visual impact: dashboards first, then the pipeline that feeds them
- Suggest full-stack POCs (pipeline + dashboard + one AI/ML feature)
- Keep pipeline logic simple — focus on the end result
- Recommend `databricks-bundles` for repeatable deployment

**Learners** (new to Databricks):
- Start with a single pipeline, explain each layer (bronze/silver/gold)
- Add dashboard after pipeline is working
- Introduce AI/ML as an extension
- Explain why each AI Dev Kit skill is being used
- Suggest `databricks-practice-skill` for hands-on exercises related to the patterns used
