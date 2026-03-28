# Use Case Catalog

## Fraud Detection

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

## Telemetry

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

## Web Traffic

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

## Student Enrollment

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

## Grant & Budget

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

## Citizen Services

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

## K-12 Early Warning

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

## Procurement

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

## Case Management

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

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-connection-patterns.md](1-connection-patterns.md) — Connection patterns and credentials
- [3-custom-use-cases.md](3-custom-use-cases.md) — Custom use cases and audience adaptation
