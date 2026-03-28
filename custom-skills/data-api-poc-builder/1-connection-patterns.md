# Connection Patterns

All blueprints reference these shared connection configurations.

## Secrets Setup

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

## Unity Catalog Target

All outputs go to a user-specified catalog and schema:

```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
```

## Kafka Structured Streaming

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

> **⚠️ Stale Kafka messages after generator restart.** Stopping a data-api-collector generator and starting a new one does **not** clear the topic. Old messages from previous runs persist, so `startingOffsets="earliest"` will consume stale data first — which is deceptive when old and new specs share column names but differ in generation logic. To avoid confusion:
> 1. Use a **new topic name** for each test iteration
> 2. Consume only **recent offsets** (not `"earliest"`) during debugging
> 3. **Delete and recreate the topic** between test runs

> **⚠️ Batch Kafka reads:** If you inspect a topic with `spark.read.format("kafka")` (batch mode), `startingOffsets="latest"` is invalid — it only works with `readStream`. For batch reads, use `startingOffsets="earliest"` with `endingOffsets="latest"` to read the full topic contents.

## PostgreSQL JDBC

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

## Neo4j Spark Connector

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

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [2-use-case-catalog.md](2-use-case-catalog.md) — Pre-built use case blueprints
- [3-custom-use-cases.md](3-custom-use-cases.md) — Custom use cases and audience adaptation
