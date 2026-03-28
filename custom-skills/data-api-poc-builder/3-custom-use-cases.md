# Custom Use Cases

When the user describes a scenario not in the catalog:

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

---

## Navigation

- [SKILL.md](SKILL.md) — Main entry point
- [1-connection-patterns.md](1-connection-patterns.md) — Connection patterns and credentials
- [2-use-case-catalog.md](2-use-case-catalog.md) — Pre-built use case blueprints
