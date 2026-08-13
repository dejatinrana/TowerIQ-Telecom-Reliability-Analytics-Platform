# Architecture

TowerIQ models a telecom reliability analytics platform. Raw operational data
from towers, subscribers, devices, calls, sessions, and alarms will be processed
into trusted datasets that support tower health, congestion, dropped-call,
outage, and regional performance analysis.

The business reason for the platform is to help a telecom operations team convert
messy network activity into reliable KPIs. The engineering reason this domain is
valuable is that telecom data naturally creates distributed data-processing
problems: high volume, late events, duplicate batches, invalid records, skewed
tower traffic, large joins, time-based partitions, and incremental corrections.

Initial target architecture:

```text
Raw telecom sources
  -> Bronze ingestion
  -> Data quality validation
  -> Silver cleaned/enriched data
  -> Gold telecom KPIs
```

## Bronze Ingestion

Bronze is the first controlled copy of raw source data. It reads raw CSV files
with explicit schemas and writes Parquet outputs while preserving the source
columns.

Bronze adds standard technical metadata:

```text
_bronze_loaded_at
_bronze_table
_dataset_profile
_source_format
```

For the tiny profile, Bronze outputs are written to:

```text
data/bronze/tiny/<table_name>/
```

The first Bronze ingestion verified that raw and Bronze row counts match for all
nine source tables.

## Data Quality And Quarantine

Data quality validation starts after Bronze. The first implemented quality layer
covers all nine first-version TowerIQ tables:

```text
regions
towers
service_plans
subscribers
devices
network_events
calls
data_sessions
tower_alarms
```

The quality job reads Bronze Parquet tables from:

```text
data/bronze/tiny/<table_name>/
```

It applies table-specific rules. Examples include:

```text
primary keys are present
primary keys are unique
foreign keys exist in reference tables
timestamps are present and logically ordered
network, status, plan, device, alarm, and event values are supported
numeric measures such as latency, bytes, price, capacity, and duration are valid
geographical values such as latitude and longitude are in valid ranges
```

Valid records are written to:

```text
data/silver/tiny/<table_name>/
```

This is the first Silver output layer: it contains records that passed table
quality checks. It is not the full Silver transformation phase yet.

Invalid records are written to:

```text
data/quarantine/tiny/<table_name>/
```

Quarantined records include rejection metadata:

```text
_rejection_reasons
_quarantined_at
_source_table
```

For the clean tiny baseline dataset, all records passed validation and `0`
records were quarantined for every table. Bad-record injection will be added
later so the quarantine path can be tested with intentionally invalid data.

## Enriched Silver Transformations

The enriched Silver phase creates analytical event tables from the valid Silver
records.

Enriched outputs are written to:

```text
data/silver/tiny/enriched/<table_name>/
```

The first enriched Silver tables are:

```text
network_events_enriched
calls_enriched
data_sessions_enriched
tower_alarms_enriched
```

These tables add business context from dimensions such as:

```text
regions
towers
subscribers
devices
service_plans
```

Examples of enriched columns include:

```text
region_id
region_name
zone
region_type
tower_type
capacity_score
customer_segment
plan_type
priority_level
manufacturer
supports_5g
event_date
event_hour
```

Some event-specific derived fields are also added:

```text
is_dropped_call
is_failed_call
total_bytes
total_mb
is_failed_session
is_critical_alarm
```

The first enriched Silver run preserved source row counts:

| Table | Input Rows | Output Rows |
| --- | ---: | ---: |
| `network_events_enriched` | 50,000 | 50,000 |
| `calls_enriched` | 20,000 | 20,000 |
| `data_sessions_enriched` | 30,000 | 30,000 |
| `tower_alarms_enriched` | 800 | 800 |

## Silver Maturity Roadmap

The current Silver layer is a clean baseline:

```text
valid Bronze records
  -> enriched Silver event tables
```

More advanced Silver behavior will be added when the related messy-data
scenarios are introduced.

Future Silver improvements:

| Scenario | Silver Work Needed |
| --- | --- |
| Duplicate events | Deduplicate by event-specific business keys. |
| Duplicate batches | Make reruns safe and prevent repeated processing. |
| Late-arriving data | Handle event time separately from ingestion time. |
| Out-of-order events | Preserve event-time correctness in downstream tables. |
| Bad reference data | Define enrichment behavior when dimensions are missing or invalid. |
| Schema evolution | Support controlled source schema changes. |
| SCD Type 2 | Join events to the correct historical dimension version. |

Silver reminder:

```text
When messy-data scenarios are introduced, the Silver layer must be revisited.
```

## Gold KPI Layer

Gold is the business-facing analytics layer. It is built from enriched Silver
tables, not directly from Raw or Bronze.

The first Gold outputs are written to:

```text
data/gold/tiny/<table_name>/
```

The first Gold KPI tables are:

```text
tower_daily_kpis
region_daily_kpis
network_type_daily_kpis
subscriber_segment_daily_kpis
```

These tables answer telecom reliability questions such as:

```text
Which towers have worse health scores?
Which regions have high failure or dropped-call rates?
Which network technologies have higher latency or failure rates?
Which subscriber segments and plan types are more affected?
```

Example Gold metrics:

```text
network_failure_rate
dropped_call_rate
failed_session_rate
avg_network_latency_ms
avg_session_latency_ms
total_data_mb
critical_alarms
tower_health_score
```

The first Gold run created:

| Table | Rows |
| --- | ---: |
| `tower_daily_kpis` | 560 |
| `region_daily_kpis` | 56 |
| `network_type_daily_kpis` | 21 |
| `subscriber_segment_daily_kpis` | 119 |

See [Gold KPI Question Bank](gold_kpi_question_bank.md) for examples of the
business questions these tables can answer.

The first dataset design follows a star-schema-inspired model. Fact tables store
network activity such as calls, data sessions, network events, and tower alarms.
Dimension tables describe the business context such as regions, towers,
subscribers, devices, and service plans.

See [Data Model](data_model.md) for the simple fact/dimension explanation and
the first planned tables.

## Local-First, Production-Portable Design

TowerIQ is developed locally first, but the code should be written so it can
move to production storage and compute with minimal changes.

Local development uses folder-based data lake zones:

```text
data/raw/
data/bronze/
data/silver/
data/gold/
data/quarantine/
```

In production, the same zones could be represented in cloud data lake storage:

```text
s3://toweriq-data-lake/raw/
s3://toweriq-data-lake/bronze/
s3://toweriq-data-lake/silver/
s3://toweriq-data-lake/gold/
s3://toweriq-data-lake/quarantine/
```

The important project idea is that Raw, Bronze, Silver, Gold, and Quarantine are
separate storage zones, whether the project is running locally or in production.

## File Format Strategy

Raw source data starts as CSV because it simulates extracts from upstream telecom
systems.

Processed pipeline layers should use Parquet:

```text
Raw: CSV
Bronze: Parquet
Silver: Parquet
Gold: Parquet
Quarantine: Parquet
```

Parquet is better for Spark analytics because it is columnar, compressed,
schema-aware, and supports efficient query behavior such as column pruning and
predicate pushdown.

## Querying Parquet

Parquet does not need to be converted to CSV before querying.

Locally, TowerIQ can query Parquet using Spark SQL or DuckDB. In production,
Parquet in a data lake can be queried directly by tools such as Databricks SQL,
Spark SQL, Trino, Athena, BigQuery external tables, Snowflake external tables,
or Redshift Spectrum.

Selected Gold summaries may later be exported to a serving database such as
PostgreSQL, but Bronze/Silver/event-level Gold data should remain in the data
lake.

## Dataset Scale Strategy

TowerIQ starts with a tiny dataset to prove correctness before scaling.

```text
tiny        -> quick local debugging and correctness checks
development -> normal local pipeline testing
medium      -> integration and moderate Spark behavior testing
large       -> selected performance experiments only
```

Dataset profiles should be stored separately so larger runs do not overwrite
small test data:

```text
data/raw/tiny/
data/raw/development/
data/raw/medium/
data/raw/large/
```

The guiding rule is:

```text
Correctness first, scale second, optimization third.
```

This file will evolve as the platform grows.
