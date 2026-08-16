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
records were quarantined for every table.

The `tiny_messy` profile intentionally injects invalid records so the quarantine
path can be tested. This profile includes duplicate dimension keys, unknown
foreign keys, unsupported values, bad timestamps, negative measures, invalid
geography, and missing business values.

Quality validation is cascading:

```text
regions and service_plans
  -> towers and subscribers
  -> devices
  -> network_events, calls, data_sessions, tower_alarms
```

Child tables validate foreign keys against valid parent records, not just raw
Bronze parent records. This matters because a bad parent dimension can make many
dependent child records unsafe for analytics.

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

## Hardened Silver Controls

The Silver layer now applies first-version hardening controls before enrichment:

```text
valid Bronze records
  -> schema alignment
  -> event deduplication
  -> late-arrival tracking
  -> enriched Silver event tables
```

Current Silver controls:

| Control | Current Behavior |
| --- | --- |
| Deduplication | Event/fact tables keep the latest record per business primary key. |
| Idempotency | Enriched Silver outputs are overwritten deterministically on rerun. |
| Late-arriving records | Records delayed by more than 24 hours are flagged. |
| Schema evolution | Known optional columns are added when older batches do not contain them. |

The added Silver metadata columns are:

```text
_silver_was_deduplicated
_arrival_delay_hours
_late_arrival_threshold_hours
_is_late_arriving
source_system
producer_schema_version
```

Dimension duplicates are quarantined because reference data needs one trusted
version. Fact/event duplicates are handled in Silver because operational event
streams often deliver repeat records and Silver can apply a latest-record rule.

The `tiny_messy` Silver run proved the controls:

| Table | Duplicates Removed | Late-Arriving Records |
| --- | --- |
| `network_events_enriched` | 1 | 1 |
| `calls_enriched` | 0 | 1 |
| `data_sessions_enriched` | 1 | 1 |
| `tower_alarms_enriched` | 1 | 1 |

## Incremental Processing

TowerIQ now uses a local batch registry for incremental control:

```text
data/registry/batch_registry.json
```

The registry prevents repeated processing of a completed batch:

```text
new batch_id       -> run Bronze, Quality, Silver, Gold
completed batch_id -> skip
force rerun        -> run again intentionally
```

The first implementation supports production-style partitioned writes:

| Layer | Write Pattern |
| --- | --- |
| Bronze | Append records partitioned by `_pipeline_batch_id`. |
| Quality valid Silver | Append valid records partitioned by `_pipeline_batch_id`. |
| Quarantine | Append rejected records partitioned by `_pipeline_batch_id`. |
| Enriched Silver | Append enriched records partitioned by `event_date` and `_pipeline_batch_id`. |
| Gold | Recompute affected dates and dynamically overwrite only those `event_date` partitions. |

This means the pipeline no longer needs to rewrite every output folder for every
successful batch. Late-arriving events can identify older affected dates, and
Gold can recompute those dates instead of rebuilding unrelated days.

Future Silver improvements:

| Scenario | Silver Work Needed |
| --- | --- |
| Incremental batches | Process only new or changed partitions. |
| Out-of-order events | Recompute affected time windows safely. |
| More realistic duplicate batches | Track batch-level processing history. |
| Schema evolution | Add stricter compatibility checks and schema registry behavior. |
| SCD Type 2 | Join events to the correct historical dimension version. |

Silver reminder:

```text
When messy-data scenarios are introduced, the Silver layer must be revisited.
```

## SCD Type 2 Dimension History

TowerIQ builds SCD Type 2 history tables for core dimensions:

```text
data/silver/<profile>/scd2/regions/
data/silver/<profile>/scd2/service_plans/
data/silver/<profile>/scd2/towers/
data/silver/<profile>/scd2/subscribers/
data/silver/<profile>/scd2/devices/
```

SCD2 metadata columns:

```text
_scd_record_hash
_scd_valid_from
_scd_valid_to
_scd_is_current
_scd_loaded_at
```

The `tiny_scd2_demo` profile validates this behavior with two batches:

```text
2026-08-01 baseline batch
2026-08-15 changed-dimension batch
```

For changed records, the old version ends on `2026-08-14` and the new version
starts on `2026-08-15`. This proves that the project can preserve dimension
history instead of overwriting reference data.

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

## Analysis Notebooks

TowerIQ includes notebooks for manually inspecting each pipeline layer after the
jobs have written Parquet outputs.

Current notebooks:

```text
notebooks/bronze_layer_inspection.ipynb
notebooks/silver_layer_inspection.ipynb
notebooks/quarantine_layer_inspection.ipynb
notebooks/gold_kpi_baseline_analysis.ipynb
```

The notebooks are not separate pipeline jobs. They are analysis workspaces used
to query the existing Bronze, Silver, Quarantine, and Gold Parquet files and
confirm that the data makes sense layer by layer.

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
