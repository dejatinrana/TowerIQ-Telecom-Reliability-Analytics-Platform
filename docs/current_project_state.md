# Current Project State

This document summarizes what has been completed in TowerIQ so far and how the
main pieces connect.

## Completed So Far

TowerIQ currently has a working local data lake flow for the clean `tiny`
dataset and the intentionally messy `tiny_messy` dataset:

```text
Raw CSV
  -> Bronze Parquet
  -> Data quality checks
  -> Silver valid and enriched Parquet
  -> Gold KPI Parquet
  -> Analysis notebooks
```

The goal is to model how a telecom analytics platform turns operational network
activity into trusted reliability metrics.

## Data Generated

The synthetic telecom dataset includes nine source tables:

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

These tables represent the core telecom story:

- Regions contain towers.
- Towers generate events, calls, data sessions, and alarms.
- Subscribers use devices and service plans.
- Calls and data sessions connect subscribers, devices, towers, and network
  technologies.
- Gold KPIs summarize this activity into business metrics.

## Bronze Layer

Bronze is the first controlled copy of the raw source data.

What Bronze does:

- Reads raw CSV files.
- Applies explicit schemas for all source tables.
- Adds technical metadata.
- Writes Parquet files to the Bronze zone.
- Preserves the original source-level records for traceability.

Bronze answers:

- Did we receive the expected tables?
- Did row counts move from Raw to Bronze correctly?
- Can Spark read the data with known schemas?

## Data Quality And Quarantine

The quality layer validates all nine first-version tables.

What it checks:

- Required primary keys are present.
- Primary keys are unique.
- Foreign keys point to valid reference records.
- Timestamps are valid and logically ordered.
- Numeric values are within valid ranges.
- Telecom categories such as network type, call status, device type, plan type,
  and alarm severity are supported.

Valid records are written to Silver. Invalid records are written to Quarantine
with rejection reasons.

For the clean tiny dataset, all rows pass and quarantine outputs are empty.

For the messy tiny dataset, intentional bad records are injected to test
quarantine behavior. Examples include duplicate dimension keys, unknown foreign
keys, unsupported statuses, invalid timestamps, invalid coordinates, negative
measures, and missing business values.

Quality validation is cascading. Parent dimensions are validated first, and
child tables are checked against valid parent records. This prevents bad
dimension records from silently contaminating downstream Silver and Gold data.

## Silver Layer

Silver currently has two parts.

The first Silver part stores valid records that passed quality checks.

The enriched Silver part joins valid facts with dimensions so analytics tables
do not need to repeat the same joins.

Current enriched Silver tables:

```text
network_events_enriched
calls_enriched
data_sessions_enriched
tower_alarms_enriched
```

These tables add business context such as region, zone, tower type, customer
segment, service plan, device manufacturer, and event date.

Silver now includes first-version hardening controls:

- Event deduplication by business primary key.
- Idempotent full-table reruns using deterministic overwrite outputs.
- Late-arriving record flags based on ingestion time minus event time.
- Optional schema-evolution columns for future producer metadata.

Fact/event duplicates are handled in Silver because events can arrive more than
once from upstream systems. Dimension duplicates are still quarantined because
master/reference records need one trusted version before child records can rely
on them.

## Incremental Processing

TowerIQ now has first-version production-style incremental processing.

The incremental runner tracks each pipeline batch in a local registry:

```text
data/registry/batch_registry.json
```

The registry records:

- `run_id`
- `profile`
- `batch_id`
- run status
- start and completion timestamps
- Bronze, Quality, Silver, and Gold stage summaries

If a batch already completed, the incremental runner skips it unless
`--force-rerun` is used.

The write strategy is now partition-oriented:

| Layer | Current Write Strategy |
| --- | --- |
| Bronze | Append by `_pipeline_batch_id`. |
| Quality valid Silver | Append by `_pipeline_batch_id`. |
| Quarantine | Append by `_pipeline_batch_id`. |
| Enriched Silver | Append by `event_date` and `_pipeline_batch_id`. |
| Gold | Recompute affected `event_date` values and overwrite only those date partitions. |

This is closer to a production data lake pattern than full-table rebuilds.

## Gold Layer

Gold is the business-facing analytics layer.

Current Gold KPI tables:

```text
tower_daily_kpis
region_daily_kpis
network_type_daily_kpis
subscriber_segment_daily_kpis
```

These tables help answer questions such as:

- Which towers have the lowest health score?
- Which regions have the highest failure rates?
- Which network type has the highest latency?
- Which subscriber segments are most affected?
- Which towers should be prioritized for maintenance?

## Fast Local Development Workflow

TowerIQ now has a developer-focused runner for quick local feedback:

```bash
python3 -m src.jobs.run_dev_stage --stage silver
```

This runner uses:

```text
configs/dev_fast.yaml
```

It can run only one stage at a time:

```text
bronze
quality
scd2
silver
gold
all
```

This is useful because small local Spark datasets are often dominated by fixed
Spark overhead instead of row-processing time. If only Silver code changed, we
can rerun only Silver and Gold instead of paying the cost of Raw, Bronze, and
Quality again.

The normal full pipeline is still used for checkpoint validation. The fast
runner is for day-to-day coding feedback.

Latest measured dev-fast result:

| Run | Wall Time |
| --- | ---: |
| Full affected flow for `network_events` | 20.454 sec |
| Silver-only rerun | 5.437 sec |
| Gold-only rerun | 7.448 sec |

Silver-only plus Gold-only was about 37% faster than rerunning the full
affected flow.

Gold-only also now maps affected source tables to affected enriched Silver
tables. For example:

```text
network_events -> network_events_enriched
```

That keeps standalone Gold reruns aligned with the incremental pipeline.

## Medium Dataset Baseline Experiment

The project has now started the 12-folder experiment workflow.

Completed experiment folder:

```text
experiments/01_baseline
```

The first medium baseline used:

```text
profile=medium_messy
batch_id=MEDIUM_MESSY_EXP001_BASELINE_20260817_001
raw_size=90 MB
total_csv_lines=559,306
```

Runtime result:

| Stage | Runtime |
| --- | ---: |
| Bronze | 10.011 sec |
| Quality | 17.633 sec |
| SCD2 | 4.566 sec |
| Silver | 9.643 sec |
| Gold | 4.582 sec |
| Total | 46.435 sec |

The main bottlenecks are Quality event validation and Silver event enrichment.
This baseline is the comparison point for the next experiment folders.

## Notebooks

Inspection notebooks have been created so results can be reviewed manually.

Current notebooks:

```text
notebooks/bronze_layer_inspection.ipynb
notebooks/silver_layer_inspection.ipynb
notebooks/quarantine_layer_inspection.ipynb
notebooks/gold_kpi_baseline_analysis.ipynb
```

These notebooks are used to query the Parquet outputs directly and review the
pipeline layer by layer.

## Messy Dataset Results

The `tiny_messy` profile has been generated and processed through Bronze,
Quality, Silver, and Gold.

After cascading validation, quarantine counts were:

| Table | Invalid Rows |
| --- | ---: |
| `regions` | 4 |
| `service_plans` | 4 |
| `towers` | 17 |
| `subscribers` | 1,328 |
| `devices` | 1,469 |
| `network_events` | 19,282 |
| `calls` | 7,670 |
| `data_sessions` | 11,622 |
| `tower_alarms` | 132 |

The hardened Silver run detected:

| Table | Duplicates Removed | Late-Arriving Records |
| --- | ---: | ---: |
| `network_events_enriched` | 1 | 1 |
| `calls_enriched` | 0 | 1 |
| `data_sessions_enriched` | 1 | 1 |
| `tower_alarms_enriched` | 1 | 1 |

## Next Work

The next recommended step is to manually inspect the Gold and Quarantine
notebooks for both `tiny` and `tiny_messy`. After that, the project should move
toward SCD Type 2 dimension history.

## SCD Type 2 Proof Dataset

The `tiny_scd2_demo` profile proves that SCD Type 2 history works.

Two batches were processed:

```text
BATCH_TINY_SCD2_DEMO_20260801_001 -> baseline dimension values
BATCH_TINY_SCD2_DEMO_20260815_002 -> changed dimension values
```

The changed batch updates selected records in:

```text
regions
service_plans
towers
subscribers
devices
```

SCD2 output now contains historical and current versions. Example:

| Dimension | Natural Key | Old Version Ends | Current Version Starts |
| --- | --- | --- | --- |
| `regions` | `REG_001` | `2026-08-14` | `2026-08-15` |
| `service_plans` | `PLAN_PREPAID_BASIC` | `2026-08-14` | `2026-08-15` |
| `towers` | `TOWER_0001` | `2026-08-14` | `2026-08-15` |
| `subscribers` | `SUB_000001` | `2026-08-14` | `2026-08-15` |
| `devices` | `DEV_000001` | `2026-08-14` | `2026-08-15` |

Documentation checkpoint:

```text
After each new project step, decide whether to update documentation before
moving to the next implementation phase.
```

Git checkpoint:

```text
After a few meaningful steps, commit and push changes so the GitHub repository
stays current.
```
