# 2026-08-16 Silver And Gold Affected-Table / Affected-Date Processing

## Problem

Quality could already validate only selected affected tables, but Silver and
Gold still behaved like full-batch stages.

That meant an incremental batch with only one changed event table could still
trigger:

```text
all Silver enriched event outputs
all Gold KPI outputs
```

This is wasteful in production-style incremental processing.

## Design

The pipeline now carries affected-table context forward:

```text
affected raw/quality tables
-> affected Silver enriched tables
-> affected Gold KPI tables
-> affected Gold event_date partitions
```

## Silver Affected-Table Logic

Silver now maps changed valid Silver source tables to enriched outputs:

| Changed Source Table | Silver Output |
| --- | --- |
| `network_events` | `network_events_enriched` |
| `calls` | `calls_enriched` |
| `data_sessions` | `data_sessions_enriched` |
| `tower_alarms` | `tower_alarms_enriched` |

If a dimension table changes, Silver treats all enriched event tables as
affected because dimension context can change enrichment results:

```text
regions
towers
service_plans
subscribers
devices
```

Dimension change behavior:

```text
dimension changed -> rebuild all enriched event table shapes
```

## Gold Affected-Table Logic

Gold now maps changed enriched Silver tables to only the KPI tables that depend
on them.

| Changed Enriched Table | Affected Gold Tables |
| --- | --- |
| `network_events_enriched` | `tower_daily_kpis`, `region_daily_kpis`, `network_type_daily_kpis` |
| `calls_enriched` | `tower_daily_kpis`, `region_daily_kpis`, `subscriber_segment_daily_kpis` |
| `data_sessions_enriched` | `tower_daily_kpis`, `region_daily_kpis`, `network_type_daily_kpis`, `subscriber_segment_daily_kpis` |
| `tower_alarms_enriched` | `tower_daily_kpis`, `region_daily_kpis` |

Gold also collects affected `event_date` values from the changed enriched
tables for the current batch and filters KPI recomputation to those dates.

This means Gold is now:

```text
affected-table aware
affected-date aware
```

## Code Changes

Main files:

```text
src/jobs/run_silver_transformations.py
src/transformations/silver_events.py
src/jobs/run_gold_kpis.py
src/transformations/gold_kpis.py
src/jobs/run_incremental_pipeline.py
tests/unit/test_silver_transformations.py
tests/unit/test_gold_kpis.py
tests/unit/test_incremental_pipeline.py
```

New CLI options:

```bash
python -m src.jobs.run_silver_transformations \
  --source-tables network_events,calls
```

```bash
python -m src.jobs.run_gold_kpis \
  --enriched-tables network_events_enriched,calls_enriched
```

```bash
python -m src.jobs.run_incremental_pipeline \
  --affected-quality-tables network_events
```

## Verification

Full pipeline benchmark after the affected-table changes:

```text
profile=tiny_messy_silver_gold_aware
batch_id=BATCH_SILVER_GOLD_AWARE_FULL_20260816_001
total runtime=29.652 sec
```

Affected-table incremental run:

```text
profile=tiny_messy_silver_gold_aware
batch_id=BATCH_SILVER_GOLD_AWARE_NETWORK_ONLY_20260816_001
affected table=network_events
wall clock runtime=20.67 sec
```

Outputs from the affected-table run:

| Layer | Tables Written |
| --- | --- |
| Quality | `network_events` |
| Silver | `network_events_enriched` |
| Gold | `tower_daily_kpis`, `region_daily_kpis`, `network_type_daily_kpis` |

This proves that the downstream pipeline no longer blindly writes every Silver
and Gold output when only one event table is affected.

## Important Limitation

This implementation is affected-table and affected-date aware for the current
batch flow. It is not yet a full historical dimension backfill engine.

For example, if a dimension changes, the pipeline knows that enrichment can be
affected broadly. But a deeper production implementation would also identify
exact historical records or dates affected by that dimension change and
recompute only those partitions.

Future improvement:

```text
dimension change -> identify impacted subscribers/towers -> identify impacted dates -> recompute only those partitions
```

## Interview Explanation

```text
I made the incremental pipeline dependency-aware after Quality. If only
network_events changes, Silver now writes only network_events_enriched, and Gold
recomputes only KPI tables that depend on network events. Gold also filters to
the event_date values affected by the current batch. This avoids unnecessary
Silver and Gold work and moves the project closer to production-style
incremental processing.
```
