# Incremental Partitioned Pipeline

## Problem

The first incremental runner could skip already completed batches, but the
pipeline still wrote most outputs as full overwrites. That protected reruns, but
it was not close enough to how production data lakes usually process new
batches.

## Change Implemented

- Added `_pipeline_batch_id` metadata to Bronze records.
- Changed Bronze writes from full overwrite to append by `_pipeline_batch_id`.
- Changed Quality valid and Quarantine writes to append by `_pipeline_batch_id`.
- Changed enriched Silver writes to append by `event_date` and
  `_pipeline_batch_id`.
- Changed Gold writes to dynamically overwrite affected `event_date`
  partitions.
- Passed `batch_id` through the incremental runner into Bronze, Quality, Silver,
  and Gold jobs.

## Current Write Strategy

| Layer | Strategy |
| --- | --- |
| Bronze | Append batch partition. |
| Quality valid Silver | Append batch partition. |
| Quarantine | Append batch partition. |
| Enriched Silver | Append event-date and batch partitions. |
| Gold | Replace affected event-date partitions. |

## Why This Matters

Production pipelines should avoid rebuilding unrelated data when a new batch
arrives. If a batch only affects a few dates, the pipeline should write those
batch/date partitions and recompute only the related Gold KPI dates.

## Verification

Unit tests:

```text
18 passed
```

Incremental run:

```text
profile=tiny_messy
batch_id=BATCH_TINY_MESSY_20260801_002
status=completed
```

Second run with the same batch:

```text
status=skipped
```

Observed local partitions:

```text
data/bronze/tiny_messy/network_events/_pipeline_batch_id=BATCH_TINY_MESSY_20260801_002
data/silver/tiny_messy/enriched/network_events_enriched/event_date=2026-08-01/_pipeline_batch_id=BATCH_TINY_MESSY_20260801_002
data/gold/tiny_messy/tower_daily_kpis/event_date=2026-08-01
```

## Interview Version

I upgraded the pipeline from full-output overwrite behavior to batch/date
partitioned incremental writes. Bronze, Silver, and Quarantine append new batch
partitions, while Gold recomputes only affected event dates. A local batch
registry prevents accidental duplicate processing.

## Next Work

The next phase is SCD Type 2 dimension history so events can join to the correct
historical version of towers, subscribers, service plans, and other dimensions.

## GitHub Checkpoint

Suggested commit message:

```text
Add incremental batch registry and partitioned pipeline writes
```

Commit status: not committed yet.

Push status: not pushed yet.
