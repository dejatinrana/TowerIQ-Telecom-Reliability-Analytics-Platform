# Enriched Silver Transformations

## Problem

Create enriched Silver event tables that are easier to use for telecom
analytics.

## Context

The first Silver layer contained valid records after data quality checks. The
next step was to enrich event tables with dimension context so later Gold KPIs
can be built without repeatedly joining raw IDs.

## Expected Behavior

The job should read valid Silver tables, join event records with tower, region,
subscriber, device, and service plan context, then write enriched Silver Parquet
outputs.

## Actual Behavior

The job completed successfully and created four enriched event tables.

## Evidence

Output counts:

| Table | Input Rows | Output Rows |
| --- | ---: | ---: |
| `network_events_enriched` | 50,000 | 50,000 |
| `calls_enriched` | 20,000 | 20,000 |
| `data_sessions_enriched` | 30,000 | 30,000 |
| `tower_alarms_enriched` | 800 | 800 |

Unit tests:

```text
7 passed
```

Output location:

```text
data/silver/tiny/enriched/
```

## Enrichment Design

`network_events`, `calls`, and `data_sessions` are enriched with:

- tower context
- region context
- subscriber context
- device context
- service plan context

`tower_alarms` are enriched with:

- tower context
- region context

## Derived Columns

Common date columns:

```text
event_date
event_hour
event_day_of_week
```

Event-specific columns:

```text
is_dropped_call
is_failed_call
total_bytes
total_mb
is_failed_session
is_critical_alarm
```

## Root Cause

Before this phase, Silver contained valid records but still required many joins
to answer business questions. Event records had IDs, but not enough business
context for convenient analytics.

## Change Implemented

- Added enrichment functions in `src/transformations/silver_events.py`.
- Added executable job `src/jobs/run_silver_transformations.py`.
- Added unit test coverage for enriched table source mapping.

## Before Metrics

Valid Silver event tables existed, but no enriched event tables existed.

## After Metrics

Four enriched Silver event tables were created with preserved row counts.

## Improvement

Gold KPI development can now use enriched event tables instead of repeatedly
joining low-level event records to reference tables.

## Why It Worked

The transformation separates reusable context builders from event-specific
enrichment functions. Tower/region context and subscriber/device/plan context
can be reused across multiple event tables.

## Trade-offs

Enriched Silver tables duplicate some dimension attributes. This improves
analytics ergonomics but means dimension changes must be handled carefully in
future incremental and SCD Type 2 phases.

The current Silver implementation is a clean baseline. Future messy-data
scenarios such as duplicate events, duplicate batches, late-arriving data,
schema evolution, and SCD Type 2 will require additional Silver logic.

## What I Learned

Silver is not only about filtering valid rows. Mature Silver tables often add
consistent business context so downstream analytics become simpler and less
error-prone.

Silver should also evolve with the failure scenarios being tested. It is better
to add deduplication, late-data handling, and SCD Type 2 logic when those
scenarios exist and can be measured.

## Interview Version

After validating Bronze records, I built enriched Silver event tables by joining
network events, calls, sessions, and alarms with tower, region, subscriber,
device, and plan context. This created analytics-ready Silver tables while
preserving source row counts.

## Resume Potential

This is useful project evidence, but final resume metrics should wait until Gold
KPIs and larger-scale experiments produce measurable outcomes.

## GitHub Checkpoint

Suggested commit message:

```text
Implement enriched Silver transformations
```

Commit status: not committed yet.

Push status: not pushed yet.
