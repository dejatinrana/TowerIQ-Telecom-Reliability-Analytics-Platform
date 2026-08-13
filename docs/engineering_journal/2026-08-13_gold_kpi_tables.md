# Gold KPI Tables

## Problem

Create the first business-facing Gold analytics tables for TowerIQ.

## Context

Bronze, data quality, valid Silver, and enriched Silver tables already existed
for the tiny profile. The next step was to aggregate enriched Silver event data
into telecom reliability KPIs.

## Expected Behavior

The Gold layer should build KPI tables that answer operational questions about
tower health, regional reliability, network technology performance, and
subscriber segment experience.

## Actual Behavior

The Gold job completed successfully and created four KPI tables.

## Evidence

Output counts:

| Table | Rows |
| --- | ---: |
| `tower_daily_kpis` | 560 |
| `region_daily_kpis` | 56 |
| `network_type_daily_kpis` | 21 |
| `subscriber_segment_daily_kpis` | 119 |

Unit tests:

```text
8 passed
```

Output location:

```text
data/gold/tiny/
```

## Gold Tables

### `tower_daily_kpis`

Daily tower reliability table.

Example metrics:

- total network events
- failed network events
- network failure rate
- total calls
- dropped calls
- dropped-call rate
- total data sessions
- failed data sessions
- failed-session rate
- critical alarms
- tower health score

### `region_daily_kpis`

Daily region reliability table aggregated from tower-level KPIs.

Example metrics:

- active towers
- total network events
- dropped-call rate
- failed-session rate
- total data usage
- critical alarms
- average tower health score

### `network_type_daily_kpis`

Daily performance table by network technology.

Example dimensions:

- 4G
- 5G
- LTE

Example metrics:

- network failure rate
- average network latency
- average signal strength
- failed-session rate
- total data usage

### `subscriber_segment_daily_kpis`

Daily experience table by customer segment and plan type.

Example metrics:

- total calls
- dropped-call rate
- failed-call rate
- total data sessions
- failed-session rate
- total data usage

## Root Cause

Before this phase, enriched Silver tables were analytics-ready but not yet
aggregated into business KPI tables.

## Change Implemented

- Added Gold KPI transformations in `src/transformations/gold_kpis.py`.
- Added executable job `src/jobs/run_gold_kpis.py`.
- Added unit test coverage for expected enriched Silver inputs.

## Before Metrics

No Gold KPI tables existed.

## After Metrics

Four Gold KPI tables were created from enriched Silver data.

## Improvement

The project now has business-facing analytics outputs that can support tower,
region, network technology, and subscriber segment reliability analysis.

## Why It Worked

The enriched Silver tables already contained event facts plus tower, region,
subscriber, device, and plan context. Gold could aggregate directly from those
tables without repeating low-level enrichment joins.

## Trade-offs

The first Gold KPI formulas are baseline formulas. Metrics such as
`tower_health_score` should be revisited after messy-data scenarios, larger
datasets, and performance experiments are introduced.

## What I Learned

Gold tables should be designed around business questions, not just Spark
functions. The same enriched Silver data can support different analytical views:
tower, region, network type, and subscriber segment.

## Interview Version

I built a Gold analytics layer from enriched Silver telecom event tables. The
Gold layer creates tower daily, region daily, network type daily, and subscriber
segment daily KPI tables with metrics such as failure rates, dropped-call rates,
data usage, critical alarms, and tower health score.

## Resume Potential

This is useful project evidence. Strong resume bullets should wait until the
Gold KPIs are tested with larger datasets and messy-data scenarios so metrics
can be supported by measured evidence.

## GitHub Checkpoint

Suggested commit message:

```text
Implement Gold KPI analytics layer
```

Commit status: not committed yet.

Push status: not pushed yet.
