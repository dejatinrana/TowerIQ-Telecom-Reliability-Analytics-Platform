# Messy Data And Silver Hardening

## Problem

The clean tiny dataset proved the happy path, but real telecom pipelines must
handle invalid records, duplicate delivery, late-arriving events, and source
schema changes.

## Context

TowerIQ already had Raw, Bronze, Quality, Silver, and Gold layers. The next step
was to create a messy data profile and prove that bad data does not silently
reach analytics.

## Change Implemented

- Added a `tiny_messy` raw dataset generator.
- Added deterministic messy-data injection in `data_generator/scenarios/chaos.py`.
- Updated quality validation so child tables check foreign keys against valid
  parent dimensions.
- Added reusable Silver controls in `src/transformations/silver_controls.py`.
- Applied Silver controls before enriched event-table joins.
- Added unit tests for messy-data generation, validator flow, and Silver control
  metadata.

## Messy Data Injected

The messy profile includes:

- Duplicate dimension keys.
- Unknown foreign keys.
- Unsupported region, plan, network, call, session, and alarm values.
- Invalid timestamps.
- Invalid latitude and longitude.
- Negative numeric measures.
- Missing business values.
- Valid duplicate event records.
- Valid late-arriving event records.

## Cascading Validation

Validation now follows dependency order:

```text
regions and service_plans
  -> towers and subscribers
  -> devices
  -> network_events, calls, data_sessions, tower_alarms
```

This matters because child records should only trust parent records that already
passed validation. A tower should not pass just because its `region_id` exists in
Bronze. It should pass only when the referenced region is valid.

## Quarantine Results

After running `tiny_messy` through quality checks:

| Table | Bronze Rows | Valid Rows | Invalid Rows |
| --- | ---: | ---: | ---: |
| `regions` | 11 | 7 | 4 |
| `service_plans` | 9 | 5 | 4 |
| `towers` | 84 | 67 | 17 |
| `subscribers` | 5,004 | 3,676 | 1,328 |
| `devices` | 5,504 | 4,035 | 1,469 |
| `network_events` | 50,007 | 30,725 | 19,282 |
| `calls` | 20,006 | 12,336 | 7,670 |
| `data_sessions` | 30,006 | 18,384 | 11,622 |
| `tower_alarms` | 806 | 674 | 132 |

The large child-table quarantine counts show the downstream impact of invalid
parent dimensions.

## Silver Hardening

Silver now handles:

- Deduplication by event business key.
- Idempotent reruns through deterministic overwrite outputs.
- Late-arriving record tracking.
- Optional schema-evolution columns.

Added Silver metadata:

```text
_silver_was_deduplicated
_arrival_delay_hours
_late_arrival_threshold_hours
_is_late_arriving
source_system
producer_schema_version
```

## Silver Results

The `tiny_messy` Silver run produced:

| Table | Input Rows | Deduplicated Input | Duplicates Removed | Late-Arriving Records |
| --- | ---: | ---: | ---: | ---: |
| `network_events_enriched` | 30,725 | 30,724 | 1 | 1 |
| `calls_enriched` | 12,336 | 12,336 | 0 | 1 |
| `data_sessions_enriched` | 18,384 | 18,383 | 1 | 1 |
| `tower_alarms_enriched` | 674 | 673 | 1 | 1 |

## Gold Verification

The pipeline still reached Gold after messy-data handling:

| Gold Table | Rows |
| --- | ---: |
| `tower_daily_kpis` | 469 |
| `region_daily_kpis` | 49 |
| `network_type_daily_kpis` | 21 |
| `subscriber_segment_daily_kpis` | 119 |

## Design Decision

Dimension duplicates are quarantined because reference tables need one trusted
version. Fact/event duplicates are handled in Silver because telecom event
systems can redeliver the same event, and Silver can apply a latest-record rule.

## Unit Tests

Latest unit test result:

```text
14 passed
```

## Interview Version

I added a messy-data profile to test realistic pipeline failures, then hardened
the Silver layer with deduplication, idempotent reruns, late-arrival tracking,
and schema-evolution support. I also changed validation to cascade through valid
parent dimensions so invalid master data cannot silently contaminate downstream
facts and KPIs.

## Next Work

The next engineering step is incremental processing and SCD Type 2 dimension
history.

## GitHub Checkpoint

Suggested commit message:

```text
Add messy data scenarios and harden Silver transformations
```

Commit status: not committed yet.

Push status: not pushed yet.
