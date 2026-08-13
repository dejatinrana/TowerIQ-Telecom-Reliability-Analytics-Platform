# All Tables Data Quality And Quarantine

## Problem

Implement table-specific data quality and quarantine checks for all first-version
TowerIQ tables.

## Context

Bronze ingestion created typed Parquet outputs for all raw telecom tables. The
next step was to validate every first-version table before building broader
Silver transformations and Gold analytics.

The quality layer now covers:

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

## Expected Behavior

The job should read Bronze tables, validate key fields and reference
relationships, then split records for each table into:

```text
valid records -> Silver
invalid records -> Quarantine
```

## Actual Behavior

The job completed successfully. Because the tiny baseline dataset is clean, all
records passed validation and `0` records were quarantined for every table.

This created the first Silver output layer: valid records from each Bronze table
were written to Silver. This is not the full Silver transformation phase yet.

## Evidence

Output counts:

| Table | Bronze Rows | Silver Valid Rows | Quarantine Rows |
| --- | ---: | ---: | ---: |
| `regions` | 8 | 8 | 0 |
| `service_plans` | 6 | 6 | 0 |
| `towers` | 80 | 80 | 0 |
| `subscribers` | 5,000 | 5,000 | 0 |
| `devices` | 5,500 | 5,500 | 0 |
| `network_events` | 50,000 | 50,000 | 0 |
| `calls` | 20,000 | 20,000 | 0 |
| `data_sessions` | 30,000 | 30,000 | 0 |
| `tower_alarms` | 800 | 800 | 0 |

Unit tests:

```text
6 passed
```

Output locations:

```text
data/silver/tiny/<table_name>/
data/quarantine/tiny/<table_name>/
```

## Validation Rules

The rule sets are table-specific.

Examples:

- Dimension tables check required IDs, uniqueness, supported status/type values,
  and valid reference relationships.
- `towers` checks region existence, latitude/longitude ranges, capacity score,
  supported networks, and tower status.
- `subscribers` checks home region existence, plan existence, status, activation
  date, segment, and age band.
- `devices` checks subscriber existence, manufacturer/model presence, OS type,
  5G support flag, release year, and device status.
- `network_events` checks required IDs, timestamps, network type, event type,
  status, signal range, latency, timestamp order, and reference-key existence.
- `calls` checks required IDs, timestamps, duration, call type, network type,
  call status, drop reason for non-completed calls, and reference-key existence.
- `data_sessions` checks required IDs, timestamps, duration, network type,
  uploaded/downloaded bytes, latency, session status, failure reason for failed
  sessions, and reference-key existence.
- `tower_alarms` checks alarm ID, tower existence, alarm timestamp, alarm type,
  severity, alarm status, resolved timestamp logic, and description.

## Quarantine Design

Invalid records are preserved rather than silently dropped.

Quarantined records include:

```text
_rejection_reasons
_quarantined_at
_source_table
```

This allows later investigation of why a record failed validation.

## Root Cause

No invalid records were found because the first tiny dataset is intentionally a
clean baseline. This is expected at this stage.

## Change Implemented

- Added reusable rule helpers in `src/quality/rules.py`.
- Added table-specific validators in `src/quality/table_validators.py`.
- Added executable all-table quality job `src/jobs/run_quality_checks.py`.
- Added unit tests in `tests/unit/test_network_event_rules.py`.

## Before Metrics

Before this change, Bronze tables existed but not every table had a data quality
split into Silver and Quarantine.

## After Metrics

```text
111,394 valid records across all tables
0 quarantined records
```

## Improvement

The project now has a working path from Bronze to Silver and Quarantine for all
nine first-version tables.

The current Silver output means "valid Bronze records." The next Silver phase
will create cleaned and enriched analytical tables by joining events with
reference context such as towers, regions, subscribers, devices, and service
plans.

## Why It Worked

The validation logic separates table-specific correctness rules from the job
runner. The job can read Bronze data, apply reusable checks, and write
valid/invalid outputs to separate data lake zones.

## Trade-offs

The current tiny dataset is clean, so quarantine behavior is structurally
verified but not yet tested with intentionally bad data.

The quality checks are intentionally basic first-version rules. They will become
more mature when chaos injection, duplicate handling, and schema evolution are
added.

## What I Learned

Data quality should not simply filter bad data away. A production-style pipeline
should preserve invalid records with rejection reasons so problems can be
investigated.

## Interview Version

I implemented table-specific data quality checks for all nine first-version
TowerIQ tables. The job validates required fields, uniqueness, supported values,
numeric ranges, timestamp logic, and reference-key relationships. Valid records
go to Silver, while invalid records are preserved in Quarantine with rejection
reasons.

## Resume Potential

Not a final resume metric yet because the baseline dataset has no invalid
records. It becomes stronger after we add bad-record injection and produce real
quarantine counts.

## GitHub Checkpoint

Suggested commit message:

```text
Implement all-table data quality and quarantine
```

Commit status: not committed yet.

Push status: not pushed yet.
