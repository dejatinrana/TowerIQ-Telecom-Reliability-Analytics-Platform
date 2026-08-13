# Bronze Ingestion for Tiny Dataset

## Problem

Implement the first Bronze ingestion job for the tiny raw telecom dataset.

## Context

The raw dataset was generated as CSV files under `data/raw/tiny/`. The next goal
was to ingest all source tables into Bronze Parquet using explicit schemas.

## Expected Behavior

The job should read all nine raw CSV files, add Bronze metadata, write Parquet
outputs under `data/bronze/tiny/`, and preserve row counts.

## Actual Behavior

The Bronze ingestion job completed successfully.

## Evidence

Successful Bronze row counts:

| Table | Raw Rows | Bronze Rows | Status |
| --- | ---: | ---: | --- |
| `regions` | 8 | 8 | OK |
| `towers` | 80 | 80 | OK |
| `service_plans` | 6 | 6 | OK |
| `subscribers` | 5,000 | 5,000 | OK |
| `devices` | 5,500 | 5,500 | OK |
| `network_events` | 50,000 | 50,000 | OK |
| `calls` | 20,000 | 20,000 | OK |
| `data_sessions` | 30,000 | 30,000 | OK |
| `tower_alarms` | 800 | 800 | OK |

Unit tests:

```text
3 passed
```

## Investigation

The verification focused on row-count preservation from Raw to Bronze for all
nine source tables.

## Root Cause

No data loss or row-count mismatch was found during Bronze ingestion.

## Options Considered

- Infer schemas from CSV files.
- Define explicit schemas for each raw source table.
- Preserve source columns exactly and add Bronze metadata fields.

## Change Implemented

- Added explicit raw schemas in `src/ingestion/schemas.py`.
- Added `src/jobs/run_bronze_ingestion.py`.
- Added unit tests for schema registration and important event column types.
- Added Bronze metadata fields:
  - `_bronze_loaded_at`
  - `_bronze_table`
  - `_dataset_profile`
  - `_source_format`

## Before Metrics

Raw CSV row counts were available for all nine tables.

## After Metrics

All nine tables were written to Bronze Parquet with matching row counts.

## Improvement

The project now has a repeatable Bronze ingestion step that converts raw telecom
CSV files into typed Bronze Parquet datasets.

## Why It Worked

Explicit schemas make the raw-to-Bronze conversion predictable. Row-count checks
prove that ingestion preserved the expected number of records for each table.

## Trade-offs

Bronze does not clean or reject bad records yet. It creates a typed, traceable
copy of raw data. Data quality and quarantine logic will be handled in a later
phase.

## What I Learned

Bronze should stay close to the raw source while adding enough technical
metadata to make later pipeline stages traceable.

## Interview Version

I implemented a Bronze ingestion layer that reads nine raw telecom CSV source
tables with explicit schemas and writes typed Parquet outputs. I verified the
ingestion by comparing Raw and Bronze row counts for every table.

## Resume Potential

Not a metric-worthy resume bullet yet, but it is useful project evidence for
building a structured Bronze ingestion layer with explicit schemas and row-count
validation.

## GitHub Checkpoint

Suggested commit message:

```text
Implement Bronze ingestion for tiny telecom dataset
```

Commit status: not committed yet.

Push status: not pushed yet.
