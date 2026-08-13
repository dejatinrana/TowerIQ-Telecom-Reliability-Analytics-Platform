# Bronze Ingestion for Tiny Dataset

## Problem

Implement the first Bronze ingestion job for the tiny raw telecom dataset.

## Context

The raw dataset was generated as CSV files under `data/raw/tiny/`. The next goal
was to ingest all source tables into Bronze Parquet using explicit schemas and
configuration-driven paths.

## Expected Behavior

The job should read all nine raw CSV files, add Bronze metadata, write Parquet
outputs under `data/bronze/tiny/`, and preserve row counts.

## Actual Behavior

The final job completed successfully. Two local Spark environment issues were
found and fixed before the successful run.

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

The first Spark run failed because local PySpark could not bind its Java gateway
inside the default sandbox. After rerunning with local Spark permissions, Spark
started but failed during `SparkSession` creation.

The root cause was a version mismatch:

```text
Python package: PySpark 3.5.5
JVM Spark runtime from SPARK_HOME: Spark 4.1.1
```

After fixing that, Spark attempted to read local relative paths through HDFS at
`localhost:9000`. This showed that local paths needed to be resolved explicitly
and Spark local mode needed the local filesystem as the default filesystem.

## Root Cause

Two local environment assumptions were unsafe:

- `SPARK_HOME` pointed to a different Spark version than the Python PySpark
  package.
- Relative paths were interpreted through Hadoop's default filesystem instead of
  the local filesystem.

## Options Considered

- Install a matching global Spark version.
- Use the PySpark package runtime for local development.
- Hard-code absolute local paths.
- Add path normalization that also remains compatible with cloud URIs.

## Change Implemented

- Added explicit raw schemas in `src/ingestion/schemas.py`.
- Added config-driven Spark session creation.
- Added local Spark protection through `use_pyspark_package`.
- Added local path normalization while preserving support for URI paths such as
  `s3://...`.
- Added `src/jobs/run_bronze_ingestion.py`.
- Added unit tests for schema registration and important event column types.
- Added a project `.venv` workflow and setup documentation so future local runs
  use the project PySpark dependency instead of global Spark/Python settings.

## Before Metrics

The Bronze job did not complete before environment fixes.

## After Metrics

All nine tables were written to Bronze Parquet with matching row counts.

The Bronze job was also rerun successfully with the project virtual environment:

```text
.venv/bin/python -m src.jobs.run_bronze_ingestion --config configs/local.yaml --profile tiny
```

## Improvement

The ingestion job moved from failing at Spark startup/path resolution to a
successful end-to-end Bronze write.

## Why It Worked

The job now uses the PySpark package runtime consistently for local development
and resolves local paths to absolute filesystem paths. This prevents Spark from
mixing incompatible Spark versions or treating local files as HDFS paths.

The project virtual environment makes this repeatable by installing PySpark,
PyYAML, and pytest locally under `.venv/`.

## Trade-offs

The `use_pyspark_package` setting is designed for local development. In a real
cluster environment, production Spark configuration may intentionally come from
the managed runtime instead.

## What I Learned

Spark jobs depend not only on PySpark code but also on runtime environment:
Python package version, JVM Spark version, Hadoop filesystem configuration, and
local path resolution can all affect whether a job runs.

## Interview Version

While building Bronze ingestion, the first implementation failed because my
Python PySpark package and JVM Spark runtime were different versions. After
diagnosing the mismatch, I made local Spark startup configuration-driven and
ensured local paths resolved to the local filesystem. The final job ingested all
nine raw telecom tables into Bronze Parquet with matching row counts.

## Resume Potential

Not a metric-worthy resume bullet yet, but it is a useful interview story about
debugging local Spark runtime configuration and building a production-portable
Bronze ingestion layer.

## GitHub Checkpoint

Suggested commit message:

```text
Implement Bronze ingestion for tiny telecom dataset
```

Commit status: not committed yet.

Push status: not pushed yet.
