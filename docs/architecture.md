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

In production, these paths could become cloud data lake locations:

```text
s3://toweriq-data-lake/raw/
s3://toweriq-data-lake/bronze/
s3://toweriq-data-lake/silver/
s3://toweriq-data-lake/gold/
s3://toweriq-data-lake/quarantine/
```

The transformation logic should not depend on local-only paths. Storage
locations, dataset profiles, and Spark settings should come from configuration.

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
