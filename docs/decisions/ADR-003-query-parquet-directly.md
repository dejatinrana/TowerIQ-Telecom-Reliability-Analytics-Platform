# ADR-003: Query Parquet Directly

## Status

Accepted

## Context

Parquet files are binary columnar files, so it may seem like they need to be
converted to CSV or loaded into a database before querying.

## Options

- Convert Parquet to CSV before querying.
- Load all Parquet data into PostgreSQL.
- Query Parquet directly through analytical engines.

## Decision

Query Parquet directly using Spark SQL locally, and later using production query
engines such as Databricks SQL, Trino, Athena, BigQuery external tables,
Snowflake external tables, or Redshift Spectrum.

## Reason

Parquet is designed for analytical query engines. Converting it to CSV would lose
the benefits of columnar storage, compression, schema information, column
pruning, and predicate pushdown.

## Trade-offs

Direct Parquet querying requires a compatible query engine. A traditional
database may still be useful later for selected Gold summaries or metadata, but
not as the main storage layer for all event-level data.

## Consequences

Bronze, Silver, and Gold datasets should remain queryable in the data lake.
Selected small Gold KPI tables may later be exported to PostgreSQL only if a
serving/dashboard requirement needs it.

