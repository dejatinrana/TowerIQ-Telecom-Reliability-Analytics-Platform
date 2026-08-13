# ADR-002: Raw CSV and Processed Parquet

## Status

Accepted

## Context

TowerIQ needs to simulate raw telecom source extracts and also produce efficient
analytical datasets for Spark processing.

## Options

- Store every layer as CSV.
- Store every layer as Parquet.
- Use CSV for raw source data and Parquet for processed layers.

## Decision

Use CSV for raw generated source data and Parquet for Bronze, Silver, Gold, and
Quarantine outputs.

## Reason

CSV is simple and useful for simulating raw files from upstream systems. Parquet
is a better format for processed analytical layers because it is columnar,
compressed, schema-aware, and efficient for Spark reads and writes.

## Trade-offs

CSV requires explicit schema handling and can contain parsing issues. Parquet is
not human-readable as plain text, so it should be inspected through Spark, DuckDB,
or another query engine.

## Consequences

The raw generator writes CSV files. Pipeline jobs should write processed outputs
as Parquet.

