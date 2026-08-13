# ADR-001: Local-First, Production-Portable Design

## Status

Accepted

## Context

TowerIQ is being developed on a local system. Source files, generated datasets,
and pipeline outputs will initially live in local folders. However, the project
should still resemble a production data platform and should be easy to move later
to cloud object storage and a Spark cluster.

## Options

- Build only for local folders.
- Use local folders now while keeping the same Raw/Bronze/Silver/Gold zone
  structure that a production data lake would use.
- Start immediately with cloud storage and managed Spark.

## Decision

Use local folders now, but write the project as local-first and
production-portable.

## Reason

Local development keeps iteration fast and inexpensive. Keeping clear data lake
zones makes the design easier to move later to storage such as S3, ADLS, or GCS.

## Trade-offs

This does not fully test production concerns such as cloud IAM, distributed
cluster sizing, object storage consistency, or managed catalogs yet.

## Consequences

The project should consistently preserve the Raw, Bronze, Silver, Gold, and
Quarantine zone structure.
