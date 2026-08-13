# ADR-001: Local-First, Production-Portable Design

## Status

Accepted

## Context

TowerIQ is being developed on a local system. Source files, generated datasets,
and pipeline outputs will initially live in local folders. However, the project
should still resemble a production data platform and should be easy to move later
to cloud object storage and a Spark cluster.

## Options

- Hard-code local paths directly in scripts.
- Use local folders now, but keep paths and runtime settings configurable.
- Start immediately with cloud storage and managed Spark.

## Decision

Use local folders now, but write the project as local-first and
production-portable.

## Reason

Local development keeps iteration fast and inexpensive. Configuration-driven
paths keep the project close to production design because local paths can later
be replaced with locations such as S3, ADLS, or GCS.

## Trade-offs

This does not fully test production concerns such as cloud IAM, distributed
cluster sizing, object storage consistency, or managed catalogs yet.

## Consequences

Pipeline code should avoid hard-coded local paths. Raw, Bronze, Silver, Gold,
and Quarantine locations should come from configuration.

