# ADR-004: Dataset Scale Strategy

## Status

Accepted

## Context

TowerIQ needs both fast local development and meaningful Spark performance
experiments. Starting with large data immediately would slow debugging and make
basic correctness issues harder to diagnose.

## Options

- Start directly with large datasets.
- Use only a tiny dataset.
- Define multiple dataset profiles and scale gradually.

## Decision

Use multiple dataset profiles and start with the tiny profile.

## Reason

Tiny data is best for proving schema design, relationships, pipeline correctness,
and data quality logic. Larger profiles should be introduced only after the
pipeline is logically correct.

## Trade-offs

Tiny data will not expose all Spark performance problems. Data skew, shuffle
cost, small-file behavior, and partitioning issues may require medium or large
profiles later.

## Consequences

Dataset profiles should be stored separately:

```text
data/raw/tiny/
data/raw/development/
data/raw/medium/
data/raw/large/
```

The project rule is:

```text
Correctness first, scale second, optimization third.
```

