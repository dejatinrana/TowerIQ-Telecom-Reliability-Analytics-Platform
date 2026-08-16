# EXP-001 Baseline Runtime Findings

## Problem

The first baseline experiment completed successfully, but the runtime looked
high for a small dataset.

## Experiment

Experiment:

```text
EXP-001 Baseline Pipeline Runtime
```

Dataset profile:

```text
tiny_messy
```

Batch:

```text
BATCH_EXP001_TINY_MESSY_20260814_001
```

## Result

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 10.029 |
| `quality` | 26.632 |
| `scd2` | 4.065 |
| `silver` | 10.585 |
| `gold` | 7.093 |

Total runtime:

```text
58.404 seconds
```

## Observation

The Quality stage was the slowest stage.

This is expected because Quality performs many validation checks and several
foreign-key joins:

```text
towers -> regions
subscribers -> regions and service_plans
devices -> subscribers
network_events/calls/data_sessions -> towers, subscribers, devices
tower_alarms -> towers
```

## Why Small Data Can Still Be Slow

Spark is configured with:

```text
local[*]
```

This allows Spark to use all local cores. However, small Spark jobs can still
feel slow because fixed overhead is large compared with the amount of data.

Likely causes:

- Separate Spark sessions are created for Bronze, Quality, SCD2, Silver, and
  Gold.
- Multiple `.count()` actions trigger repeated Spark jobs.
- Quality validation performs repeated joins and duplicate checks.
- Local Parquet folders contain multiple appended experiment batches.
- Spark startup, scheduling, file listing, and shuffle overhead are visible on
  small data.

## Engineering Decision

Do not scale immediately just because the pipeline works.

First, run a small-data optimization experiment so the project has a cleaner
baseline before moving to larger data.

## Next Experiment

Suggested next experiment:

```text
EXP-002 Runtime Overhead Reduction
```

Optimization candidates:

- Reuse one Spark session across the full pipeline.
- Reduce repeated `.count()` actions.
- Cache or broadcast small dimension tables during Quality.
- Tune `spark.sql.shuffle.partitions` for local execution.
- Measure file counts and partition counts.
- Compare the optimized runtime against the `58.404` second baseline.

## Interview Version

The first performance baseline showed that the Quality stage was the bottleneck
even on small data. I investigated why and identified Spark session startup,
repeated actions, validation joins, and local Parquet overhead as likely causes.
Before scaling, I planned an optimization experiment to reduce runtime overhead
and create a cleaner baseline.
