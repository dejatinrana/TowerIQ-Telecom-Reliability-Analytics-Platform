# 2026-08-14 EXP-004 Shuffle Partition Tuning

## Problem

The Quality stage was taking around 40 seconds on the local messy dataset.
EXP-003 showed that removing unnecessary actions was not enough because Spark
was still creating too many tiny shuffle tasks.

Spark's default `spark.sql.shuffle.partitions` is `200`. That default is often
reasonable for larger cluster jobs, but it is too high for a local dataset of
this size.

## Change

Set local shuffle partitions through configuration:

```text
spark.shuffle_partitions: 8
```

The setting is now passed through the shared Spark session helper and every
job/experiment runner that creates Spark.

## Result

The clean EXP-004 run used:

```text
profile=tiny_messy_exp004_shuffle8
batch_id=BATCH_EXP004_SHUFFLE8_20260814_001
shuffle_partitions=8
```

Runtime comparison:

```text
EXP-003 previous runtime: 68.487 seconds
EXP-004 tuned runtime: 43.552 seconds
improvement: 24.935 seconds faster
```

Quality stage comparison:

```text
EXP-003 quality: 39.456 seconds
EXP-004 quality: 23.485 seconds
improvement: 15.971 seconds faster
```

## Stage Timings

| Stage | Runtime Seconds |
| --- | ---: |
| Bronze | 5.912 |
| Quality | 23.485 |
| SCD2 | 3.738 |
| Silver | 5.987 |
| Gold | 4.430 |

## Learning

For local Spark development, too many shuffle partitions can be expensive even
when the dataset is not large. The pipeline was spending time scheduling many
small tasks instead of doing useful work.

For production, this value should not be hardcoded to `8`. It should stay
config-driven and be tuned based on cluster size, data volume, and observed
shuffle behavior.

## Remaining Quality Optimizations

- Broadcast small reference tables during validation joins.
- Reduce repeated Quality count actions while preserving audit metrics.
- Avoid recomputing valid parent DataFrames during cascading validation.
- Consider parallelizing independent child-table validation after parent
  dimensions are validated.
- Add rejection-reason metrics to identify which rules create the most
  quarantine volume.
