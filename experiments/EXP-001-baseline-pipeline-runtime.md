# EXP-001 Baseline Pipeline Runtime

## Purpose

Measure the current local TowerIQ pipeline runtime before scaling the dataset.

## Dataset

```text
profile=tiny_messy
batch_id=BATCH_EXP001_TINY_MESSY_20260814_001
```

## Runtime

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

## Evidence

Detailed JSON result:

```text
experiments/results/exp_001_baseline_tiny_messy_BATCH_EXP001_TINY_MESSY_20260814_001.json
```

## Notes

This is the small-data baseline. Future experiments should compare larger
profiles and Spark tuning changes against this result.

## First Findings

The total runtime was high for a small local dataset:

```text
58.404 seconds
```

The slowest stage was:

```text
quality = 26.632 seconds
```

This does not mean the business logic is wrong. It means the current local Spark
implementation has overhead that becomes visible on small data.

Likely causes:

- Each stage starts a separate Spark session.
- Multiple `.count()` calls trigger repeated Spark jobs.
- Quality validation performs many foreign-key joins.
- Append-based local Parquet outputs now contain multiple experiment batches.
- Spark has startup and scheduling overhead even when the data is small.

Current Spark master:

```text
local[*]
```

This allows Spark to use all available local cores, but small local jobs may
still not fully utilize every core because startup, scheduling, file listing,
and shuffle overhead can dominate the actual work.

## Optimization Candidates

The next experiment should test runtime improvements before scaling data:

- Reuse one Spark session across the full pipeline.
- Reduce repeated `.count()` actions.
- Cache or broadcast small dimension/reference tables during quality checks.
- Tune `spark.sql.shuffle.partitions` for local execution.
- Measure file counts and partition counts before and after each stage.
- Compare full pipeline runtime before and after each optimization.
