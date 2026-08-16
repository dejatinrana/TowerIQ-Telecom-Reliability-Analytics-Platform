# EXP-003 Reduce Spark Actions

## Problem

EXP-002 reused one Spark session, but the pipeline still spent time on
redundant actions such as counting data after writes and running separate
counts for related audit metrics.

## Solution

Remove unnecessary read-back counts, calculate related metrics in fewer
aggregations, and reuse controlled Silver DataFrames for enrichment.

## Code Changes

- Bronze no longer reads a table back after writing only to count the same
  rows again.
- SCD2 now calculates total/current row counts before writing instead of
  writing, reading back, and counting twice.
- Silver now combines deduplicated and late-arrival metrics into one
  aggregation and no longer counts enriched output after writing.
- A temporary Silver cache experiment was tested and removed because it made
  the small local workload slower.

## Dataset

```text
profile=tiny_messy_exp003_nocache
batch_id=BATCH_EXP003_NOCACHE_20260814_001
```

## Runtime Comparison

```text
EXP-002 previous best: 53.884 seconds
EXP-003 optimized: 68.487 seconds
improvement: -14.603 seconds (-27.10%)
```

## Interpretation

This experiment did reduce unnecessary action call sites, but it did not reduce
the end-to-end runtime. The pipeline became slower in this run because the
remaining bottleneck is not only action count. Quality validation still performs
many joins and counts, and Spark is still using the default 200 shuffle
partitions, which is too much overhead for this local dataset size.

An earlier EXP-003 run on the reused `tiny_messy` profile took 84.300 seconds,
but that result is not a clean comparison because the profile already contained
multiple appended experiment batches. The clean profile result above is the
better number to use.

## Next Optimization

EXP-004 should tune local shuffle behavior and validation execution:

- Set `spark.sql.shuffle.partitions` to a smaller local value such as 8 or 16.
- Broadcast small dimension/reference tables during quality validation.
- Reduce repeated quality count jobs by calculating validation metrics in a
  more consolidated way.
- Keep audit counts, but make them cheaper.

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 8.181 |
| `quality` | 39.456 |
| `scd2` | 5.215 |
| `silver` | 9.764 |
| `gold` | 5.871 |

## Evidence

Detailed JSON result:

```text
experiments/results/exp_003_reduce_spark_actions_tiny_messy_exp003_nocache_BATCH_EXP003_NOCACHE_20260814_001.json
```
