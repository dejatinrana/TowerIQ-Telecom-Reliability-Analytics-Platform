# EXP-004 Shuffle Partition Tuning

## Problem

EXP-003 showed that removing redundant actions was not enough. Quality checks
still spent a lot of time in small shuffle-heavy jobs, and Spark was using the
default 200 shuffle partitions.

## Solution

Set `spark.sql.shuffle.partitions` from configuration for local runs.

## Dataset

```text
profile=tiny_messy_bronze_fast_audit
batch_id=BATCH_BRONZE_FAST_AUDIT_20260816_001
shuffle_partitions=8
```

## Runtime Comparison

```text
EXP-003 previous runtime: 68.487 seconds
EXP-004 tuned runtime: 28.489 seconds
improvement: 39.998 seconds (58.40%)
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 4.808 |
| `quality` | 12.244 |
| `scd2` | 3.195 |
| `silver` | 4.985 |
| `gold` | 3.257 |

## Evidence

Detailed JSON result:

```text
experiments/results/exp_004_shuffle_partitions_tiny_messy_bronze_fast_audit_BATCH_BRONZE_FAST_AUDIT_20260816_001.json
```
