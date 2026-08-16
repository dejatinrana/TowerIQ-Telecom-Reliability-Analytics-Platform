# EXP-004 Shuffle Partition Tuning

## Problem

EXP-003 showed that removing redundant actions was not enough. Quality checks
still spent a lot of time in small shuffle-heavy jobs, and Spark was using the
default 200 shuffle partitions.

## Solution

Set `spark.sql.shuffle.partitions` from configuration for local runs.

## Dataset

```text
profile=tiny_messy_silver_audit_only
batch_id=BATCH_SILVER_AUDIT_ONLY_20260816_001
shuffle_partitions=8
```

## Runtime Comparison

```text
EXP-003 previous runtime: 68.487 seconds
EXP-004 tuned runtime: 29.894 seconds
improvement: 38.593 seconds (56.35%)
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 5.278 |
| `quality` | 11.785 |
| `scd2` | 3.299 |
| `silver` | 5.486 |
| `gold` | 4.046 |

## Evidence

Detailed JSON result:

```text
experiments/results/exp_004_shuffle_partitions_tiny_messy_silver_audit_only_BATCH_SILVER_AUDIT_ONLY_20260816_001.json
```
