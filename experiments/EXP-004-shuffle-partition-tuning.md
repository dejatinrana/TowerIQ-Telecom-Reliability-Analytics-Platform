# EXP-004 Shuffle Partition Tuning

## Problem

EXP-003 showed that removing redundant actions was not enough. Quality checks
still spent a lot of time in small shuffle-heavy jobs, and Spark was using the
default 200 shuffle partitions.

## Solution

Set `spark.sql.shuffle.partitions` from configuration for local runs.

## Dataset

```text
profile=medium_messy
batch_id=MEDIUM_MESSY_FULL_20260817_001
shuffle_partitions=8
```

## Runtime Comparison

```text
EXP-003 previous runtime: 68.487 seconds
EXP-004 tuned runtime: 53.061 seconds
improvement: 15.426 seconds (22.52%)
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 8.347 |
| `quality` | 21.511 |
| `scd2` | 4.796 |
| `silver` | 13.566 |
| `gold` | 4.841 |

## Evidence

Detailed JSON result:

```text
experiments/results/exp_004_shuffle_partitions_medium_messy_MEDIUM_MESSY_FULL_20260817_001.json
```
