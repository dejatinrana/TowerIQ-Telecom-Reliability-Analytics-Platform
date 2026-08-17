# Spark Performance Experiments

Each experiment should define a telecom problem, hypothesis, workload, baseline,
physical plan evidence, metrics, optimization, rerun, trade-offs, and journal link.

## Current Experiments

| Experiment | Purpose |
| --- | --- |
| `01_baseline` | Measure current full-pipeline runtime before applying new optimizations. |
| `02_partitioning` | Study partition count, partition columns, and output layout. |
| `03_repartition_vs_coalesce` | Compare repartition and coalesce behavior. |
| `04_broadcast_join` | Test broadcast joins for small reference/context DataFrames. |
| `05_data_skew` | Detect and handle uneven key distribution. |
| `06_aqe` | Study Adaptive Query Execution behavior. |
| `07_cache_persist` | Compare cache/persist benefits and costs. |
| `08_small_files` | Measure small-file impact and compaction strategies. |
| `09_partition_pruning` | Verify partition pruning for date/profile filters. |
| `10_predicate_pushdown` | Verify Parquet predicate pushdown behavior. |
| `11_column_pruning` | Reduce unnecessary column reads through selective projection. |
| `12_join_strategies` | Compare join strategies for enrichment and validation workloads. |

## Completed Checkpoint

The first medium-data baseline is recorded in:

```text
experiments/01_baseline/
```

Current baseline:

```text
profile=medium_messy
batch_id=MEDIUM_MESSY_EXP001_BASELINE_20260817_001
total_runtime_seconds=46.435
```
