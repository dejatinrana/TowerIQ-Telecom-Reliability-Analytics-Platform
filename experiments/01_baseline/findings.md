# Baseline Findings

## Completed Run

```text
experiment=01_baseline
profile=medium_messy
batch_id=MEDIUM_MESSY_EXP001_BASELINE_20260817_001
config=configs/local_fast.yaml
total_runtime_seconds=46.435
```

## Stage Results

| Rank | Stage | Runtime | Share Of Total |
| ---: | --- | ---: | ---: |
| 1 | Quality | 17.633 sec | 37.96% |
| 2 | Bronze | 10.011 sec | 21.56% |
| 3 | Silver | 9.643 sec | 20.77% |
| 4 | SCD2 | 4.566 sec | 9.83% |
| 5 | Gold | 4.582 sec | 9.87% |

## Task-Level Bottlenecks

| Rank | Task | Runtime |
| ---: | --- | ---: |
| 1 | `quality.network_events` | 3.936 sec |
| 2 | `silver.network_events_enriched` | 3.660 sec |
| 3 | `quality.data_sessions` | 3.487 sec |
| 4 | `quality.calls` | 2.326 sec |
| 5 | `bronze.regions` | 2.287 sec |

## What This Tells Us

The pipeline is no longer dominated only by Spark startup overhead. On the
medium dataset, the event-heavy Quality and Silver tasks are now visible.

Quality is slowest because it validates large event tables against trusted
parent tables:

```text
network_events -> towers, subscribers, devices
calls -> towers, subscribers, devices
data_sessions -> towers, subscribers, devices
```

Silver is second because it enriches large event tables with tower, region,
subscriber, device, and plan context.

## Next Experiment Direction

Use this baseline as the comparison point for the next experiment folder.

The next useful experiment is likely one of:

```text
02_partitioning
04_broadcast_join
11_column_pruning
12_join_strategies
```

Because the current bottlenecks are mostly event validation and enrichment.

## Interview Explanation

```text
I started the medium-data optimization phase with a baseline experiment instead
of changing code immediately. The medium_messy profile has about 559k CSV rows
and the baseline full pipeline completed in 46.435 seconds. The slowest layer
was Quality at 17.633 seconds, followed by Silver at 9.643 seconds. That told me
the next optimizations should target event-table validation and enrichment joins
instead of guessing blindly.
```
