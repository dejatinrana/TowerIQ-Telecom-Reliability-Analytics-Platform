# EXP-001 Baseline Pipeline Runtime

## Purpose

Measure the current local TowerIQ pipeline runtime before scaling the dataset.

## Dataset

```text
profile=medium_messy
batch_id=MEDIUM_MESSY_EXP001_BASELINE_20260817_001
```

## Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 10.011 |
| `quality` | 17.633 |
| `scd2` | 4.566 |
| `silver` | 9.643 |
| `gold` | 4.582 |

Total runtime:

```text
46.435 seconds
```

## Evidence

Detailed JSON result:

```text
experiments/results/exp_001_baseline_medium_messy_MEDIUM_MESSY_EXP001_BASELINE_20260817_001.json
```

## Notes

This is the small-data baseline. Future experiments should compare larger
profiles and Spark tuning changes against this result.
