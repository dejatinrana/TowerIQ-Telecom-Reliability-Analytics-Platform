# Experiment 01: Baseline Runtime

## Goal

Measure the current TowerIQ pipeline runtime before applying a new optimization.

This experiment answers:

```text
How long does the current full pipeline take on the selected dataset?
Which stages and tasks are slowest before the next optimization?
```

## Why This Experiment Matters

Baseline measurement is the first step in production-style optimization.

We should not optimize directly. We first need a stable measurement so every
future change can be compared against it.

The workflow is:

```text
run baseline
find bottleneck
choose one optimization
rerun same workload
compare before and after
document result
```

## Dataset Used

Current medium baseline:

```text
profile=medium_messy
batch_id=MEDIUM_MESSY_EXP001_BASELINE_20260817_001
raw_size=90 MB
total_csv_lines=559,306
```

Important event table sizes:

| Table | Rows |
| --- | ---: |
| `network_events` | 250,007 |
| `data_sessions` | 150,006 |
| `calls` | 100,006 |

## How To Run

Run the baseline experiment from the project root:

```bash
python3 -m experiments.01_baseline.run_baseline \
  --config configs/local_fast.yaml \
  --profile medium_messy \
  --batch-id MEDIUM_MESSY_EXP001_BASELINE_NEW
```

The result will be written under:

```text
experiments/01_baseline/results/
```

## Current Result

```text
total_runtime_seconds=46.435
```

| Stage | Runtime |
| --- | ---: |
| Bronze | 10.011 sec |
| Quality | 17.633 sec |
| SCD2 | 4.566 sec |
| Silver | 9.643 sec |
| Gold | 4.582 sec |

## Slowest Tasks

| Task | Runtime |
| --- | ---: |
| `quality.network_events` | 3.936 sec |
| `silver.network_events_enriched` | 3.660 sec |
| `quality.data_sessions` | 3.487 sec |
| `quality.calls` | 2.326 sec |
| `bronze.regions` | 2.287 sec |

## Evidence

Current baseline evidence:

```text
experiments/01_baseline/results/exp_001_baseline_medium_messy_MEDIUM_MESSY_EXP001_BASELINE_20260817_001.json
```

## Interpretation

The biggest bottleneck is the Quality layer, followed by Silver.

This means the next optimization experiments should focus first on:

```text
Quality event-table validation
Silver event enrichment joins
```

## Important Note

Local Spark timing can vary because of:

```text
JVM startup
OS file cache
existing output files
background system load
Spark planning overhead
```

So one baseline run is useful, but repeated baseline runs are better when we
need high confidence.
