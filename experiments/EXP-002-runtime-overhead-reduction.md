# EXP-002 Runtime Overhead Reduction

## Problem

EXP-001 showed high runtime on small data because each stage started its own
Spark session.

## Solution

Create one Spark session for the full pipeline run and pass it into Bronze,
Quality, SCD2, Silver, and Gold.

## Dataset

```text
profile=tiny_messy
batch_id=BATCH_EXP002_TINY_MESSY_20260814_001
```

## Runtime Comparison

```text
EXP-001 baseline: 58.404 seconds
EXP-002 optimized: 53.884 seconds
improvement: 4.520 seconds (7.74%)
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 7.441 |
| `quality` | 25.090 |
| `scd2` | 3.195 |
| `silver` | 9.587 |
| `gold` | 8.571 |

## Evidence

Detailed JSON result:

```text
experiments/results/exp_002_runtime_overhead_tiny_messy_BATCH_EXP002_TINY_MESSY_20260814_001.json
```
