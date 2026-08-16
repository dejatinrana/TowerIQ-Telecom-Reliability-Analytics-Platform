# EXP-002 Runtime Overhead Reduction

## Problem

EXP-001 showed that the local pipeline took `58.404` seconds on the `tiny_messy`
profile. That was high for a small dataset.

One suspected cause was stage-level Spark startup overhead:

```text
Bronze  -> start Spark -> stop Spark
Quality -> start Spark -> stop Spark
SCD2    -> start Spark -> stop Spark
Silver  -> start Spark -> stop Spark
Gold    -> start Spark -> stop Spark
```

## Solution

Refactor the stage runners so each can accept an existing Spark session.

The optimized pipeline now does:

```text
start Spark once
  -> Bronze
  -> Quality
  -> SCD2
  -> Silver
  -> Gold
stop Spark once
```

Standalone stage commands still work because each stage only stops Spark when it
created the session itself.

## Result

| Experiment | Runtime |
| --- | ---: |
| EXP-001 baseline | 58.404 sec |
| EXP-002 optimized | 53.884 sec |

Improvement:

```text
4.520 seconds faster
7.74% faster
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
| `bronze` | 7.441 |
| `quality` | 25.090 |
| `scd2` | 3.195 |
| `silver` | 9.587 |
| `gold` | 8.571 |

## What This Proves

Reusing one Spark session reduced fixed overhead, but it did not solve the main
bottleneck. Quality is still the slowest stage, which means the next performance
work should focus on repeated actions, joins, caching, broadcast strategy, and
shuffle settings.

## Unit Tests

Latest test result:

```text
25 passed
```

## Interview Version

I measured a baseline runtime, identified repeated Spark session startup as a
likely overhead source, refactored the pipeline to reuse one Spark session, and
measured a 7.74% runtime improvement. The experiment also showed that Quality
remained the main bottleneck, giving a clear next optimization target.

## Next Experiment

Suggested next experiment:

```text
EXP-003 Quality Stage Optimization
```

Candidate optimizations:

- Reduce repeated `.count()` actions.
- Cache small reference tables.
- Broadcast small dimensions.
- Tune local shuffle partitions.
- Measure join and file-read behavior.
