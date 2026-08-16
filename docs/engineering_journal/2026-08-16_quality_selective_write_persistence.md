# 2026-08-16 Quality Selective Write Persistence

## Problem

The slowest Quality tasks were event-table writes:

```text
quality.network_events
quality.calls
quality.data_sessions
```

In fast audit mode, Quality skipped the valid/invalid count action. That removed
one Spark action, but it also meant the valid write and quarantine write could
recompute the same validation lineage separately.

Bad pattern:

```text
classified validation DataFrame
-> valid filter -> write Silver
-> invalid filter -> write Quarantine
```

Without selective persistence, Spark can rerun the validation joins and rule
logic once for the valid write and again for the quarantine write.

## Optimization

Added selective persistence for the classified Quality DataFrame only for the
write-heavy event tables:

```yaml
quality:
  persist_classified_tables:
    - network_events
    - calls
    - data_sessions
```

The pipeline now persists the classified DataFrame for those tables before
writing valid and quarantine outputs, then unpersists it after the writes.

This is intentionally selective. We did not cache every Quality table because
earlier experiments showed broad caching can make the local workload slower.

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/jobs/run_quality_checks.py
tests/unit/test_quality_job_optimization.py
```

Quality job results now record:

```text
persisted_for_write
```

This makes it visible which tables used the optimization.

## Benchmark

Before selective Quality write persistence:

```text
profile=tiny_messy_full_timing_latest
batch_id=BATCH_FULL_TIMING_LATEST_20260816_001
total runtime=30.076 sec
quality runtime=12.625 sec
```

After selective Quality write persistence:

```text
profile=tiny_messy_quality_persist_writes
batch_id=BATCH_QUALITY_PERSIST_WRITES_20260816_001
total runtime=28.906 sec
quality runtime=11.054 sec
```

Target task improvements:

| Task | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| `quality.network_events` | 2.224 sec | 1.816 sec | 0.408 sec faster |
| `quality.calls` | 1.833 sec | 1.178 sec | 0.655 sec faster |
| `quality.data_sessions` | 1.822 sec | 1.528 sec | 0.294 sec faster |

Write-time improvements:

| Task | Before Write | After Write | Improvement |
| --- | ---: | ---: | ---: |
| `quality.network_events` | 2.002 sec | 1.606 sec | 0.396 sec faster |
| `quality.calls` | 1.547 sec | 1.006 sec | 0.541 sec faster |
| `quality.data_sessions` | 1.614 sec | 1.330 sec | 0.284 sec faster |

## Result

```text
Quality runtime: 12.625 sec -> 11.054 sec
Quality improvement: 1.571 sec faster

Total runtime: 30.076 sec -> 28.906 sec
Total improvement: 1.170 sec faster
```

## Lesson

Removing count actions is useful, but if the same DataFrame is then written
twice, Spark may recompute the lineage twice.

Better pattern:

```text
Persist only the expensive classified DataFrames that feed multiple writes.
Write valid output.
Write quarantine output.
Unpersist immediately.
```

Do not cache everything. Cache only where the DataFrame is expensive enough and
reused by multiple actions.

## Interview Explanation

```text
After removing Quality audit counts, I noticed the valid and quarantine writes
could recompute the same validation lineage. I added selective persistence only
for the write-heavy event tables: network_events, calls, and data_sessions.
That reduced Quality runtime from 12.625 seconds to 11.054 seconds and improved
the full pipeline from 30.076 seconds to 28.906 seconds.
```
