# 2026-08-16 Bronze Audit Mode Optimization

## Problem

Bronze ingestion was doing this pattern for every raw table:

```python
raw_count = raw_df.count()
bronze_df.write.parquet(...)
```

That count is useful because it tells us how many rows were read from CSV before
writing to Bronze. But `count()` and `write()` are both Spark actions, so Spark
can scan the same raw input once for the audit count and again for the write.

For normal development, this audit count is helpful. For runtime experiments, it
can add extra work that does not change the output data.

## Design Decision

Keep Bronze input counts enabled by default, but make them configurable.

Normal local config:

```yaml
audit:
  count_bronze_inputs: true
```

Fast benchmark config:

```yaml
audit:
  count_bronze_inputs: false
```

This gives the pipeline two clear modes:

| Mode | Behavior | Use Case |
| --- | --- | --- |
| Audit enabled | Count raw input rows before writing Bronze | Correctness checks, demos, normal development |
| Audit reduced | Skip raw input counts and write directly | Performance benchmarks, repeated optimization tests |

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/ingestion/bronze_ingestion.py
src/jobs/run_bronze_ingestion.py
tests/unit/test_bronze_audit_mode.py
```

Bronze results now record:

```text
raw_count
bronze_count
audit_count_enabled
```

When `count_bronze_inputs=false`, both counts are stored as `None` and
`audit_count_enabled` is `False`. This is intentional because the pipeline did
not measure the count in that mode.

## Benchmark

Previous fast-audit benchmark with only Gold output counts disabled:

```text
profile=tiny_messy_fast_audit
batch_id=BATCH_FAST_AUDIT_20260816_001
total runtime=27.920 sec
bronze runtime=5.603 sec
```

New benchmark with both Bronze input counts and Gold output counts disabled:

```text
profile=tiny_messy_bronze_fast_audit
batch_id=BATCH_BRONZE_FAST_AUDIT_20260816_001
total runtime=28.489 sec
bronze runtime=4.808 sec
```

Bronze task timings with input counts skipped:

| Bronze Table | Runtime | Raw Count | Bronze Count | Audit Count Enabled |
| --- | ---: | --- | --- | --- |
| `regions` | 2.212 sec | `None` | `None` | `False` |
| `towers` | 0.260 sec | `None` | `None` | `False` |
| `service_plans` | 0.204 sec | `None` | `None` | `False` |
| `subscribers` | 0.291 sec | `None` | `None` | `False` |
| `devices` | 0.239 sec | `None` | `None` | `False` |
| `network_events` | 0.672 sec | `None` | `None` | `False` |
| `calls` | 0.362 sec | `None` | `None` | `False` |
| `data_sessions` | 0.372 sec | `None` | `None` | `False` |
| `tower_alarms` | 0.196 sec | `None` | `None` | `False` |

## Result

```text
Bronze runtime: 5.603 sec -> 4.808 sec
Bronze improvement: 0.795 sec faster

Total runtime: 27.920 sec -> 28.489 sec
Total result: 0.569 sec slower in this run
```

The Bronze stage improved, but the full pipeline did not improve because Quality
was slower in the same benchmark run:

```text
Quality runtime: 10.898 sec -> 12.244 sec
```

This is an important performance lesson: one stage can improve while total
runtime still moves in the other direction because local Spark timings have
fixed overhead and run-to-run variance.

## Interview Explanation

```text
I added configurable Bronze audit mode. In normal mode, Bronze counts raw input
rows before writing, which improves auditability. In fast benchmark mode, Bronze
skips that count and writes directly, recording the count fields as null and
audit_count_enabled as false. This shows that I understand Spark actions are not
free and that auditability should be configurable by environment.
```

## Lesson

Audit checks are valuable, but they should not be hardcoded into every runtime
path.

```text
Correctness mode favors row-count audit checks.
Benchmark mode removes nonessential actions.
Production chooses based on SLA, data criticality, and observability strategy.
```
