# 2026-08-16 Gold Audit Mode Optimization

## Problem

Gold KPI tables were doing this pattern:

```python
output_count = result.dataframe.count()
result.dataframe.write.parquet(...)
```

This is useful for audit because it confirms that each KPI table produced rows.
But in Spark, `count()` and `write()` are both actions. Unless the DataFrame is
cached, Spark can execute the KPI lineage once for the count and again for the
write.

For small local data, the count is often more expensive than the business value
it provides during performance testing.

## Design Decision

Keep audit counts enabled by default, but make them configurable.

Normal local config:

```yaml
audit:
  count_gold_outputs: true
```

Fast benchmark config:

```yaml
audit:
  count_gold_outputs: false
```

This creates two clear modes:

| Mode | Behavior | Use Case |
| --- | --- | --- |
| Audit enabled | Count Gold output rows before writing | Correctness checks, normal development, demos |
| Audit reduced | Skip Gold output counts and write directly | Runtime benchmarking, performance experiments |

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/jobs/run_gold_kpis.py
tests/unit/test_gold_kpis.py
```

Gold job result now records whether the audit count was enabled:

```text
output_count
audit_count_enabled
```

When `count_gold_outputs=false`, `output_count` is `None` and
`audit_count_enabled` is `False`. This keeps the report honest instead of
pretending we measured a row count.

## Benchmark

Audit-enabled previous best:

```text
profile=tiny_messy_silver_join_opt
total runtime=28.095 sec
gold runtime=3.927 sec
```

Audit-reduced benchmark:

```text
profile=tiny_messy_fast_audit
batch_id=BATCH_FAST_AUDIT_20260816_001
total runtime=27.920 sec
gold runtime=3.089 sec
```

Gold task timings with output counts skipped:

| Gold Table | Runtime | Output Count | Audit Count Enabled |
| --- | ---: | --- | --- |
| `region_daily_kpis` | 0.764 sec | `None` | `False` |
| `tower_daily_kpis` | 0.670 sec | `None` | `False` |
| `subscriber_segment_daily_kpis` | 0.399 sec | `None` | `False` |
| `network_type_daily_kpis` | 0.368 sec | `None` | `False` |

## Result

```text
Gold runtime: 3.927 sec -> 3.089 sec
Gold improvement: 0.838 sec faster

Total runtime: 28.095 sec -> 27.920 sec
Total improvement: 0.175 sec faster
```

The total improvement is small because Gold was already relatively optimized,
but the design is still valuable because it gives the project a production-style
audit/performance switch.

## Interview Explanation

```text
I added a configurable audit mode for Gold KPI output counts. In audit mode, the
pipeline counts Gold rows before writing, which improves observability but costs
an extra Spark action. In fast benchmark mode, the pipeline skips that count and
writes directly, recording output_count as null and audit_count_enabled as false.
This shows that I understand the tradeoff between auditability and runtime.
```

## Lesson

Not every validation metric must run in every environment.

```text
Correctness mode should favor auditability.
Benchmark mode can reduce nonessential Spark actions.
Production can choose based on SLA, monitoring, and data quality requirements.
```
