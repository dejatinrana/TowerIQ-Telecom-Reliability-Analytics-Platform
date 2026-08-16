# 2026-08-16 Quality Audit Mode, Partition Planning, And Affected Tables

## Problem

The Quality layer was still one of the heavier stages in the local pipeline.
The validation rules themselves were not the main problem. Most time was spent
around Spark actions and output writes.

Latest timing before this pass:

```text
profile=tiny_messy_bronze_auto_partitions
batch_id=BATCH_BRONZE_AUTO_PARTITIONS_20260816_001
quality runtime=11.777 sec
```

The table-level timing showed the pattern clearly:

| Table | Runtime | Validation Build | Write/Action Time |
| --- | ---: | ---: | ---: |
| `network_events` | 2.062 sec | 0.246 sec | 1.816 sec |
| `data_sessions` | 1.706 sec | 0.210 sec | 1.496 sec |
| `calls` | 1.394 sec | 0.228 sec | 1.166 sec |
| `regions` | 1.250 sec | 0.147 sec | 1.103 sec |
| `subscribers` | 1.218 sec | 0.128 sec | 1.090 sec |

The main lesson:

```text
Quality rules are cheap to build.
Quality actions and writes dominate runtime.
```

## Change 1: Quality Audit Mode

Quality now has a configurable audit switch:

```yaml
audit:
  count_quality_outputs: true
```

Fast benchmark config:

```yaml
audit:
  count_quality_outputs: false
```

When audit counts are enabled, Quality counts valid and invalid rows before
writing. This improves observability.

When audit counts are disabled, Quality writes directly and records:

```text
bronze_count=None
valid_count=None
invalid_count=None
audit_count_enabled=False
```

This keeps benchmark output honest because the row counts were intentionally not
measured.

## Change 2: Quality Output Partition Planning

Quality now supports output partition planning using the same reusable
partitioning utility as Bronze.

Local config:

```yaml
quality:
  output_partitioning:
    strategy: auto
    target_file_size_mb: 128
    min_partitions: 1
    max_partitions: 64
```

The planner uses the Bronze Parquet input size as cheap metadata for deciding
how many output partitions to target.

Current local result:

```text
planned_output_partitions=1 for all Quality tables
```

That is appropriate for the current 50k-row local benchmark because the output
is still small.

## Change 3: Affected-Table Quality

Quality can now validate and write only selected affected tables:

```bash
python -m src.jobs.run_quality_checks \
  --config configs/local_fast.yaml \
  --profile tiny_messy \
  --batch-id BATCH_ID \
  --tables network_events,calls
```

The important part is that this is dependency-aware.

If we ask Quality to write only:

```text
network_events
```

Quality still validates the parent tables needed for foreign-key checks:

```text
regions
service_plans
towers
subscribers
devices
network_events
```

But it writes only:

```text
network_events
```

This avoids unnecessary Quality output writes for unaffected tables while still
protecting referential integrity.

## Important Caveat

Affected-table Quality is the first step only.

The full pipeline is not yet fully affected-table-aware because Silver and Gold
still expect the normal set of downstream inputs. So affected-table Quality is
safe as a Quality-stage optimization, but a full production incremental design
should also make Silver and Gold recompute only affected tables/dates.

Next related future work:

```text
Affected-table Silver transformations.
Affected-date Gold recomputation.
Dependency-aware incremental orchestration across all layers.
```

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/jobs/run_quality_checks.py
src/jobs/run_incremental_pipeline.py
tests/unit/test_quality_job_optimization.py
tests/unit/test_incremental_pipeline.py
```

## Benchmark

Previous benchmark:

```text
profile=tiny_messy_bronze_auto_partitions
batch_id=BATCH_BRONZE_AUTO_PARTITIONS_20260816_001
total runtime=28.604 sec
quality runtime=11.777 sec
```

New benchmark with Quality audit counts disabled and Quality partition planning:

```text
profile=tiny_messy_quality_fast_audit
batch_id=BATCH_QUALITY_FAST_AUDIT_20260816_001
total runtime=28.494 sec
quality runtime=11.479 sec
```

Quality task timings:

| Table | Runtime | Counts | Audit Count Enabled | Planned Output Partitions |
| --- | ---: | --- | --- | ---: |
| `regions` | 1.139 sec | `None` | `False` | 1 |
| `service_plans` | 0.679 sec | `None` | `False` | 1 |
| `towers` | 1.024 sec | `None` | `False` | 1 |
| `subscribers` | 1.022 sec | `None` | `False` | 1 |
| `devices` | 0.882 sec | `None` | `False` | 1 |
| `network_events` | 2.268 sec | `None` | `False` | 1 |
| `calls` | 1.436 sec | `None` | `False` | 1 |
| `data_sessions` | 1.564 sec | `None` | `False` | 1 |
| `tower_alarms` | 0.661 sec | `None` | `False` | 1 |

## Result

```text
Quality runtime: 11.777 sec -> 11.479 sec
Quality improvement: 0.298 sec faster

Total runtime: 28.604 sec -> 28.494 sec
Total improvement: 0.110 sec faster
```

The improvement is small because Quality still writes both valid and quarantine
outputs for every table in a full run. The affected-table path is where this
design becomes more useful, because it can skip writes for tables that were not
part of the incoming batch.

## Interview Explanation

```text
I optimized Quality by separating auditability from benchmark speed. In normal
mode, Quality counts valid and invalid rows before writing. In fast benchmark
mode, it skips those count actions, writes directly, and records the count
fields as null. I also made Quality output partition planning config-driven and
added affected-table validation so incremental batches can validate only the
tables that changed while still validating parent dependencies for foreign-key
checks.
```
