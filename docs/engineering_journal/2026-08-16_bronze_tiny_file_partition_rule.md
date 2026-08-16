# 2026-08-16 Bronze Tiny-File Partition Rule

## Problem

Bronze auto partition planning previously used this local rule:

```text
small file -> minimum 2 output partitions
```

That was production-portable, but it was too much for extremely tiny tables like
`regions` and `service_plans`.

Example:

```text
regions source size: 963 bytes
previous planned output partitions: 2
```

For a tiny file, writing two output partitions adds task and file-commit
overhead without any real parallelism benefit.

## Optimization

Added a tiny-file rule to the shared partition planner:

```yaml
bronze:
  output_partitioning:
    strategy: auto
    tiny_file_threshold_mb: 1
    tiny_file_partitions: 1
    target_file_size_mb: 128
    min_partitions: 2
    max_partitions: 64
```

Meaning:

```text
If source file <= 1 MB -> write 1 output partition.
If source file > 1 MB -> use normal auto planning with minimum 2 partitions.
```

This keeps the design production-portable:

```text
Tiny dimension files avoid unnecessary output files.
Larger event files still use multiple partitions.
Large future files still scale by source size.
```

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/utils/partitioning.py
src/ingestion/bronze_ingestion.py
src/jobs/run_quality_checks.py
tests/unit/test_partitioning.py
```

The planner now accepts:

```text
tiny_file_threshold_mb
tiny_file_partitions
```

## Benchmark

Before tiny-file rule:

```text
profile=tiny_messy_quality_persist_writes
batch_id=BATCH_QUALITY_PERSIST_WRITES_20260816_001
total runtime=28.906 sec
bronze runtime=5.657 sec
```

After tiny-file rule:

```text
profile=tiny_messy_bronze_tiny_rule
batch_id=BATCH_BRONZE_TINY_RULE_20260816_001
total runtime=28.188 sec
bronze runtime=5.376 sec
```

Bronze table changes:

| Table | Before | After | Partitions Before | Partitions After |
| --- | ---: | ---: | ---: | ---: |
| `regions` | 2.359 sec | 2.249 sec | 2 | 1 |
| `towers` | 0.338 sec | 0.288 sec | 2 | 1 |
| `service_plans` | 0.246 sec | 0.229 sec | 2 | 1 |
| `subscribers` | 0.353 sec | 0.290 sec | 2 | 1 |
| `devices` | 0.281 sec | 0.297 sec | 2 | 1 |
| `network_events` | 0.847 sec | 0.667 sec | 2 | 2 |
| `calls` | 0.376 sec | 0.513 sec | 2 | 2 |
| `data_sessions` | 0.526 sec | 0.563 sec | 2 | 2 |
| `tower_alarms` | 0.328 sec | 0.275 sec | 2 | 1 |

## Result

```text
Bronze runtime: 5.657 sec -> 5.376 sec
Bronze improvement: 0.281 sec faster

Total runtime: 28.906 sec -> 28.188 sec
Total improvement: 0.718 sec faster
```

## Lesson

A simple minimum partition rule is not enough. Tiny files need their own rule.

Better logic:

```text
tiny file -> 1 partition
small/medium file -> configured minimum partitions
large file -> source-size-based partition count
```

## Interview Explanation

```text
I improved Bronze output planning by adding a tiny-file rule. Earlier, every
small file used at least two output partitions, which was wasteful for dimension
tables that were less than 1 MB. I changed the planner so files under 1 MB use
one partition, while larger files still scale using the target file size. This
reduced Bronze runtime from 5.657 seconds to 5.376 seconds and improved the full
pipeline from 28.906 seconds to 28.188 seconds.
```
