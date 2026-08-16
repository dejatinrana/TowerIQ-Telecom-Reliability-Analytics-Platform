# 2026-08-16 Bronze Auto Partition Planning

## Problem

Bronze writes were production-style append writes, but the number of output files
was not planned from the size of the incoming raw data.

That creates two possible problems:

```text
Small local data can create unnecessary file-management overhead.
Large future data can create too few or too many output files if partitioning is hardcoded.
```

The important design goal was:

```text
Do not hardcode local-only partition behavior.
Use cheap source file metadata to make the write decision configurable and portable.
```

## Design

Bronze now uses raw CSV file size metadata to plan output partitions before
writing Parquet.

The basic formula is:

```text
ceil(source_file_size / target_file_size) bounded by min_partitions and max_partitions
```

Local config:

```yaml
bronze:
  output_partitioning:
    strategy: auto
    target_file_size_mb: 128
    min_partitions: 2
    max_partitions: 64
```

This means:

```text
Small files -> 2 output partitions
Medium files -> more partitions based on size
Large files -> capped at 64 partitions in local config
```

In production, the same logic can be tuned by changing config only:

```text
target_file_size_mb can be increased.
min_partitions can be adjusted.
max_partitions can be raised based on cluster size.
strategy can be disabled if the storage engine should decide naturally.
```

## Code Changes

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/utils/partitioning.py
src/ingestion/bronze_ingestion.py
src/jobs/run_bronze_ingestion.py
tests/unit/test_partitioning.py
tests/unit/test_bronze_audit_mode.py
```

New utility functions:

```text
get_storage_path_size_bytes()
plan_output_partitions()
apply_output_partition_plan()
```

Bronze results now include:

```text
source_size_bytes
planned_output_partitions
```

This makes the write decision visible in benchmark output.

## Benchmark

Previous fast benchmark with Bronze and Gold audit counts disabled:

```text
profile=tiny_messy_bronze_fast_audit
batch_id=BATCH_BRONZE_FAST_AUDIT_20260816_001
total runtime=28.489 sec
bronze runtime=4.808 sec
```

New benchmark with Bronze auto partition planning:

```text
profile=tiny_messy_bronze_auto_partitions
batch_id=BATCH_BRONZE_AUTO_PARTITIONS_20260816_001
total runtime=28.604 sec
bronze runtime=5.577 sec
```

Bronze planned partitions:

| Table | Source Size Bytes | Planned Output Partitions |
| --- | ---: | ---: |
| `regions` | 963 | 2 |
| `towers` | 8,965 | 2 |
| `service_plans` | 685 | 2 |
| `subscribers` | 349,811 | 2 |
| `devices` | 391,793 | 2 |
| `network_events` | 8,446,205 | 2 |
| `calls` | 3,447,357 | 2 |
| `data_sessions` | 5,839,205 | 2 |
| `tower_alarms` | 158,243 | 2 |

## Result

```text
Bronze runtime: 4.808 sec -> 5.577 sec
Bronze result: 0.769 sec slower

Total runtime: 28.489 sec -> 28.604 sec
Total result: 0.115 sec slower
```

This is not a failure of the design. It tells us that for this small local
dataset, forcing two output partitions adds more Spark write overhead than it
saves. The design is still valuable because it prepares the pipeline for larger
data without hardcoding a local-only rule.

## Why 11 Rows Can Still Take Seconds

The `regions` table has only 11 rows, but Spark still spent about 2.496 seconds
on that Bronze task in the auto-partition benchmark.

That time is not mostly spent processing 11 rows. It is mostly Spark's fixed
execution overhead:

```text
Build the logical and physical plan.
Read CSV metadata, header, and schema.
Create Spark stages and tasks.
Coordinate local worker execution.
Add Bronze metadata columns.
Plan output partitions.
Write Parquet files.
Create batch partition folders.
Create success marker files.
Commit the output write.
```

This is why tiny tables can look slow in Spark. Spark is a distributed-style
processing engine, so every job pays a minimum coordination cost even when the
data itself is very small.

Simple takeaway:

```text
For tiny data, runtime is dominated by Spark planning and write overhead.
For larger data, Spark's parallel processing benefit becomes more useful.
```

## Lesson

Automatic partition planning is a production-portable design, but the local
minimum partition count must be chosen carefully.

```text
For tiny local data, 1 output partition may be fastest.
For this project requirement, we configured 2 minimum partitions.
For larger data, the planner will scale partitions from raw file size.
For production, tune target_file_size_mb and max_partitions by cluster capacity.
```

## Interview Explanation

```text
I added a Bronze output partition planner that uses raw CSV file size metadata
instead of triggering expensive Spark counts. The planner calculates how many
Parquet output partitions to write based on a target file size and min/max
bounds. In the local benchmark, forcing two output partitions was slightly
slower for tiny data, but the design is production-portable because larger input
files automatically get more partitions by config rather than code changes.

I also learned that a table with only a few rows can still take seconds in
Spark, not because the rows are hard to process, but because Spark must plan,
schedule, write, and commit a distributed-style job.
```
