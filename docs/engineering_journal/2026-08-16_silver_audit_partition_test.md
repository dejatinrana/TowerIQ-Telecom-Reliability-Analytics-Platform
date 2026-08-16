# 2026-08-16 Silver Audit Mode And Output Partition Test

## Problem

Silver enriched event tables were still among the slower tasks:

```text
silver.network_events_enriched
silver.data_sessions_enriched
silver.calls_enriched
```

Two possible optimizations were tested:

```text
1. Silver output partition planning.
2. Silver audit mode for optional count/metric actions.
```

## Change 1: Silver Output Partition Planning

Silver now supports config-driven output partition planning:

```yaml
silver:
  output_partitioning:
    strategy: none
    tiny_file_threshold_mb: 1
    tiny_file_partitions: 1
    target_file_size_mb: 128
    min_partitions: 2
    max_partitions: 64
```

The feature is available, but the default strategy is `none` because the local
benchmark showed that forcing Silver output partitions made writes slower.

## Change 2: Silver Audit Mode

Silver now supports:

```yaml
audit:
  count_silver_outputs: true
```

When disabled, Silver skips:

```text
input_count
deduplicated_input_count
duplicates_removed
late_arriving_count
```

and records those metrics as `None`.

## Benchmark

Previous best full run:

```text
profile=tiny_messy_bronze_tiny_rule
batch_id=BATCH_BRONZE_TINY_RULE_20260816_001
total runtime=28.188 sec
silver runtime=5.259 sec
```

Silver audit disabled with output partition planning:

```text
profile=tiny_messy_silver_audit_partition
batch_id=BATCH_SILVER_AUDIT_PARTITION_20260816_001
total runtime=28.550 sec
silver runtime=5.490 sec
```

Silver audit disabled without output partition forcing:

```text
profile=tiny_messy_silver_audit_only
batch_id=BATCH_SILVER_AUDIT_ONLY_20260816_001
total runtime=29.894 sec
silver runtime=5.486 sec
```

Silver task comparison:

| Task | Before | Audit + Partition | Audit Only |
| --- | ---: | ---: | ---: |
| `network_events_enriched` | 1.283 sec | 1.597 sec | 1.525 sec |
| `calls_enriched` | 1.043 sec | 1.128 sec | 0.973 sec |
| `data_sessions_enriched` | 1.128 sec | 1.145 sec | 1.213 sec |
| `tower_alarms_enriched` | 0.596 sec | 0.639 sec | 0.563 sec |

## Result

Silver audit mode reduced control metric time, but the write path became slower
enough that the full Silver stage did not improve.

Example:

```text
network_events_enriched control: 0.409 sec -> 0.082 sec
network_events_enriched write:   0.874 sec -> 1.443 sec
```

For the current local dataset, the best measured behavior is:

```yaml
silver:
  output_partitioning:
    strategy: none

audit:
  count_silver_outputs: true
```

## Decision

Keep both features available, but do not enable them for local fast mode right
now:

```text
Silver output partition planning: available, default off.
Silver audit skipping: available, local_fast keeps counts on.
```

## Lesson

An optimization that reduces one part of a task can still make the whole task
slower.

For Silver, skipping metric actions reduced control time, but it did not improve
end-to-end writes at this scale. We should revisit this when the dataset is much
larger.

## Interview Explanation

```text
I added configurable Silver audit mode and output partition planning, then
benchmarked both. The result showed that skipping Silver metrics reduced control
time, but the write path became slower, so the full Silver stage did not improve.
I kept the features available but left them disabled for the local fast profile,
which shows I validate optimizations with measurements instead of assuming they
help.
```
