# 2026-08-16 Gold Region KPI Persistence Test

## Problem

`region_daily_kpis` is built from `tower_daily_kpis`.

That creates a possible recomputation pattern:

```text
build tower_daily_kpis -> write tower_daily_kpis
build tower_daily_kpis again -> aggregate region_daily_kpis
```

The idea was to persist `tower_daily_kpis` when both `tower_daily_kpis` and
`region_daily_kpis` are selected.

## Test Design

Added a config-driven option:

```yaml
gold:
  persist_tower_daily_for_region: false
```

When enabled, Gold persists the shared `tower_daily_kpis` DataFrame before
writing Gold outputs, then unpersists it after the writes finish.

## Benchmark

Before the persistence test:

```text
profile=tiny_messy_bronze_tiny_rule
batch_id=BATCH_BRONZE_TINY_RULE_20260816_001
total runtime=28.188 sec
gold runtime=3.428 sec
```

With `persist_tower_daily_for_region=true`:

```text
profile=tiny_messy_gold_tower_cache
batch_id=BATCH_GOLD_TOWER_CACHE_20260816_001
total runtime=32.053 sec
gold runtime=4.417 sec
```

Gold task comparison:

| Gold Table | Before | With Persistence | Result |
| --- | ---: | ---: | --- |
| `tower_daily_kpis` | 0.685 sec | 1.099 sec | 0.414 sec slower |
| `region_daily_kpis` | 0.895 sec | 0.646 sec | 0.249 sec faster |
| `network_type_daily_kpis` | 0.518 sec | 0.821 sec | 0.303 sec slower |
| `subscriber_segment_daily_kpis` | 0.412 sec | 0.582 sec | 0.170 sec slower |

## Result

```text
Gold runtime: 3.428 sec -> 4.417 sec
Gold result: 0.989 sec slower

Total runtime: 28.188 sec -> 32.053 sec
Total result: 3.865 sec slower
```

`region_daily_kpis` improved, but the overall Gold stage became slower because
persisting and materializing `tower_daily_kpis` added more overhead than it
saved for this local 50k-row dataset.

## Decision

Keep the feature available, but disable it by default:

```yaml
gold:
  persist_tower_daily_for_region: false
```

This is useful because the same option may help later on a much larger dataset,
but it is not beneficial for the current local benchmark.

## Lesson

Caching/persisting a shared DataFrame is not automatically good.

Use this rule:

```text
Persist only if the saved recomputation cost is larger than the materialization,
memory, disk, and cache-management cost.
```

## Interview Explanation

```text
I tested whether persisting tower_daily_kpis would speed up region_daily_kpis,
because region KPIs depend on tower KPIs. It did make region_daily_kpis faster,
but the total Gold stage became slower because the cache materialization cost was
higher than the recomputation cost at this data size. I kept the feature
configurable but disabled by default, showing that I benchmark optimizations
instead of assuming caching always helps.
```
