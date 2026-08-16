# 2026-08-16 Pipeline Performance Optimization Summary

## Purpose

This note records why the TowerIQ local Spark pipeline was slow, which code
patterns caused the overhead, what optimizations were applied, and what timing
improvement each change produced.

The main lesson is simple:

```text
Spark performance is not only about row count.
It is also about actions, shuffles, joins, recomputation, small files, and fixed job overhead.
```

For very small tables, the fixed overhead can be bigger than the actual data
work. For example, the `regions` table has only 11 rows, but Spark still has to
plan a job, create tasks, read CSV metadata, write Parquet files, create output
folders, and commit the write.

## Dataset Used For Benchmarks

The benchmark workload used the messy 50k event dataset family.

Approximate source scale:

| Table | Rows |
| --- | ---: |
| `network_events` | 50,007 |
| `data_sessions` | 30,006 |
| `calls` | 20,006 |
| `devices` | 5,504 |
| `subscribers` | 5,004 |
| `tower_alarms` | 806 |
| `towers` | 84 |
| `regions` | 11 |
| `service_plans` | 9 |

For fair timing comparisons, each benchmark used a fresh dataset profile so
previous appended output files did not distort the runtime.

## Runtime Timeline

| Step | Main Change | Total Runtime | Notes |
| --- | --- | ---: | --- |
| EXP-001 | Baseline pipeline | 58.404 sec | Separate Spark sessions and repeated Spark actions created overhead. |
| EXP-002 | One shared Spark session | 53.884 sec | Avoided starting Spark separately for each stage. |
| EXP-003 | Removed some redundant actions | 68.487 sec | Code cleanup was valid, but runtime did not improve; Quality and shuffle overhead dominated. |
| EXP-004 | Set local shuffle partitions to 8 | 43.552 sec | Reduced tiny shuffle task overhead from Spark's default 200 partitions. |
| Quality optimization pass | Broadcast references, reduce counts, cache selected parent DataFrames, coalesce outputs | 35.162 sec | Quality dropped from 24.294 sec to 12.970 sec in the measured comparison. |
| Selective persistence test | Tested broader persistence, then removed harmful caches | 36.160 sec | Proved that caching too much slows this local workload. |
| Silver join optimization | Built reusable contexts once and broadcasted them | 28.095 sec | Silver dropped from 7.397 sec to 4.807 sec; full pipeline went under 30 sec. |
| Gold audit mode | Made Gold output counts configurable | 27.920 sec | Gold output counts can be skipped for benchmark mode while remaining enabled by default. |
| Bronze audit mode | Made Bronze input counts configurable | 28.489 sec | Bronze improved from 5.603 sec to 4.808 sec, but total runtime did not improve because Quality varied higher in the same run. |
| Bronze auto partition planning | Planned Bronze output partitions from raw CSV file size | 28.604 sec | Production-portable design, but forcing two output partitions was slightly slower for this tiny local dataset. |
| Quality audit and affected tables | Made Quality counts configurable and added affected-table selection | 28.494 sec | Quality improved from 11.777 sec to 11.479 sec; bigger benefit is expected when only some tables are affected. |
| Silver/Gold affected processing | Propagated affected-table context to Silver and Gold | 20.67 sec affected run | `network_events` affected run wrote only dependent Silver and Gold outputs. |
| Quality selective write persistence | Persisted classified DataFrames for write-heavy Quality event tables | 28.906 sec | Quality improved from 12.625 sec to 11.054 sec in the latest full timing comparison. |
| Bronze tiny-file rule | Used 1 partition for Bronze source files <= 1 MB | 28.188 sec | Bronze improved from 5.657 sec to 5.376 sec; full pipeline improved by 0.718 sec. |
| Gold tower persistence test | Tested persisting `tower_daily_kpis` for reuse by region KPIs | 32.053 sec | Region KPI improved, but Gold and total runtime got slower; disabled by default. |
| Silver audit and partition test | Tested Silver audit skipping and output partition planning | 28.550 sec / 29.894 sec | Both variants were slower than the 28.188 sec baseline; features kept available but disabled in local fast mode. |
| Developer fast workflow | Added stage-specific local runner and dev-fast config | Workflow change | Avoids rerunning unchanged stages during small-dataset development. |
| Developer fast benchmark | Compared full affected flow with Silver-only and Gold-only reruns | 20.454 sec full vs 12.885 sec Silver+Gold | About 37% faster for a common edit loop. |

## Final Measured Runtime

Final benchmark:

```text
profile=tiny_messy_silver_join_opt
batch_id=BATCH_SILVER_JOIN_OPT_20260816_001
```

Final stage timings:

| Stage | Runtime Seconds |
| --- | ---: |
| Bronze | 5.527 |
| Quality | 10.943 |
| SCD2 | 2.891 |
| Silver | 4.807 |
| Gold | 3.927 |
| Total | 28.095 |

Latest audit-reduced benchmark:

```text
profile=tiny_messy_fast_audit
batch_id=BATCH_FAST_AUDIT_20260816_001
total runtime=27.920 sec
```

Latest Bronze plus Gold audit-reduced benchmark:

```text
profile=tiny_messy_bronze_fast_audit
batch_id=BATCH_BRONZE_FAST_AUDIT_20260816_001
total runtime=28.489 sec
```

Latest Bronze auto-partition benchmark:

```text
profile=tiny_messy_bronze_auto_partitions
batch_id=BATCH_BRONZE_AUTO_PARTITIONS_20260816_001
total runtime=28.604 sec
```

Latest Quality audit-reduced benchmark:

```text
profile=tiny_messy_quality_fast_audit
batch_id=BATCH_QUALITY_FAST_AUDIT_20260816_001
total runtime=28.494 sec
```

Latest affected-table incremental run:

```text
profile=tiny_messy_silver_gold_aware
batch_id=BATCH_SILVER_GOLD_AWARE_NETWORK_ONLY_20260816_001
affected table=network_events
wall clock runtime=20.67 sec
```

Latest selective Quality write-persistence run:

```text
profile=tiny_messy_quality_persist_writes
batch_id=BATCH_QUALITY_PERSIST_WRITES_20260816_001
total runtime=28.906 sec
quality runtime=11.054 sec
```

Latest Bronze tiny-file rule run:

```text
profile=tiny_messy_bronze_tiny_rule
batch_id=BATCH_BRONZE_TINY_RULE_20260816_001
total runtime=28.188 sec
bronze runtime=5.376 sec
```

Latest Gold tower persistence test:

```text
profile=tiny_messy_gold_tower_cache
batch_id=BATCH_GOLD_TOWER_CACHE_20260816_001
total runtime=32.053 sec
gold runtime=4.417 sec
```

Latest Silver audit/partition tests:

```text
profile=tiny_messy_silver_audit_partition
batch_id=BATCH_SILVER_AUDIT_PARTITION_20260816_001
total runtime=28.550 sec
silver runtime=5.490 sec

profile=tiny_messy_silver_audit_only
batch_id=BATCH_SILVER_AUDIT_ONLY_20260816_001
total runtime=29.894 sec
silver runtime=5.486 sec
```

## Problem 1: Starting Spark More Than Once

### Bad Pattern

The first baseline ran each stage like an independent job:

```text
Bronze starts Spark
Quality starts Spark
SCD2 starts Spark
Silver starts Spark
Gold starts Spark
```

Starting Spark has fixed overhead even before real data processing begins.
For a 50k-row local dataset, this overhead is very visible.

The same idea applies inside individual tiny table jobs. A table can have only
9 or 11 rows, but Spark still pays planning, scheduling, and write-commit costs.
So when `regions` takes a couple of seconds, the time is not mostly row
processing. It is mostly Spark's minimum execution cost.

### Optimization

Created one Spark session for the full pipeline and passed it into each stage:

```text
create one Spark session
run Bronze with same Spark
run Quality with same Spark
run SCD2 with same Spark
run Silver with same Spark
run Gold with same Spark
stop Spark once
```

Main files:

```text
src/jobs/run_incremental_pipeline.py
experiments/runtime_overhead_reduction.py
src/utils/spark.py
```

### Result

```text
58.404 sec -> 53.884 sec
Improvement: 4.520 sec faster
```

## Problem 2: Too Many Tiny Shuffle Tasks

### Bad Pattern

Spark was using the default:

```text
spark.sql.shuffle.partitions = 200
```

This is often reasonable for larger cluster workloads, but it was too high for
local development. Spark was creating many tiny shuffle tasks for joins,
duplicate checks, and aggregations.

### Optimization

Made shuffle partitions config-driven and set the local value to 8:

```yaml
spark:
  shuffle_partitions: 8
```

Main files:

```text
configs/local.yaml
src/utils/spark.py
src/jobs/run_bronze_ingestion.py
src/jobs/run_quality_checks.py
src/jobs/run_silver_transformations.py
src/jobs/run_scd2_dimensions.py
src/jobs/run_gold_kpis.py
src/jobs/run_incremental_pipeline.py
```

### Result

```text
68.487 sec -> 43.552 sec
Improvement: 24.935 sec faster
```

Quality stage comparison:

```text
39.456 sec -> 23.485 sec
Improvement: 15.971 sec faster
```

### Lesson

Do not hardcode `8` for production. Keep it config-driven. In production, this
should be tuned based on cluster size, data size, shuffle volume, and executor
configuration.

## Problem 3: Quality Validation Did Too Much Repeated Work

### Bad Patterns

Quality originally had several expensive patterns:

```text
1. Read Bronze again only to count rows.
2. Count valid rows separately.
3. Count invalid rows separately.
4. Write valid rows.
5. Write invalid rows.
6. Reuse valid parent dimensions without selective persistence.
7. Join small reference tables without broadcast hints.
8. Run duplicate-count joins on event tables even though Silver handles event deduplication.
```

The worst pattern was that every `count()` and `write()` could trigger Spark to
execute the validation lineage again unless the reused DataFrame was persisted.

Example of expensive logic:

```python
valid_count = result.valid.count()
invalid_count = result.invalid.count()
result.valid.write.parquet(...)
result.invalid.write.parquet(...)
```

This can execute the same validation work multiple times.

### Optimizations

#### 1. Broadcast small reference keys

Reference checks now broadcast small key sets:

```python
return df.join(F.broadcast(keys), F.col(left_column) == F.col(alias), "left")
```

This avoids unnecessary shuffle joins when validating foreign keys.

Main file:

```text
src/quality/table_validators.py
```

#### 2. Persist valid parent dimensions

Valid parent tables are reused by child validations:

```text
regions -> towers, subscribers
service_plans -> subscribers
towers -> network_events, calls, data_sessions, tower_alarms
subscribers -> devices, network_events, calls, data_sessions
devices -> network_events, calls, data_sessions
```

Those valid parent DataFrames are now persisted with:

```python
StorageLevel.MEMORY_AND_DISK
```

Then released after Quality finishes.

Main files:

```text
src/quality/table_validators.py
src/jobs/run_quality_checks.py
```

#### 3. Reduce Quality count actions

Quality now creates a classified DataFrame:

```text
_is_quality_valid = true/false
```

Then valid and invalid counts are calculated in one grouped action:

```python
result.classified.groupBy("_is_quality_valid").count().collect()
```

This replaced separate valid and invalid count actions.

#### 4. Skip duplicate-count joins for event tables in Quality

Duplicate event IDs are handled in Silver deduplication, so Quality no longer
does shuffle-heavy duplicate-count joins for:

```text
network_events
calls
data_sessions
tower_alarms
```

Dimension duplicate checks remain in Quality because dimension primary key
duplicates can break downstream reference integrity.

#### 5. Coalesce local Quality outputs

Local Quality writes use:

```yaml
quality:
  output_partitions: 1
```

This reduces small-file overhead for local development.

### Result

Before the Quality optimization pass:

```text
Total runtime: 45.098 sec
Quality runtime: 24.294 sec
```

After the Quality optimization pass:

```text
Total runtime: 35.162 sec
Quality runtime: 12.970 sec
```

Improvement:

```text
Total pipeline: 9.936 sec faster
Quality layer: 11.324 sec faster
```

Important task improvements:

| Task | Before | After |
| --- | ---: | ---: |
| `quality.network_events` | 4.552 sec | 2.059 sec |
| `quality.calls` | 4.316 sec | 1.543 sec |
| `quality.data_sessions` | 4.057 sec | 1.665 sec |
| `quality.devices` | 2.518 sec | 1.228 sec |

## Problem 4: Caching Too Much Made The Pipeline Slower

### Bad Assumption

A tempting assumption was:

```text
If a DataFrame is used more than once, cache it.
```

That is not always correct. Caching also has a cost:

```text
compute the DataFrame
serialize/store the DataFrame
use memory/disk
possibly evict other cached data
add cache management overhead
```

### What We Tested

We tested broader persistence in:

```text
Bronze
SCD2
Silver
Gold
```

### Result

```text
Previous good run: 34.388 sec
Over-cached run: 42.202 sec
Final selective-cache run: 36.160 sec
```

### Decision

Removed harmful caches from:

```text
Bronze DataFrames
SCD2 output DataFrames
Silver source/controlled DataFrames
Gold input/KPI DataFrames
```

Kept only:

```text
Quality valid parent DataFrames
Quality classified DataFrames
```

### Lesson

Use this rule:

```text
Persist only when the DataFrame is expensive, reused by multiple actions,
and cheaper to store than recompute.
```

Always benchmark after adding cache. If runtime gets worse, remove it.

## Problem 5: Silver Rebuilt The Same Context Joins Again And Again

### Bad Pattern

Silver enrichment originally rebuilt the same context DataFrames inside each
enrichment function.

Repeated work:

```text
tower + region context: built 4 times
subscriber + device + plan context: built 3 times
```

This pattern existed conceptually like this:

```python
def enrich_network_events(...):
    tower_context = tower_region_context(towers, regions)
    subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)

def enrich_calls(...):
    tower_context = tower_region_context(towers, regions)
    subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)
```

That repeated the same dimension joins for multiple event tables.

### Optimization

Build each context once in `build_enriched_event_tables()`:

```python
tower_context = tower_region_context(towers, regions)
subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)
```

Then pass those contexts into each enrichment:

```python
enrich_network_events(controlled_network_events, tower_context, subscriber_context)
enrich_calls(controlled_calls, tower_context, subscriber_context)
enrich_data_sessions(controlled_data_sessions, tower_context, subscriber_context)
enrich_tower_alarms(controlled_tower_alarms, tower_context)
```

The event joins now broadcast the reusable contexts:

```python
.join(F.broadcast(tower_context).alias("tower_ctx"), ...)
.join(F.broadcast(subscriber_context).alias("sub_ctx"), ...)
```

Main file:

```text
src/transformations/silver_events.py
```

### Result

Before Silver join optimization:

```text
Silver runtime: 7.397 sec
Total runtime: 36.160 sec
```

After Silver join optimization:

```text
Silver runtime: 4.807 sec
Total runtime: 28.095 sec
```

Improvement:

```text
Silver: 2.590 sec faster
Total pipeline: 8.065 sec faster
```

Silver task timings after optimization:

| Task | Runtime |
| --- | ---: |
| `network_events_enriched` | 1.318 sec |
| `data_sessions_enriched` | 1.048 sec |
| `calls_enriched` | 0.940 sec |
| `tower_alarms_enriched` | 0.553 sec |

## Problem 6: Gold Audit Counts Recomputed KPI DataFrames

### Bad Pattern

Gold KPI output used this pattern:

```python
output_count = result.dataframe.count()
result.dataframe.write.parquet(...)
```

Both `count()` and `write()` are Spark actions. That means the KPI DataFrame can
be computed once for the count and again for the write.

### Optimization

Made Gold output counting configurable:

```yaml
audit:
  count_gold_outputs: true
```

Default local mode keeps audit counts enabled. Fast benchmark mode uses:

```yaml
audit:
  count_gold_outputs: false
```

When disabled, Gold writes directly and records:

```text
output_count=None
audit_count_enabled=False
```

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/jobs/run_gold_kpis.py
tests/unit/test_gold_kpis.py
```

### Result

```text
Gold runtime: 3.927 sec -> 3.089 sec
Total runtime: 28.095 sec -> 27.920 sec
```

The total improvement was small, but the design is important because it shows a
clear auditability versus performance tradeoff.

## Problem 7: Bronze Input Counts Added Extra Spark Actions

### Bad Pattern

Bronze ingestion used this pattern for each table:

```python
raw_count = raw_df.count()
bronze_df.write.parquet(...)
```

Both `count()` and `write()` are Spark actions. The count is useful for audit,
but it can make Spark scan the raw data separately from the write path.

### Optimization

Made Bronze input counting configurable:

```yaml
audit:
  count_bronze_inputs: true
```

Default local mode keeps Bronze row-count auditing enabled. Fast benchmark mode
uses:

```yaml
audit:
  count_bronze_inputs: false
```

When disabled, Bronze writes directly and records:

```text
raw_count=None
bronze_count=None
audit_count_enabled=False
```

Main files:

```text
configs/local.yaml
configs/local_fast.yaml
src/ingestion/bronze_ingestion.py
src/jobs/run_bronze_ingestion.py
tests/unit/test_bronze_audit_mode.py
```

### Result

```text
Bronze runtime: 5.603 sec -> 4.808 sec
Bronze improvement: 0.795 sec faster

Total runtime: 27.920 sec -> 28.489 sec
Total result: 0.569 sec slower in this run
```

The Bronze stage got faster, but the full pipeline did not. Quality increased
from 10.898 sec to 12.244 sec in the same run, which absorbed the Bronze gain.

### Lesson

Do not judge an optimization only by total runtime. Look at both:

```text
1. The stage where the code changed.
2. The full pipeline runtime.
```

This change is still useful because it gives the project a clean audit versus
performance switch for Bronze, just like Gold.

## Problem 8: Bronze Output Partitions Needed A Production-Portable Plan

### Bad Pattern

Hardcoding Bronze output files is risky:

```text
coalesce(1) is convenient for tiny local data.
coalesce(1) is dangerous for large production data.
letting Spark decide everything can create too many small files locally.
```

### Optimization

Added a file-size-based partition planner.

The planner uses raw CSV file size metadata, which is cheap compared with a
Spark `count()` action:

```text
source_file_size / target_file_size = planned output partitions
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

Main files:

```text
src/utils/partitioning.py
src/ingestion/bronze_ingestion.py
src/jobs/run_bronze_ingestion.py
configs/local.yaml
configs/local_fast.yaml
tests/unit/test_partitioning.py
```

Bronze benchmark output now records:

```text
source_size_bytes
planned_output_partitions
```

### Result

```text
Bronze runtime: 4.808 sec -> 5.577 sec
Bronze result: 0.769 sec slower

Total runtime: 28.489 sec -> 28.604 sec
Total result: 0.115 sec slower
```

All current raw CSV files planned `2` output partitions because they are below
the 128 MB target size and the local minimum is set to `2`.

### Lesson

This was a production-readiness optimization more than a local-speed
optimization. The code is better prepared for large data, but the local
benchmark proves that forcing extra partitions for tiny data can add overhead.

## Problem 9: Quality Needed Audit Mode And Affected-Table Control

### Bad Pattern

Quality always counted valid and invalid records before writing:

```text
classified.groupBy("_is_quality_valid").count().collect()
write valid records
write invalid records
```

That count is valuable for auditing, but it is still a Spark action. In fast
benchmark mode, it can be skipped.

Quality also wrote every table in full runs, even though production incremental
batches may affect only a subset of tables.

### Optimization

Added configurable Quality audit mode:

```yaml
audit:
  count_quality_outputs: true
```

Fast config disables it:

```yaml
audit:
  count_quality_outputs: false
```

When disabled, Quality records:

```text
bronze_count=None
valid_count=None
invalid_count=None
audit_count_enabled=False
```

Added Quality output partition planning:

```yaml
quality:
  output_partitioning:
    strategy: auto
    target_file_size_mb: 128
    min_partitions: 1
    max_partitions: 64
```

Added affected-table selection:

```text
--tables network_events,calls
```

This selection is dependency-aware. If `network_events` is selected, Quality
still validates parent dependencies such as `regions`, `service_plans`,
`towers`, `subscribers`, and `devices`, but it writes only `network_events`.

### Result

```text
Quality runtime: 11.777 sec -> 11.479 sec
Quality improvement: 0.298 sec faster

Total runtime: 28.604 sec -> 28.494 sec
Total improvement: 0.110 sec faster
```

The full-run improvement is small because all Quality tables are still written.
The larger design value is for incremental workloads where only a subset of
tables changed.

### Caveat

Affected-table Quality is only one layer of the full incremental design.
Downstream Silver and Gold still need affected-table and affected-date logic
before the full pipeline can safely recompute only selected downstream outputs.

## Problem 10: Silver And Gold Needed Affected-Table / Affected-Date Processing

### Bad Pattern

After Quality became affected-table aware, Silver and Gold still had full-stage
behavior:

```text
Silver built all enriched event tables.
Gold built all KPI tables.
```

This wasted work when only one source event table changed.

### Optimization

Silver now maps changed source tables to enriched outputs:

```text
network_events -> network_events_enriched
calls -> calls_enriched
data_sessions -> data_sessions_enriched
tower_alarms -> tower_alarms_enriched
dimension table changes -> all enriched event tables
```

Gold now maps changed enriched tables to dependent KPI tables:

```text
network_events_enriched -> tower_daily, region_daily, network_type_daily
calls_enriched -> tower_daily, region_daily, subscriber_segment_daily
data_sessions_enriched -> tower_daily, region_daily, network_type_daily, subscriber_segment_daily
tower_alarms_enriched -> tower_daily, region_daily
```

Gold also collects affected `event_date` values from the changed enriched
tables and filters recomputation to those dates.

### Result

Affected `network_events` run:

```text
Quality wrote: network_events
Silver wrote: network_events_enriched
Gold wrote: tower_daily_kpis, region_daily_kpis, network_type_daily_kpis
Wall clock runtime: 20.67 sec
```

Compared with the full run on the same profile:

```text
Full run: 29.652 sec
Affected run: 20.67 sec
Improvement: 8.982 sec faster
```

### Caveat

This is affected-table and affected-date aware for event-table incremental
processing. A deeper future improvement is historical dimension backfill:

```text
dimension change -> impacted entity ids -> impacted dates -> recompute only those partitions
```

## Problem 11: Quality Valid And Quarantine Writes Recomputed The Same Lineage

### Bad Pattern

After Quality audit counts became optional, the fast mode avoided the count
action. But for larger event tables, Quality still wrote both valid and invalid
outputs:

```text
classified validation DataFrame
-> valid rows -> write Silver
-> invalid rows -> write Quarantine
```

Without persistence, Spark could recompute the same validation lineage for both
writes.

### Optimization

Persist only the write-heavy classified Quality DataFrames:

```yaml
quality:
  persist_classified_tables:
    - network_events
    - calls
    - data_sessions
```

Then:

```text
persist classified DataFrame
write valid rows
write invalid rows
unpersist classified DataFrame
```

### Result

```text
Quality runtime: 12.625 sec -> 11.054 sec
Quality improvement: 1.571 sec faster

Total runtime: 30.076 sec -> 28.906 sec
Total improvement: 1.170 sec faster
```

Target task improvements:

| Task | Before | After |
| --- | ---: | ---: |
| `quality.network_events` | 2.224 sec | 1.816 sec |
| `quality.calls` | 1.833 sec | 1.178 sec |
| `quality.data_sessions` | 1.822 sec | 1.528 sec |

### Lesson

Removing counts is not enough if the same expensive lineage feeds multiple
actions. Selective persistence is useful when a DataFrame is expensive and feeds
multiple writes.

## Problem 12: Bronze Tiny Files Were Still Using Too Many Output Partitions

### Bad Pattern

Bronze auto partition planning used a minimum of 2 output partitions for all
small files. That was too much for extremely tiny files:

```text
regions: 963 bytes
service_plans: 685 bytes
```

These tables do not benefit from two output files locally.

### Optimization

Added a tiny-file rule:

```yaml
bronze:
  output_partitioning:
    tiny_file_threshold_mb: 1
    tiny_file_partitions: 1
```

Now:

```text
source file <= 1 MB -> 1 partition
source file > 1 MB -> normal auto partition planning
```

### Result

```text
Bronze runtime: 5.657 sec -> 5.376 sec
Bronze improvement: 0.281 sec faster

Total runtime: 28.906 sec -> 28.188 sec
Total improvement: 0.718 sec faster
```

### Lesson

Partition planning should handle tiny files separately from regular small files.
The goal is not always "more partitions"; the goal is the right number of
partitions for the data size.

## Problem 13: Gold Region KPI Might Recompute Tower Daily KPI

### Hypothesis

`region_daily_kpis` depends on `tower_daily_kpis`, so persisting
`tower_daily_kpis` might prevent recomputation.

### Test

Added a config-driven switch:

```yaml
gold:
  persist_tower_daily_for_region: false
```

Then benchmarked with it enabled.

### Result

```text
Gold runtime: 3.428 sec -> 4.417 sec
Gold result: 0.989 sec slower

Total runtime: 28.188 sec -> 32.053 sec
Total result: 3.865 sec slower
```

Detailed Gold result:

| Table | Before | With Persistence |
| --- | ---: | ---: |
| `tower_daily_kpis` | 0.685 sec | 1.099 sec |
| `region_daily_kpis` | 0.895 sec | 0.646 sec |
| `network_type_daily_kpis` | 0.518 sec | 0.821 sec |
| `subscriber_segment_daily_kpis` | 0.412 sec | 0.582 sec |

### Decision

Keep the feature available, but disabled by default.

The test proved the important engineering lesson:

```text
Persisting can improve one downstream task while making the full stage slower.
```

## Problem 14: Silver Audit And Partition Planning Did Not Improve Local Runtime

### Hypothesis

Silver could become faster by:

```text
1. Skipping audit metric actions.
2. Planning output partitions before enriched writes.
```

### Result

Both tested variants were slower than the previous best full run:

```text
Previous best: 28.188 sec total, 5.259 sec Silver
Silver audit + partition: 28.550 sec total, 5.490 sec Silver
Silver audit only: 29.894 sec total, 5.486 sec Silver
```

Skipping Silver audit metrics did reduce control time:

```text
network_events_enriched control: 0.409 sec -> 0.082 sec
```

But write time increased:

```text
network_events_enriched write: 0.874 sec -> 1.443 sec
```

### Decision

Keep the code paths available, but do not enable them for local fast mode:

```yaml
silver:
  output_partitioning:
    strategy: none

audit:
  count_silver_outputs: true
```

### Lesson

Do not optimize by only looking at one sub-metric. End-to-end stage runtime is
the deciding metric.

## Problem 15: Some Correct Optimizations Did Not Improve The Small Local Run

### What Happened

Two recent optimizations were technically valid, but they did not improve the
current 50k-row local benchmark:

```text
1. Gold tower_daily_kpis persistence.
2. Silver audit skipping and output partition planning.
```

This looks confusing at first because each idea sounds reasonable:

```text
Gold: persist a shared KPI DataFrame so region KPI does not recompute it.
Silver: skip extra audit metrics and control output partitions.
```

But Spark performance depends on the full execution cost, not only the logical
idea.

### Why Gold Did Not Improve

Persisting `tower_daily_kpis` did help one downstream task:

```text
region_daily_kpis: 0.895 sec -> 0.646 sec
```

But the whole Gold stage became slower:

```text
Gold runtime: 3.428 sec -> 4.417 sec
```

The reason is that persisting is not free. Spark has to:

```text
compute the DataFrame
materialize it into memory/disk
manage the cached blocks
read it again for downstream work
unpersist it later
```

For the current small local dataset, that cache cost was larger than the
recomputation cost. So the feature is useful to keep, but it should stay
disabled until a larger benchmark proves that it helps.

### Why Silver Did Not Improve

Silver audit skipping reduced the control-metric part of one task:

```text
network_events_enriched control: 0.409 sec -> 0.082 sec
```

But the write path became slower:

```text
network_events_enriched write: 0.874 sec -> 1.443 sec
```

So the full Silver stage did not improve:

```text
Previous best Silver: 5.259 sec
Silver audit + partition: 5.490 sec
Silver audit only: 5.486 sec
```

The likely reason is that, on a small local dataset, Spark fixed overhead,
Parquet write planning, task scheduling, and partition handling dominate the
actual row processing. Removing one metric action does not automatically reduce
the final write cost.

### Senior Engineering Decision

The decision was:

```text
Keep the features available.
Disable them by default where benchmarks showed they hurt local runtime.
Re-test them later on larger datasets.
```

Current defaults:

```yaml
gold:
  persist_tower_daily_for_region: false

silver:
  output_partitioning:
    strategy: none

audit:
  count_silver_outputs: true
```

### Lesson

An optimization is only successful if the measured end-to-end runtime improves
for the workload being tested.

For small local Spark data, many costs are fixed overhead:

```text
query planning
task scheduling
shuffle setup
Parquet write setup
commit protocol
cache materialization
partition management
```

That is why a change can improve one sub-task but still make the full stage
slower.

### Interview Explanation

```text
I tested optimizations in Silver and Gold that were logically correct, but the
benchmarks showed they did not improve the local 50k-row workload. In Gold,
persisting tower_daily_kpis made region_daily_kpis faster, but slowed the full
Gold stage because cache materialization cost was higher than recomputation. In
Silver, skipping audit metrics reduced control time, but write time increased,
so the full stage became slower. I kept those features configurable but disabled
them by default. This shows that I use benchmark evidence instead of assuming
every optimization is useful.
```

## Problem 16: Small-Data Optimization Needed A Faster Developer Loop

### Problem

After several Spark-level optimizations, the small local dataset was still
paying fixed Spark overhead. Continuing to optimize tiny transformations was no
longer the best use of effort.

The real local-development problem was:

```text
I change one layer, but rerun the full affected pipeline.
```

### Optimization

Added:

```text
configs/dev_fast.yaml
src/jobs/run_dev_stage.py
```

The new runner can execute one stage at a time:

```text
bronze
quality
scd2
silver
gold
all
```

It also carries affected-table context so local runs can focus on changed
tables.

### Gold-Only Fix

Testing showed one bug in the new workflow: Gold-only did not know how to map
source affected tables to enriched Silver affected tables when Silver was not
run in the same command.

The fix was:

```text
network_events -> network_events_enriched
```

and similarly for the other event tables.

### Result

Measured with:

```text
profile=tiny_messy
batch_id=DEV_FAST_FULL_20260816_001
affected table=network_events
```

| Run | Wall Time |
| --- | ---: |
| Full affected dev flow | 20.454 sec |
| Silver-only | 5.437 sec |
| Gold-only | 7.448 sec |

Silver-only plus Gold-only:

```text
12.885 sec
```

Improvement compared with rerunning full affected flow:

```text
7.569 sec faster, about 37%
```

### Lesson

For small local Spark datasets, workflow optimization can be more useful than
more transformation optimization. The best fast-feedback loop is to rerun only
the layer that changed, then run a full checkpoint after several edits.

## Code Quality Lessons For Future Projects

### Avoid these patterns

```text
Starting Spark repeatedly inside one logical pipeline.
Using Spark's default shuffle partitions blindly for local workloads.
Calling count() several times on related DataFrames.
Counting output by reading back data that was just written.
Rebuilding the same dimension context in multiple transformations.
Joining small reference DataFrames without broadcast when appropriate.
Caching every DataFrame just because it is reused.
Running expensive audit counts in every environment without a config switch.
Counting raw Bronze inputs during performance benchmarks when the count is not needed.
Hardcoding coalesce(1) into Bronze writes for all environments.
Writing every Quality table when only a subset of tables changed.
Rebuilding every Silver and Gold output when only one event table changed.
Writing valid and quarantine outputs from the same expensive DataFrame without selective persistence.
Forcing multiple Bronze output partitions for files that are less than 1 MB.
Assuming a shared Gold DataFrame should be persisted without benchmarking.
Assuming skipped Silver audit metrics will automatically reduce Silver runtime.
Treating a sub-task improvement as a full-stage improvement.
Letting experiment outputs accumulate in the same profile and comparing polluted timings.
```

### Prefer these patterns

```text
Use one Spark session for one full batch run.
Keep Spark settings config-driven.
Broadcast small dimension/reference DataFrames.
Persist only expensive DataFrames reused by multiple actions.
Unpersist after use.
Combine related audit counts into one aggregation.
Move repeated context creation to one shared builder.
Benchmark every optimization with a fresh profile.
Keep honest notes when an optimization makes runtime worse.
Separate audit-heavy correctness mode from fast benchmark mode.
Record skipped audit counts as null instead of pretending they were measured.
Plan output partitions from cheap source metadata and keep min/max values config-driven.
Make incremental validation dependency-aware so parent checks still protect child tables.
Carry affected-table context from Quality into Silver and Gold.
Recompute Gold only for affected KPI dependencies and affected event dates.
Persist expensive classified Quality DataFrames only when they feed multiple writes.
Use a tiny-file partition rule before the normal min/max partition rule.
Disable optimizations that improve one task but hurt the full pipeline.
Keep measured-but-unhelpful optimizations configurable but disabled by default.
Use end-to-end stage timing as the final decision point.
Re-test disabled optimizations after the dataset becomes larger.
Separate fast developer feedback runs from full correctness checkpoint runs.
```

## Final Interview Story

A concise way to explain this optimization work:

```text
The pipeline originally took around 58 seconds for a 50k-row local messy dataset.
I profiled it stage by stage and found that the bottleneck was not row volume,
but Spark overhead: repeated sessions, excessive shuffle partitions, repeated
actions, unnecessary duplicate checks, repeated dimension-context joins, and
over-caching experiments.

I moved to one Spark session, tuned local shuffle partitions, broadcasted small
reference tables, reduced quality-count actions, persisted only reusable parent
DataFrames, removed event duplicate checks from Quality because Silver owns
deduplication, rebuilt Silver context joins only once, and added configurable
Bronze and Gold audit modes to control count actions.

The final audit-enabled benchmark reduced the runtime to 28.095 seconds while
preserving Bronze, Quality, Quarantine, SCD2, Silver, and Gold behavior. The
audit-reduced benchmark reached 27.920 seconds by skipping Gold output counts.
When I also skipped Bronze input counts, Bronze itself improved from 5.603
seconds to 4.808 seconds, but total runtime did not improve because Quality
varied higher in that run. I also added Bronze output partition planning from
raw CSV file size metadata. That made the design more production-portable, even
though forcing two output partitions was slightly slower for this tiny local
dataset. I then added Quality audit mode and affected-table validation, so
Quality can skip nonessential count actions in benchmark mode and validate only
changed tables while still checking required parent dependencies. Finally, I
propagated the affected-table context into Silver and Gold, so an incremental
`network_events` batch writes only `network_events_enriched` and the Gold KPI
tables that depend on network events for the affected dates.
I also added selective Quality write persistence for network_events, calls, and
data_sessions so valid and quarantine writes reuse the classified validation
result instead of recomputing it.
Then I added a Bronze tiny-file rule so source files under 1 MB write one output
partition, while larger event files still use auto partition planning.
I also tested persisting tower_daily_kpis for Gold region KPIs. It improved the
region KPI task but slowed the full Gold stage, so I kept the option
configurable and disabled it by default.
I tested Silver audit skipping and output partition planning too. Both were
slower on the local 50k dataset, so I kept the features available but disabled
them for the local fast profile.
```

## Current Next Optimization Candidates

The latest top slow tasks are:

| Task | Runtime |
| --- | ---: |
| `bronze.regions` | 2.467 sec |
| `quality.network_events` | 1.983 sec |
| `quality.data_sessions` | 1.406 sec |
| `silver.network_events_enriched` | 1.318 sec |
| `quality.calls` | 1.312 sec |

The next useful optimization should probably focus on:

```text
1. Bronze small-table overhead and small-file writes.
2. Further Quality write-path optimization.
3. Stage-level variance and repeated benchmark averaging.
```
