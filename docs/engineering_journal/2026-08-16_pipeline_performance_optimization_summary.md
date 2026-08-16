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
varied higher in that run.
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
