# 2026-08-16 Developer Fast Workflow

## Why This Was Added

The TowerIQ pipeline is intentionally production-style:

```text
Raw -> Bronze -> Quality/Quarantine -> SCD2 -> Silver -> Gold
```

That is good for learning real data engineering patterns, but it is not always
ideal while editing code on a small local dataset.

For small data, Spark spends a lot of time on fixed overhead:

```text
session setup
query planning
task scheduling
Parquet write setup
commit handling
small-file management
```

So the best local optimization is not always another Spark transformation
change. The better development workflow is to avoid rerunning unnecessary
stages.

## What Was Added

Added a developer-focused config:

```text
configs/dev_fast.yaml
```

Added a stage-specific runner:

```text
src/jobs/run_dev_stage.py
```

This runner can execute only the stage being tested:

```text
bronze
quality
scd2
silver
gold
all
```

## Why This Helps

If only Silver code changed, we do not need to regenerate Raw, Bronze, and
Quality every time.

If only Gold KPI logic changed, we can reuse existing Silver enriched outputs
and rerun only Gold.

That gives faster feedback during development while keeping the full pipeline
available for checkpoint validation.

## Fast Defaults

The developer config uses:

```yaml
audit:
  count_bronze_inputs: false
  count_quality_outputs: false
  count_silver_outputs: true
  count_gold_outputs: false
```

It also uses:

```yaml
dev_workflow:
  default_profile: tiny_messy
  default_batch_id: DEV_FAST_BATCH
  default_affected_tables:
    - network_events
```

This means the default fast workflow focuses on the most important event table
for development feedback.

## Example Commands

Run only Bronze:

```bash
python3 -m src.jobs.run_dev_stage --stage bronze
```

Run only Quality for the default affected table:

```bash
python3 -m src.jobs.run_dev_stage --stage quality
```

Run only Silver for selected source tables:

```bash
python3 -m src.jobs.run_dev_stage --stage silver --affected-tables network_events,calls
```

Run only Gold from existing Silver outputs:

```bash
python3 -m src.jobs.run_dev_stage --stage gold
```

Run a fast affected-table flow:

```bash
python3 -m src.jobs.run_dev_stage --stage all --affected-tables network_events
```

Clean only the requested stage output before running:

```bash
python3 -m src.jobs.run_dev_stage --stage gold --clean-stage-output
```

## Important Rule

This does not replace the real checkpoint run.

Use the fast runner while editing. After a few changes, run the full pipeline
with the normal local config:

```bash
python3 -m src.jobs.run_incremental_pipeline \
  --config configs/local.yaml \
  --profile tiny_messy \
  --batch-id CHECKPOINT_BATCH \
  --force-rerun
```

## Benchmark Result

Measured with:

```text
profile=tiny_messy
batch_id=DEV_FAST_FULL_20260816_001
affected table=network_events
config=configs/dev_fast.yaml
```

| Run | Wall Time | Task Runtime Detail | Purpose |
| --- | ---: | --- | --- |
| Full affected dev flow | 20.454 sec | Bronze 5.421 sec, Quality 3.381 sec, SCD2 3.061 sec, Silver 1.441 sec, Gold 2.877 sec | Check the full affected-table flow. |
| Silver only | 5.437 sec | Silver 3.066 sec | Test Silver changes without rerunning upstream layers. |
| Gold only | 7.448 sec | Gold 3.927 sec | Test Gold changes from existing Silver outputs. |

Silver-only plus Gold-only took:

```text
5.437 + 7.448 = 12.885 sec
```

Compared with the full affected dev flow:

```text
20.454 - 12.885 = 7.569 sec faster
```

That is about 37% faster for a common local development loop.

The improvement comes from avoiding unnecessary stage reruns. Gold-only still
takes several seconds because it starts Spark as a separate command, and Spark
startup/planning overhead is visible on small data.

## Gold-Only Affected Mapping Fix

During the first Gold-only test, the runner did not apply affected-table mapping
when Silver was not executed in the same command.

The problem was:

```text
Gold-only command received affected source table: network_events
Gold needs affected enriched table: network_events_enriched
Silver was not running, so the runner had no Silver result list to pass forward
```

The fix was to map affected source tables directly before calling Gold:

```text
network_events -> network_events_enriched
calls -> calls_enriched
data_sessions -> data_sessions_enriched
tower_alarms -> tower_alarms_enriched
```

Now this command correctly runs only the Gold KPI tables affected by
`network_events`:

```bash
python3 -m src.jobs.run_dev_stage \
  --stage gold \
  --affected-tables network_events \
  --batch-id DEV_FAST_FULL_20260816_001
```

This is an important incremental-processing lesson: every standalone stage must
receive or derive enough context to know what changed upstream.

## Interview Explanation

```text
For local development, I separated fast feedback runs from full correctness
runs. Instead of rerunning the full Spark pipeline after every small change, I
created a dev-fast profile and a stage-specific runner. This lets me rerun only
Bronze, Quality, Silver, or Gold depending on the code I changed. The production
style pipeline still exists for checkpoint validation, but the developer loop is
faster and more practical.

While testing Gold-only, I found that affected-table context was not being
mapped when Silver was skipped. I fixed the runner so source tables such as
network_events are translated to enriched Silver tables such as
network_events_enriched before Gold runs. That keeps standalone stage reruns
consistent with the full incremental pipeline.
```
