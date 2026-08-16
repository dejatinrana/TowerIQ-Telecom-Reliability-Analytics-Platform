# SCD2 Change Dataset

## Problem

The SCD Type 2 tables existed, but the first runs only had one version of each
dimension row. That proved the structure, but not historical versioning.

## Change Implemented

- Added `data_generator/scenarios/scd2_changes.py`.
- Added `data_generator/generate_scd2_change_dataset.py`.
- Updated SCD2 valid-from logic to use batch-date metadata when a dimension does
  not have a natural effective-date column.
- Generated and processed a dedicated `tiny_scd2_demo` profile.

## Test Flow

Baseline batch:

```text
BATCH_TINY_SCD2_DEMO_20260801_001
```

Changed-dimension batch:

```text
BATCH_TINY_SCD2_DEMO_20260815_002
```

## Changed Records

| Dimension | Natural Key | Change |
| --- | --- | --- |
| `regions` | `REG_001` | Region name and zone changed. |
| `service_plans` | `PLAN_PREPAID_BASIC` | Plan name, data limit, and price changed. |
| `towers` | `TOWER_0001` | Capacity score and tower status changed. |
| `subscribers` | `SUB_000001` | Plan and segment changed. |
| `devices` | `DEV_000001` | Model and status changed. |

## Evidence

Each changed key now has two versions:

```text
old version     -> _scd_valid_to = 2026-08-14, _scd_is_current = false
current version -> _scd_valid_from = 2026-08-15, _scd_is_current = true
```

Example:

```text
PLAN_PREPAID_BASIC | Prepaid Basic         | 199 | 2026-08-01 | 2026-08-14 | false
PLAN_PREPAID_BASIC | Prepaid Basic Refresh | 249 | 2026-08-15 | 9999-12-31 | true
```

## Why It Matters

SCD Type 2 prevents dimension changes from rewriting history. A fact event can
later be joined to the dimension version that was valid when the event happened.

## Unit Tests

Latest unit test result:

```text
23 passed
```

## Interview Version

I created a second dimension-change batch to prove SCD Type 2 behavior. The
pipeline now preserves old and current versions for changed regions, service
plans, towers, subscribers, and devices using valid-from, valid-to, current flag,
and record hash metadata.

## GitHub Checkpoint

Suggested commit message:

```text
Add SCD2 change dataset and prove dimension history
```

Commit status: not committed yet.

Push status: not pushed yet.
