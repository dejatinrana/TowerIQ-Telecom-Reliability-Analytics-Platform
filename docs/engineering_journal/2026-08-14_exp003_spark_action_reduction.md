# 2026-08-14 EXP-003 Spark Action Reduction

## What Changed

- Removed the Bronze read-after-write count. Bronze now reports the raw count as
  the written count because ingestion is append-only and no row-level filtering
  happens in Bronze.
- Changed SCD2 count reporting so total/current counts are calculated before the
  write, instead of writing, reading the table back, and counting twice.
- Changed Silver metric calculation so deduplicated count and late-arriving
  count are calculated in one aggregation.
- Removed the Silver enriched-output count after write. The output count is
  now the deduplicated source count because the enrichment joins are expected to
  preserve event row counts.

## Result

The clean EXP-003 run used:

```text
profile=tiny_messy_exp003_nocache
batch_id=BATCH_EXP003_NOCACHE_20260814_001
```

Runtime:

```text
EXP-002 previous best: 53.884 seconds
EXP-003 action reduction: 68.487 seconds
change: 14.603 seconds slower
```

Stage timings:

| Stage | Runtime Seconds |
| --- | ---: |
| Bronze | 8.181 |
| Quality | 39.456 |
| SCD2 | 5.215 |
| Silver | 9.764 |
| Gold | 5.871 |

## Learning

Removing unnecessary actions is still good engineering, but it did not solve
the main runtime problem. Quality validation is now the largest bottleneck, and
Spark still uses 200 shuffle partitions for small local data, which creates too
much scheduling overhead.

## Next Step

EXP-004 should focus on local Spark execution tuning and quality validation:

- configure a smaller local shuffle partition count;
- broadcast small reference dimensions;
- reduce repeated validation count work;
- keep audit metrics, but calculate them in cheaper grouped aggregations.
