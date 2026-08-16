# Spark Performance Experiments

Each experiment should define a telecom problem, hypothesis, workload, baseline,
physical plan evidence, metrics, optimization, rerun, trade-offs, and journal link.

## Current Experiments

| Experiment | Purpose |
| --- | --- |
| `EXP-001` | Measure baseline pipeline runtime before dataset scaling. |
| `EXP-002` | Reduce runtime overhead by reusing one Spark session across the pipeline. |
| `EXP-003` | Remove redundant Spark actions and measure whether action reduction improves runtime. |
| `EXP-004` | Tune local Spark shuffle partitions to reduce small shuffle job overhead. |
