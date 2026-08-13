# TowerIQ

TowerIQ is a telecom network reliability analytics platform that processes raw
network events into trusted operational KPIs for tower health, congestion,
dropped-call rates, outage analysis, and regional network performance.

Telecom networks generate high-volume, time-sensitive, and messy event data from
towers, subscribers, devices, calls, data sessions, and alarms. Operations teams
need reliable analytics to detect unhealthy towers, service degradation, delayed
events, duplicate batches, and regional outages before they affect customers.

This project uses that business problem as the foundation for a production-style
PySpark data platform. The telecom domain is a strong fit because it naturally
creates real data engineering challenges such as dirty records, late-arriving
events, duplicate processing, incremental pipelines, idempotency, data skew,
large joins, partitioning trade-offs, and Spark performance tuning.

The repository will be built progressively. The first goal is not to implement
every feature at once, but to create a strong foundation for measured engineering
work.

## Business Goal

Convert raw telecom network activity into trusted reliability analytics that help
answer questions such as:

- Which towers are unhealthy or overloaded?
- Which regions are experiencing congestion or outages?
- Where are dropped calls or failed sessions increasing?
- How much traffic is moving across 4G, 5G, and other network technologies?
- Can the data platform safely handle bad records, duplicate batches, and late
  events?

## Initial Direction

- Generate reproducible synthetic telecom data.
- Build Bronze, Silver, Gold pipeline layers.
- Validate and quarantine bad records.
- Study Spark behavior through execution plans, Spark UI, and benchmarks.
- Document decisions, failures, measurements, and interview stories.

## Generate Tiny Raw Dataset

```bash
python3 data_generator/generate_tiny_dataset.py --output-dir data/raw/tiny
```

## Run Bronze Ingestion

```bash
python3 -m src.jobs.run_bronze_ingestion --config configs/local.yaml --profile tiny
```

## Local Setup

Use a project virtual environment before running jobs:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

See [Local Setup](docs/setup.md) for details about avoiding local Spark runtime
mismatches.

## Current Status

Scaffold only. Real implementation starts with Phase 0: Project Foundation and Learning Contract.
