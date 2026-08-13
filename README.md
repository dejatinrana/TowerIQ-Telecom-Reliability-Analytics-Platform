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

## Run Data Quality Checks

```bash
python3 -m src.jobs.run_quality_checks --config configs/local.yaml --profile tiny
```

## Run Silver Transformations

```bash
python3 -m src.jobs.run_silver_transformations --config configs/local.yaml --profile tiny
```

## Run Gold KPI Transformations

```bash
python3 -m src.jobs.run_gold_kpis --config configs/local.yaml --profile tiny
```

## Current Status

Current completed layers for the tiny profile:

- Synthetic raw telecom dataset generated.
- Bronze ingestion implemented with explicit schemas.
- Data quality and quarantine implemented for all first-version tables.
- First valid-record Silver layer created.
- Enriched Silver event tables created for analytics.
- First Gold KPI tables created.

Next planned layer:

- Additional Gold KPI refinement and baseline analytics review.

Useful project docs:

- [Architecture](docs/architecture.md)
- [Data Model](docs/data_model.md)
- [Gold KPI Question Bank](docs/gold_kpi_question_bank.md)
