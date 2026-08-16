"""Experiment 003: reduce unnecessary Spark actions in the local pipeline."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.incremental.batch_registry import BatchRegistry
from src.incremental.pipeline_summary import summarize_results
from src.jobs.run_bronze_ingestion import run_bronze_ingestion
from src.jobs.run_gold_kpis import run_gold_kpis
from src.jobs.run_quality_checks import run_quality_checks
from src.jobs.run_scd2_dimensions import run_scd2_dimensions
from src.jobs.run_silver_transformations import run_silver_transformations
from src.utils.config import load_config
from src.utils.spark import create_spark_session


EXPERIMENT_ID = "EXP-003"
EXPERIMENT_NAME = "Reduce Spark Actions"
PREVIOUS_BEST_SECONDS = 53.884


@dataclass(frozen=True)
class TimedStage:
    """Runtime and result summary for one pipeline stage."""

    stage_name: str
    runtime_seconds: float
    results: list[dict]


def utc_timestamp() -> str:
    """Return a stable UTC timestamp string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_registry_path(config: dict) -> str:
    """Read the configured registry path or use the local default."""
    return str(config.get("incremental", {}).get("registry_path", "data/registry/batch_registry.json"))


def time_stage(stage_name: str, action: Callable[[], list]) -> TimedStage:
    """Run one stage and capture elapsed time plus summarized results."""
    started = time.perf_counter()
    results = action()
    runtime_seconds = round(time.perf_counter() - started, 3)
    return TimedStage(stage_name=stage_name, runtime_seconds=runtime_seconds, results=summarize_results(results))


def run_reduce_spark_actions_experiment(config_path: str, profile: str, batch_id: str, output_dir: str) -> dict:
    """Run EXP-003 after removing redundant Spark actions."""
    config = load_config(config_path)
    registry = BatchRegistry(default_registry_path(config))

    if registry.has_completed_batch(profile=profile, batch_id=batch_id):
        raise RuntimeError(f"Batch already completed: profile={profile}, batch_id={batch_id}")

    run_record = registry.start_run(profile=profile, batch_id=batch_id)
    spark_config = config["spark"]
    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-EXP003-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
        shuffle_partitions=spark_config.get("shuffle_partitions"),
    )
    stages: list[TimedStage] = []

    try:
        stages.append(
            time_stage("bronze", lambda: run_bronze_ingestion(config_path, profile, batch_id=batch_id, spark=spark))
        )
        stages.append(
            time_stage("quality", lambda: run_quality_checks(config_path, profile, batch_id=batch_id, spark=spark))
        )
        stages.append(time_stage("scd2", lambda: run_scd2_dimensions(config_path, profile, spark=spark)))
        stages.append(
            time_stage(
                "silver",
                lambda: run_silver_transformations(config_path, profile, batch_id=batch_id, spark=spark),
            )
        )
        stages.append(time_stage("gold", lambda: run_gold_kpis(config_path, profile, batch_id=batch_id, spark=spark)))

        registry.complete_run(
            run_id=run_record.run_id,
            stages={stage.stage_name: stage.results for stage in stages},
        )
    except Exception as error:
        registry.fail_run(run_id=run_record.run_id, error_message=str(error))
        raise
    finally:
        spark.stop()

    total_runtime = round(sum(stage.runtime_seconds for stage in stages), 3)
    improvement_seconds = round(PREVIOUS_BEST_SECONDS - total_runtime, 3)
    improvement_percent = round((improvement_seconds / PREVIOUS_BEST_SECONDS) * 100, 2)
    report = {
        "experiment_id": EXPERIMENT_ID,
        "experiment_name": EXPERIMENT_NAME,
        "created_at": utc_timestamp(),
        "profile": profile,
        "batch_id": batch_id,
        "run_id": run_record.run_id,
        "config_path": config_path,
        "previous_best_runtime_seconds": PREVIOUS_BEST_SECONDS,
        "total_runtime_seconds": total_runtime,
        "improvement_seconds": improvement_seconds,
        "improvement_percent": improvement_percent,
        "optimization": "remove_redundant_spark_actions",
        "stages": [
            {
                "stage_name": stage.stage_name,
                "runtime_seconds": stage.runtime_seconds,
                "results": stage.results,
            }
            for stage in stages
        ],
    }
    output_path = write_experiment_outputs(report, output_dir)
    report["output_path"] = str(output_path)
    return report


def write_experiment_outputs(report: dict, output_dir: str) -> Path:
    """Write JSON and markdown outputs for EXP-003."""
    results_dir = Path(output_dir, "results")
    results_dir.mkdir(parents=True, exist_ok=True)
    json_path = results_dir / f"exp_003_reduce_spark_actions_{report['profile']}_{report['batch_id']}.json"
    md_path = Path(output_dir, "EXP-003-reduce-spark-actions.md")

    with json_path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, sort_keys=True)
        file.write("\n")

    with md_path.open("w", encoding="utf-8") as file:
        file.write(render_markdown_report(report, json_path))

    return json_path


def render_markdown_report(report: dict, json_path: Path) -> str:
    """Render the EXP-003 report."""
    stage_rows = "\n".join(
        f"| `{stage['stage_name']}` | {stage['runtime_seconds']:.3f} |"
        for stage in report["stages"]
    )
    return f"""# EXP-003 Reduce Spark Actions

## Problem

EXP-002 reused one Spark session, but the pipeline still spent time on
redundant actions such as counting data after writes and running separate
counts for related audit metrics.

## Solution

Remove unnecessary read-back counts, calculate related metrics in fewer
aggregations, and reuse controlled Silver DataFrames for enrichment.

## Dataset

```text
profile={report['profile']}
batch_id={report['batch_id']}
```

## Runtime Comparison

```text
EXP-002 previous best: {report['previous_best_runtime_seconds']:.3f} seconds
EXP-003 optimized: {report['total_runtime_seconds']:.3f} seconds
improvement: {report['improvement_seconds']:.3f} seconds ({report['improvement_percent']:.2f}%)
```

## Stage Runtime

| Stage | Runtime Seconds |
| --- | ---: |
{stage_rows}

## Evidence

Detailed JSON result:

```text
{json_path}
```
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EXP-003 Spark action reduction experiment.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny_messy", help="Dataset profile to benchmark.")
    parser.add_argument("--batch-id", required=True, help="Unique batch ID for this experiment run.")
    parser.add_argument("--output-dir", default="experiments", help="Directory for experiment outputs.")
    args = parser.parse_args()

    report = run_reduce_spark_actions_experiment(args.config, args.profile, args.batch_id, args.output_dir)
    print(f"{report['experiment_id']} completed")
    print(f"profile={report['profile']}, batch_id={report['batch_id']}")
    print(f"total_runtime_seconds={report['total_runtime_seconds']:.3f}")
    print(f"improvement_percent={report['improvement_percent']:.2f}")
    print(f"output={report['output_path']}")


if __name__ == "__main__":
    main()
