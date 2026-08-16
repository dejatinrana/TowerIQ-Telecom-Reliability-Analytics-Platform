"""Run the TowerIQ local incremental batch pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.incremental.batch_registry import BatchRegistry
from src.incremental.pipeline_summary import summarize_results
from src.jobs.run_bronze_ingestion import run_bronze_ingestion
from src.jobs.run_gold_kpis import run_gold_kpis
from src.jobs.run_quality_checks import run_quality_checks
from src.jobs.run_scd2_dimensions import run_scd2_dimensions
from src.jobs.run_silver_transformations import run_silver_transformations
from src.utils.config import load_config
from src.utils.spark import create_spark_session


def parse_affected_tables(value: str | None) -> list[str] | None:
    """Parse a comma-separated affected Quality table list."""
    if value is None or value.strip() == "":
        return None
    return [table.strip() for table in value.split(",") if table.strip()]


def default_registry_path(config: dict) -> str:
    """Read the configured registry path or use the local default."""
    return str(config.get("incremental", {}).get("registry_path", "data/registry/batch_registry.json"))


def run_incremental_pipeline(
    config_path: str,
    profile: str,
    batch_id: str,
    force_rerun: bool = False,
    affected_quality_tables: list[str] | None = None,
) -> dict:
    """Run Bronze through Gold once for a new batch, tracked by the registry."""
    config = load_config(config_path)
    registry = BatchRegistry(default_registry_path(config))

    if registry.has_completed_batch(profile=profile, batch_id=batch_id) and not force_rerun:
        return {
            "status": "skipped",
            "profile": profile,
            "batch_id": batch_id,
            "message": "Batch already completed. Use --force-rerun to run it again.",
        }

    run_record = registry.start_run(profile=profile, batch_id=batch_id)
    stages = {}
    spark_config = config["spark"]
    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-IncrementalPipeline-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
        shuffle_partitions=spark_config.get("shuffle_partitions"),
    )

    try:
        bronze_results = run_bronze_ingestion(config_path=config_path, profile=profile, batch_id=batch_id, spark=spark)
        stages["bronze"] = summarize_results(bronze_results)

        quality_results = run_quality_checks(
            config_path=config_path,
            profile=profile,
            batch_id=batch_id,
            spark=spark,
            table_names=affected_quality_tables,
        )
        stages["quality"] = summarize_results(quality_results)

        scd2_results = run_scd2_dimensions(config_path=config_path, profile=profile, spark=spark)
        stages["scd2"] = summarize_results(scd2_results)

        silver_results = run_silver_transformations(
            config_path=config_path,
            profile=profile,
            batch_id=batch_id,
            spark=spark,
            source_table_names=affected_quality_tables,
        )
        stages["silver"] = summarize_results(silver_results)

        affected_enriched_tables = [result.table_name for result in silver_results] if affected_quality_tables else None
        gold_results = run_gold_kpis(
            config_path=config_path,
            profile=profile,
            batch_id=batch_id,
            spark=spark,
            enriched_table_names=affected_enriched_tables,
        )
        stages["gold"] = summarize_results(gold_results)

        completed = registry.complete_run(run_id=run_record.run_id, stages=stages)
        return {
            "status": completed.status,
            "run_id": completed.run_id,
            "profile": profile,
            "batch_id": batch_id,
            "registry_path": str(Path(default_registry_path(config)).resolve()),
            "stages": stages,
        }
    except Exception as error:
        failed = registry.fail_run(run_id=run_record.run_id, error_message=str(error), stages=stages)
        return {
            "status": failed.status,
            "run_id": failed.run_id,
            "profile": profile,
            "batch_id": batch_id,
            "registry_path": str(Path(default_registry_path(config)).resolve()),
            "error_message": failed.error_message,
            "stages": stages,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TowerIQ incremental batch pipeline.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to process.")
    parser.add_argument("--batch-id", required=True, help="Business batch ID for registry tracking.")
    parser.add_argument("--force-rerun", action="store_true", help="Run even if the batch already completed.")
    parser.add_argument(
        "--affected-quality-tables",
        default=None,
        help="Optional comma-separated Quality tables to validate/write for this batch.",
    )
    args = parser.parse_args()

    result = run_incremental_pipeline(
        config_path=args.config,
        profile=args.profile,
        batch_id=args.batch_id,
        force_rerun=args.force_rerun,
        affected_quality_tables=parse_affected_tables(args.affected_quality_tables),
    )
    print(f"Incremental pipeline status: {result['status']}")
    print(f"profile={result['profile']}, batch_id={result['batch_id']}")
    if "run_id" in result:
        print(f"run_id={result['run_id']}")
    if "registry_path" in result:
        print(f"registry={result['registry_path']}")
    if result["status"] == "failed":
        raise RuntimeError(result["error_message"])
    if result["status"] == "skipped":
        print(result["message"])


if __name__ == "__main__":
    main()
