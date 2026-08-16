"""Fast local developer runner for one TowerIQ pipeline stage at a time."""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path
from typing import Any

from src.incremental.pipeline_summary import summarize_results
from src.jobs.run_bronze_ingestion import run_bronze_ingestion
from src.jobs.run_gold_kpis import run_gold_kpis
from src.jobs.run_quality_checks import run_quality_checks
from src.jobs.run_scd2_dimensions import run_scd2_dimensions
from src.jobs.run_silver_transformations import affected_enriched_tables
from src.jobs.run_silver_transformations import run_silver_transformations
from src.utils.config import load_config
from src.utils.spark import create_spark_session


STAGES = ("bronze", "quality", "scd2", "silver", "gold", "all")


def default_from_config(config: dict, key: str, fallback: Any) -> Any:
    """Read a developer workflow default without requiring every config to define it."""
    return config.get("dev_workflow", {}).get(key, fallback)


def parse_tables(value: str | None) -> list[str] | None:
    """Parse a comma-separated table list."""
    if value is None or value.strip() == "":
        return None
    return [table.strip() for table in value.split(",") if table.strip()]


def safe_remove_path(path: str) -> None:
    """Remove a generated project data path, refusing broad or suspicious paths."""
    resolved = Path(path).resolve()
    cwd = Path.cwd().resolve()
    if cwd not in resolved.parents:
        raise ValueError(f"Refusing to remove path outside project: {resolved}")
    if resolved == cwd or len(resolved.parts) < len(cwd.parts) + 2:
        raise ValueError(f"Refusing to remove broad project path: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def clean_stage_output(config: dict, profile: str, stage: str) -> None:
    """Clean only generated output folders for one stage and profile."""
    paths = config["paths"]
    if stage == "bronze":
        safe_remove_path(str(Path(paths["bronze"]) / profile))
    elif stage == "quality":
        for base in (paths["silver"], paths["quarantine"]):
            for table_dir in Path(base).joinpath(profile).glob("*"):
                if table_dir.is_dir() and table_dir.name != "enriched" and table_dir.name != "scd2":
                    safe_remove_path(str(table_dir))
    elif stage == "scd2":
        safe_remove_path(str(Path(paths["silver"]) / profile / "scd2"))
    elif stage == "silver":
        safe_remove_path(str(Path(paths["silver"]) / profile / "enriched"))
    elif stage == "gold":
        safe_remove_path(str(Path(paths["gold"]) / profile))
    elif stage == "all":
        for single_stage in ("bronze", "quality", "scd2", "silver", "gold"):
            clean_stage_output(config, profile, single_stage)


def run_dev_stage(
    config_path: str = "configs/dev_fast.yaml",
    profile: str | None = None,
    stage: str = "all",
    batch_id: str | None = None,
    affected_tables: list[str] | None = None,
    clean_output: bool = False,
) -> dict[str, Any]:
    """Run one stage with fast local defaults and return timing summaries."""
    if stage not in STAGES:
        raise ValueError(f"Unknown stage '{stage}'. Expected one of {STAGES}.")

    config = load_config(config_path)
    resolved_profile = profile or str(default_from_config(config, "default_profile", "tiny_messy"))
    resolved_batch_id = batch_id or str(default_from_config(config, "default_batch_id", "DEV_FAST_BATCH"))
    resolved_affected_tables = affected_tables
    if resolved_affected_tables is None:
        configured_tables = default_from_config(config, "default_affected_tables", None)
        resolved_affected_tables = list(configured_tables) if configured_tables else None

    if clean_output:
        clean_stage_output(config, resolved_profile, stage)

    spark_config = config["spark"]
    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-DevStage-{resolved_profile}-{stage}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
        shuffle_partitions=spark_config.get("shuffle_partitions"),
    )

    started = time.perf_counter()
    stages: dict[str, list[dict[str, Any]]] = {}
    try:
        if stage in ("bronze", "all"):
            stages["bronze"] = summarize_results(
                run_bronze_ingestion(
                    config_path=config_path,
                    profile=resolved_profile,
                    batch_id=resolved_batch_id,
                    spark=spark,
                )
            )

        if stage in ("quality", "all"):
            stages["quality"] = summarize_results(
                run_quality_checks(
                    config_path=config_path,
                    profile=resolved_profile,
                    batch_id=resolved_batch_id,
                    spark=spark,
                    table_names=resolved_affected_tables,
                )
            )

        if stage in ("scd2", "all"):
            stages["scd2"] = summarize_results(
                run_scd2_dimensions(config_path=config_path, profile=resolved_profile, spark=spark)
            )

        if stage in ("silver", "all"):
            stages["silver"] = summarize_results(
                run_silver_transformations(
                    config_path=config_path,
                    profile=resolved_profile,
                    batch_id=resolved_batch_id,
                    spark=spark,
                    source_table_names=resolved_affected_tables,
                )
            )

        if stage in ("gold", "all"):
            enriched_tables = None
            if resolved_affected_tables is not None and "silver" in stages:
                enriched_tables = [result["table_name"] for result in stages["silver"]]
            elif resolved_affected_tables is not None:
                enriched_tables = affected_enriched_tables(resolved_affected_tables)
            stages["gold"] = summarize_results(
                run_gold_kpis(
                    config_path=config_path,
                    profile=resolved_profile,
                    batch_id=resolved_batch_id,
                    spark=spark,
                    enriched_table_names=enriched_tables,
                )
            )
    finally:
        spark.stop()

    return {
        "status": "completed",
        "config_path": config_path,
        "profile": resolved_profile,
        "batch_id": resolved_batch_id,
        "stage": stage,
        "affected_tables": resolved_affected_tables,
        "runtime_seconds": round(time.perf_counter() - started, 3),
        "stages": stages,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fast local TowerIQ developer stages.")
    parser.add_argument("--config", default="configs/dev_fast.yaml", help="Path to developer YAML config.")
    parser.add_argument("--profile", default=None, help="Dataset profile. Defaults to config dev_workflow.default_profile.")
    parser.add_argument("--stage", choices=STAGES, default="all", help="Stage to run.")
    parser.add_argument("--batch-id", default=None, help="Batch ID. Defaults to config dev_workflow.default_batch_id.")
    parser.add_argument("--affected-tables", default=None, help="Comma-separated source tables for fast affected runs.")
    parser.add_argument(
        "--clean-stage-output",
        action="store_true",
        help="Remove generated output for the requested stage/profile before running.",
    )
    args = parser.parse_args()

    result = run_dev_stage(
        config_path=args.config,
        profile=args.profile,
        stage=args.stage,
        batch_id=args.batch_id,
        affected_tables=parse_tables(args.affected_tables),
        clean_output=args.clean_stage_output,
    )
    print(
        f"Dev stage completed: stage={result['stage']}, profile={result['profile']}, "
        f"batch_id={result['batch_id']}, runtime={result['runtime_seconds']:.3f}s"
    )
    if result["affected_tables"]:
        print(f"affected_tables={','.join(result['affected_tables'])}")
    for stage_name, stage_results in result["stages"].items():
        stage_runtime = sum(float(item.get("runtime_seconds", 0) or 0) for item in stage_results)
        print(f"{stage_name}: tasks={len(stage_results)}, task_runtime_sum={stage_runtime:.3f}s")


if __name__ == "__main__":
    main()
