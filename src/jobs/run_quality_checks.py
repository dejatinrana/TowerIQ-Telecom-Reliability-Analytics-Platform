"""Run data quality checks for all first-version TowerIQ Bronze tables."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from pyspark.storagelevel import StorageLevel

from src.ingestion.bronze_ingestion import build_storage_path
from src.ingestion.schemas import RAW_SCHEMAS
from src.quality.table_validators import (
    TableQualityResult,
    persist_valid_parent,
    unpersist_valid_parents,
    validate_calls,
    validate_data_sessions,
    validate_devices,
    validate_network_events,
    validate_regions,
    validate_service_plans,
    validate_subscribers,
    validate_tower_alarms,
    validate_towers,
)
from src.utils.config import load_config
from src.utils.partitioning import apply_output_partition_plan
from src.utils.partitioning import get_storage_path_size_bytes
from src.utils.partitioning import plan_output_partitions
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class TableQualityJobResult:
    """Counts produced by one table quality check."""

    table_name: str
    bronze_count: int | None
    valid_count: int | None
    invalid_count: int | None
    silver_output_path: str
    quarantine_output_path: str
    validation_runtime_seconds: float
    write_runtime_seconds: float
    runtime_seconds: float
    audit_count_enabled: bool
    source_size_bytes: int | None
    planned_output_partitions: int | None
    persisted_for_write: bool


TABLE_ORDER = [
    "regions",
    "service_plans",
    "towers",
    "subscribers",
    "devices",
    "network_events",
    "calls",
    "data_sessions",
    "tower_alarms",
]

QUALITY_DEPENDENCIES = {
    "regions": set(),
    "service_plans": set(),
    "towers": {"regions"},
    "subscribers": {"regions", "service_plans"},
    "devices": {"subscribers"},
    "network_events": {"towers", "subscribers", "devices"},
    "calls": {"towers", "subscribers", "devices"},
    "data_sessions": {"towers", "subscribers", "devices"},
    "tower_alarms": {"towers"},
}

REUSABLE_PARENT_TABLES = {"regions", "service_plans", "towers", "subscribers", "devices"}


def parse_table_names(value: str | None) -> list[str] | None:
    """Parse a comma-separated table list from CLI/config input."""
    if value is None or value.strip() == "":
        return None
    return [table.strip() for table in value.split(",") if table.strip()]


def required_quality_tables(write_tables: set[str]) -> set[str]:
    """Return selected tables plus parent dependencies needed for validation."""
    required = set(write_tables)
    pending = list(write_tables)
    while pending:
        table_name = pending.pop()
        for dependency in QUALITY_DEPENDENCIES[table_name]:
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return required


def validate_table_names(table_names: list[str] | None) -> list[str]:
    """Validate requested table names and return them in pipeline order."""
    if table_names is None:
        return TABLE_ORDER
    unknown = sorted(set(table_names) - set(TABLE_ORDER))
    if unknown:
        raise ValueError(f"Unknown Quality table names: {unknown}")
    requested = set(table_names)
    return [table_name for table_name in TABLE_ORDER if table_name in requested]


def write_quality_result(
    result: TableQualityResult,
    paths: dict[str, str],
    profile: str,
    validation_runtime_seconds: float,
    batch_id: str | None = None,
    count_outputs: bool = True,
    partition_config: dict | None = None,
    source_size_bytes: int | None = None,
    persist_for_write: bool = False,
) -> TableQualityJobResult:
    """Write one table's valid and invalid records."""
    started = time.perf_counter()
    silver_output_path = build_storage_path(paths["silver"], profile, result.table_name)
    quarantine_output_path = build_storage_path(paths["quarantine"], profile, result.table_name)
    partition_settings = partition_config or {}
    partition_plan = plan_output_partitions(
        source_size_bytes=source_size_bytes,
        strategy=str(partition_settings.get("strategy", "none")),
        target_file_size_mb=int(partition_settings.get("target_file_size_mb", 128)),
        min_partitions=int(partition_settings.get("min_partitions", 1)),
        max_partitions=int(partition_settings.get("max_partitions", 64)),
        tiny_file_threshold_mb=(
            int(partition_settings["tiny_file_threshold_mb"])
            if "tiny_file_threshold_mb" in partition_settings
            else None
        ),
        tiny_file_partitions=(
            int(partition_settings["tiny_file_partitions"])
            if "tiny_file_partitions" in partition_settings
            else None
        ),
    )

    should_persist_classified = result.classified is not None and (count_outputs or persist_for_write)

    if should_persist_classified:
        result.classified.persist(StorageLevel.MEMORY_AND_DISK)

    if count_outputs and result.classified is not None:
        count_rows = result.classified.groupBy("_is_quality_valid").count().collect()
        count_by_validity = {row["_is_quality_valid"]: row["count"] for row in count_rows}
        valid_count = count_by_validity.get(True, 0)
        invalid_count = count_by_validity.get(False, 0)
    elif count_outputs:
        valid_count = result.valid.count()
        invalid_count = result.invalid.count()
    else:
        valid_count = None
        invalid_count = None
    bronze_count = None if valid_count is None or invalid_count is None else valid_count + invalid_count

    valid_output = apply_output_partition_plan(result.valid, partition_plan.planned_partitions)
    invalid_output = apply_output_partition_plan(result.invalid, partition_plan.planned_partitions)

    try:
        if not count_outputs or valid_count > 0:
            valid_output.write.mode("append").partitionBy("_pipeline_batch_id").parquet(silver_output_path)
        if not count_outputs or invalid_count > 0:
            invalid_output.write.mode("append").partitionBy("_pipeline_batch_id").parquet(quarantine_output_path)
    finally:
        if should_persist_classified:
            result.classified.unpersist()

    return TableQualityJobResult(
        table_name=result.table_name,
        bronze_count=bronze_count,
        valid_count=valid_count,
        invalid_count=invalid_count,
        silver_output_path=silver_output_path,
        quarantine_output_path=quarantine_output_path,
        validation_runtime_seconds=validation_runtime_seconds,
        write_runtime_seconds=round(time.perf_counter() - started, 3),
        runtime_seconds=round(validation_runtime_seconds + (time.perf_counter() - started), 3),
        audit_count_enabled=count_outputs,
        source_size_bytes=source_size_bytes,
        planned_output_partitions=partition_plan.planned_partitions,
        persisted_for_write=should_persist_classified,
    )


def time_validation(action) -> tuple[TableQualityResult, float]:
    """Run one table validator and return its lazy-plan build time."""
    started = time.perf_counter()
    result = action()
    return result, round(time.perf_counter() - started, 3)


def run_quality_checks(
    config_path: str,
    profile: str,
    batch_id: str | None = None,
    spark=None,
    table_names: list[str] | None = None,
) -> list[TableQualityJobResult]:
    """Validate all configured Bronze tables and write Silver/Quarantine outputs."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    quality_config = config.get("quality", {})
    audit_config = config.get("audit", {})
    count_quality_outputs = bool(audit_config.get("count_quality_outputs", True))
    partition_config = quality_config.get("output_partitioning")
    if partition_config is None and quality_config.get("output_partitions") is not None:
        partition_config = {
            "strategy": "auto",
            "target_file_size_mb": 128,
            "min_partitions": int(quality_config["output_partitions"]),
            "max_partitions": int(quality_config["output_partitions"]),
        }
    persist_classified_tables = set(quality_config.get("persist_classified_tables", []))
    selected_write_tables = set(validate_table_names(table_names))
    selected_validation_tables = required_quality_tables(selected_write_tables)

    should_stop_spark = spark is None
    if spark is None:
        spark = create_spark_session(
            app_name=f"{spark_config['app_name']}-QualityChecks-{profile}",
            master=spark_config["master"],
            aqe_enabled=bool(spark_config["adaptive_query_execution"]),
            use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
            shuffle_partitions=spark_config.get("shuffle_partitions"),
        )

    try:
        def bronze_table_path(table_name: str) -> str:
            if batch_id is None:
                return build_storage_path(paths["bronze"], profile, table_name)
            return build_storage_path(paths["bronze"], profile, table_name, f"_pipeline_batch_id={batch_id}")

        bronze_tables = {}
        bronze_source_sizes = {}
        for table_name in selected_validation_tables:
            table_path = build_storage_path(paths["bronze"], profile, table_name)
            bronze_tables[table_name] = (
                spark.read.parquet(table_path)
                if batch_id is None
                else spark.read.parquet(table_path).filter(f"_pipeline_batch_id = '{batch_id}'")
            )
            bronze_source_sizes[table_name] = get_storage_path_size_bytes(spark, bronze_table_path(table_name))

        results: list[TableQualityJobResult] = []
        cached_parent_results: list[TableQualityResult] = []

        def write_result(result: TableQualityResult, runtime: float) -> TableQualityJobResult:
            return write_quality_result(
                result=result,
                paths=paths,
                profile=profile,
                validation_runtime_seconds=runtime,
                batch_id=batch_id,
                count_outputs=count_quality_outputs,
                partition_config=partition_config,
                source_size_bytes=bronze_source_sizes.get(result.table_name),
                persist_for_write=result.table_name in persist_classified_tables,
            )

        validation_results: dict[str, TableQualityResult] = {}

        def keep_parent(table_name: str, result: TableQualityResult) -> TableQualityResult:
            if table_name in REUSABLE_PARENT_TABLES:
                result = persist_valid_parent(result)
                cached_parent_results.append(result)
            return result

        for table_name in TABLE_ORDER:
            if table_name not in selected_validation_tables:
                continue
            if table_name == "regions":
                result, runtime = time_validation(lambda: validate_regions(bronze_tables["regions"]))
            elif table_name == "service_plans":
                result, runtime = time_validation(lambda: validate_service_plans(bronze_tables["service_plans"]))
            elif table_name == "towers":
                result, runtime = time_validation(
                    lambda: validate_towers(bronze_tables["towers"], validation_results["regions"].valid)
                )
            elif table_name == "subscribers":
                result, runtime = time_validation(
                    lambda: validate_subscribers(
                        bronze_tables["subscribers"],
                        validation_results["regions"].valid,
                        validation_results["service_plans"].valid,
                    )
                )
            elif table_name == "devices":
                result, runtime = time_validation(
                    lambda: validate_devices(bronze_tables["devices"], validation_results["subscribers"].valid)
                )
            elif table_name == "network_events":
                result, runtime = time_validation(
                    lambda: validate_network_events(
                        bronze_tables["network_events"],
                        validation_results["towers"].valid,
                        validation_results["subscribers"].valid,
                        validation_results["devices"].valid,
                    )
                )
            elif table_name == "calls":
                result, runtime = time_validation(
                    lambda: validate_calls(
                        bronze_tables["calls"],
                        validation_results["towers"].valid,
                        validation_results["subscribers"].valid,
                        validation_results["devices"].valid,
                    )
                )
            elif table_name == "data_sessions":
                result, runtime = time_validation(
                    lambda: validate_data_sessions(
                        bronze_tables["data_sessions"],
                        validation_results["towers"].valid,
                        validation_results["subscribers"].valid,
                        validation_results["devices"].valid,
                    )
                )
            elif table_name == "tower_alarms":
                result, runtime = time_validation(
                    lambda: validate_tower_alarms(bronze_tables["tower_alarms"], validation_results["towers"].valid)
                )
            else:
                raise ValueError(f"Unsupported Quality table: {table_name}")

            result = keep_parent(table_name, result)
            validation_results[table_name] = result
            if table_name in selected_write_tables:
                results.append(write_result(result, runtime))

        return results
    finally:
        if "cached_parent_results" in locals():
            unpersist_valid_parents(cached_parent_results)
        if should_stop_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate all first-version TowerIQ Bronze tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to validate.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline batch ID to validate.")
    parser.add_argument("--tables", default=None, help="Optional comma-separated list of affected tables to write.")
    args = parser.parse_args()

    results = run_quality_checks(
        config_path=args.config,
        profile=args.profile,
        batch_id=args.batch_id,
        table_names=parse_table_names(args.tables),
    )
    print("Data quality checks completed")
    for result in results:
        print(
            f"{result.table_name}: bronze={result.bronze_count}, valid={result.valid_count}, "
            f"invalid={result.invalid_count}, silver={result.silver_output_path}, "
            f"quarantine={result.quarantine_output_path}, audit_count={result.audit_count_enabled}, "
            f"planned_output_partitions={result.planned_output_partitions}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
