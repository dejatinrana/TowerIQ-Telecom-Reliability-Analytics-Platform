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
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class TableQualityJobResult:
    """Counts produced by one table quality check."""

    table_name: str
    bronze_count: int
    valid_count: int
    invalid_count: int
    silver_output_path: str
    quarantine_output_path: str
    validation_runtime_seconds: float
    write_runtime_seconds: float
    runtime_seconds: float


def write_quality_result(
    result: TableQualityResult,
    paths: dict[str, str],
    profile: str,
    validation_runtime_seconds: float,
    batch_id: str | None = None,
    output_partitions: int | None = None,
) -> TableQualityJobResult:
    """Write one table's valid and invalid records."""
    started = time.perf_counter()
    silver_output_path = build_storage_path(paths["silver"], profile, result.table_name)
    quarantine_output_path = build_storage_path(paths["quarantine"], profile, result.table_name)

    if result.classified is not None:
        result.classified.persist(StorageLevel.MEMORY_AND_DISK)
        count_rows = result.classified.groupBy("_is_quality_valid").count().collect()
        count_by_validity = {row["_is_quality_valid"]: row["count"] for row in count_rows}
        valid_count = count_by_validity.get(True, 0)
        invalid_count = count_by_validity.get(False, 0)
    else:
        valid_count = result.valid.count()
        invalid_count = result.invalid.count()
    bronze_count = valid_count + invalid_count

    valid_output = result.valid.coalesce(output_partitions) if output_partitions else result.valid
    invalid_output = result.invalid.coalesce(output_partitions) if output_partitions else result.invalid

    try:
        if valid_count > 0:
            valid_output.write.mode("append").partitionBy("_pipeline_batch_id").parquet(silver_output_path)
        if invalid_count > 0:
            invalid_output.write.mode("append").partitionBy("_pipeline_batch_id").parquet(quarantine_output_path)
    finally:
        if result.classified is not None:
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
) -> list[TableQualityJobResult]:
    """Validate all configured Bronze tables and write Silver/Quarantine outputs."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    quality_config = config.get("quality", {})
    output_partitions = quality_config.get("output_partitions")

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
        bronze_tables = {
            table_name: (
                spark.read.parquet(build_storage_path(paths["bronze"], profile, table_name))
                if batch_id is None
                else spark.read.parquet(build_storage_path(paths["bronze"], profile, table_name)).filter(
                    f"_pipeline_batch_id = '{batch_id}'"
                )
            )
            for table_name in RAW_SCHEMAS
        }
        results: list[TableQualityJobResult] = []
        cached_parent_results: list[TableQualityResult] = []

        def write_result(result: TableQualityResult, runtime: float) -> TableQualityJobResult:
            return write_quality_result(
                result=result,
                paths=paths,
                profile=profile,
                validation_runtime_seconds=runtime,
                batch_id=batch_id,
                output_partitions=output_partitions,
            )

        regions_result, runtime = time_validation(lambda: validate_regions(bronze_tables["regions"]))
        regions_result = persist_valid_parent(regions_result)
        cached_parent_results.append(regions_result)
        results.append(write_result(regions_result, runtime))

        service_plans_result, runtime = time_validation(lambda: validate_service_plans(bronze_tables["service_plans"]))
        service_plans_result = persist_valid_parent(service_plans_result)
        cached_parent_results.append(service_plans_result)
        results.append(write_result(service_plans_result, runtime))

        towers_result, runtime = time_validation(lambda: validate_towers(bronze_tables["towers"], regions_result.valid))
        towers_result = persist_valid_parent(towers_result)
        cached_parent_results.append(towers_result)
        results.append(write_result(towers_result, runtime))

        subscribers_result, runtime = time_validation(
            lambda: validate_subscribers(
                bronze_tables["subscribers"],
                regions_result.valid,
                service_plans_result.valid,
            )
        )
        subscribers_result = persist_valid_parent(subscribers_result)
        cached_parent_results.append(subscribers_result)
        results.append(write_result(subscribers_result, runtime))

        devices_result, runtime = time_validation(lambda: validate_devices(bronze_tables["devices"], subscribers_result.valid))
        devices_result = persist_valid_parent(devices_result)
        cached_parent_results.append(devices_result)
        results.append(write_result(devices_result, runtime))

        network_events_result, runtime = time_validation(
            lambda: validate_network_events(
                bronze_tables["network_events"],
                towers_result.valid,
                subscribers_result.valid,
                devices_result.valid,
            )
        )
        results.append(write_result(network_events_result, runtime))

        calls_result, runtime = time_validation(
            lambda: validate_calls(
                bronze_tables["calls"],
                towers_result.valid,
                subscribers_result.valid,
                devices_result.valid,
            )
        )
        results.append(write_result(calls_result, runtime))

        data_sessions_result, runtime = time_validation(
            lambda: validate_data_sessions(
                bronze_tables["data_sessions"],
                towers_result.valid,
                subscribers_result.valid,
                devices_result.valid,
            )
        )
        results.append(write_result(data_sessions_result, runtime))

        tower_alarms_result, runtime = time_validation(
            lambda: validate_tower_alarms(bronze_tables["tower_alarms"], towers_result.valid)
        )
        results.append(write_result(tower_alarms_result, runtime))

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
    args = parser.parse_args()

    results = run_quality_checks(config_path=args.config, profile=args.profile, batch_id=args.batch_id)
    print("Data quality checks completed")
    for result in results:
        print(
            f"{result.table_name}: bronze={result.bronze_count}, valid={result.valid_count}, "
            f"invalid={result.invalid_count}, silver={result.silver_output_path}, "
            f"quarantine={result.quarantine_output_path}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
