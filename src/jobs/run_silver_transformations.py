"""Run enriched Silver transformations."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from pyspark.sql import functions as F

from src.ingestion.bronze_ingestion import build_storage_path
from src.ingestion.schemas import RAW_SCHEMAS
from src.transformations.silver_controls import SILVER_EVENT_SPECS, apply_silver_event_controls
from src.transformations.silver_events import SilverEnrichmentResult, build_enriched_event_tables
from src.utils.config import load_config
from src.utils.partitioning import apply_output_partition_plan
from src.utils.partitioning import get_storage_path_size_bytes
from src.utils.partitioning import plan_output_partitions
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class SilverTransformationJobResult:
    """Counts and output path for one enriched Silver table."""

    table_name: str
    input_count: int | None
    deduplicated_input_count: int | None
    output_count: int | None
    duplicates_removed: int | None
    late_arriving_count: int | None
    output_path: str
    control_runtime_seconds: float
    write_runtime_seconds: float
    runtime_seconds: float
    audit_count_enabled: bool
    source_size_bytes: int | None
    planned_output_partitions: int | None


SOURCE_TABLE_BY_ENRICHED_TABLE = {
    "network_events_enriched": "network_events",
    "calls_enriched": "calls",
    "data_sessions_enriched": "data_sessions",
    "tower_alarms_enriched": "tower_alarms",
}

DIMENSION_TABLES = ["regions", "towers", "service_plans", "subscribers", "devices"]
EVENT_SOURCE_TABLES = set(SOURCE_TABLE_BY_ENRICHED_TABLE.values())
ENRICHED_TABLE_BY_SOURCE_TABLE = {source: enriched for enriched, source in SOURCE_TABLE_BY_ENRICHED_TABLE.items()}


def parse_table_names(value: str | None) -> list[str] | None:
    """Parse a comma-separated table list from CLI/config input."""
    if value is None or value.strip() == "":
        return None
    return [table.strip() for table in value.split(",") if table.strip()]


def affected_enriched_tables(source_table_names: list[str] | None) -> list[str]:
    """Map affected valid Silver source tables to enriched Silver outputs."""
    if source_table_names is None:
        return list(SOURCE_TABLE_BY_ENRICHED_TABLE)
    unknown = sorted(set(source_table_names) - set(RAW_SCHEMAS))
    if unknown:
        raise ValueError(f"Unknown Silver source table names: {unknown}")
    if set(source_table_names) & set(DIMENSION_TABLES):
        return list(SOURCE_TABLE_BY_ENRICHED_TABLE)
    selected = [
        ENRICHED_TABLE_BY_SOURCE_TABLE[table_name]
        for table_name in source_table_names
        if table_name in EVENT_SOURCE_TABLES
    ]
    return [table_name for table_name in SOURCE_TABLE_BY_ENRICHED_TABLE if table_name in set(selected)]


def read_dimension_table(spark, paths: dict[str, str], profile: str, table_name: str):
    """Read SCD2 dimension when available, otherwise fall back to valid Silver."""
    scd2_path = build_storage_path(paths["silver"], profile, "scd2", table_name)
    try:
        return spark.read.parquet(scd2_path)
    except Exception:
        return spark.read.parquet(build_storage_path(paths["silver"], profile, table_name))


def write_enriched_table(
    result: SilverEnrichmentResult,
    paths: dict[str, str],
    profile: str,
    source_count: int | None,
    deduplicated_source_count: int | None,
    duplicates_removed: int | None,
    late_arriving_count: int | None,
    control_runtime_seconds: float,
    count_outputs: bool = True,
    partition_config: dict | None = None,
    source_size_bytes: int | None = None,
) -> SilverTransformationJobResult:
    """Write one enriched Silver table and verify controlled row counts."""
    started = time.perf_counter()
    output_path = build_storage_path(paths["silver"], profile, "enriched", result.table_name)
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
    output_df = apply_output_partition_plan(result.dataframe, partition_plan.planned_partitions)
    output_df.write.mode("append").partitionBy("event_date", "_pipeline_batch_id").parquet(output_path)
    write_runtime_seconds = round(time.perf_counter() - started, 3)

    return SilverTransformationJobResult(
        table_name=result.table_name,
        input_count=source_count,
        deduplicated_input_count=deduplicated_source_count,
        output_count=deduplicated_source_count,
        duplicates_removed=duplicates_removed,
        late_arriving_count=late_arriving_count,
        output_path=output_path,
        control_runtime_seconds=control_runtime_seconds,
        write_runtime_seconds=write_runtime_seconds,
        runtime_seconds=round(control_runtime_seconds + write_runtime_seconds, 3),
        audit_count_enabled=count_outputs,
        source_size_bytes=source_size_bytes,
        planned_output_partitions=partition_plan.planned_partitions,
    )


def build_controlled_sources(
    silver_tables: dict[str, object],
    enriched_table_names: list[str] | None = None,
    count_outputs: bool = True,
) -> tuple[dict[str, object], dict[str, dict[str, int]]]:
    """Build controlled event sources and calculate their audit metrics."""
    controlled_tables = {}
    metrics = {}
    selected_tables = enriched_table_names or list(SOURCE_TABLE_BY_ENRICHED_TABLE)
    for enriched_table in selected_tables:
        source_table = SOURCE_TABLE_BY_ENRICHED_TABLE[enriched_table]
        started = time.perf_counter()
        source_df = silver_tables[source_table]
        controlled_df = apply_silver_event_controls(source_df, SILVER_EVENT_SPECS[enriched_table])
        input_count = None
        deduplicated_count = None
        duplicates_removed = None
        late_arriving_count = None
        if count_outputs:
            input_count = source_df.count()
            controlled_metrics = controlled_df.agg(
                F.count(F.lit(1)).alias("deduplicated_count"),
                F.sum(F.col("_is_late_arriving").cast("int")).alias("late_arriving_count"),
            ).first()
            deduplicated_count = controlled_metrics["deduplicated_count"]
            late_arriving_count = controlled_metrics["late_arriving_count"] or 0
            duplicates_removed = input_count - deduplicated_count
        controlled_tables[enriched_table] = controlled_df
        metrics[enriched_table] = {
            "input_count": input_count,
            "deduplicated_input_count": deduplicated_count,
            "duplicates_removed": duplicates_removed,
            "late_arriving_count": late_arriving_count,
            "control_runtime_seconds": round(time.perf_counter() - started, 3),
        }
    return controlled_tables, metrics


def run_silver_transformations(
    config_path: str,
    profile: str,
    batch_id: str | None = None,
    spark=None,
    source_table_names: list[str] | None = None,
) -> list[SilverTransformationJobResult]:
    """Build enriched Silver event tables from valid Silver tables."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    silver_config = config.get("silver", {})
    audit_config = config.get("audit", {})
    count_silver_outputs = bool(audit_config.get("count_silver_outputs", True))
    partition_config = silver_config.get("output_partitioning")
    selected_enriched_tables = affected_enriched_tables(source_table_names)

    should_stop_spark = spark is None
    if spark is None:
        spark = create_spark_session(
            app_name=f"{spark_config['app_name']}-SilverTransformations-{profile}",
            master=spark_config["master"],
            aqe_enabled=bool(spark_config["adaptive_query_execution"]),
            use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
            shuffle_partitions=spark_config.get("shuffle_partitions"),
        )

    try:
        required_source_tables = set(DIMENSION_TABLES) | {
            SOURCE_TABLE_BY_ENRICHED_TABLE[table_name] for table_name in selected_enriched_tables
        }
        def silver_table_path(table_name: str) -> str:
            if batch_id is None:
                return build_storage_path(paths["silver"], profile, table_name)
            return build_storage_path(paths["silver"], profile, table_name, f"_pipeline_batch_id={batch_id}")

        silver_tables = {
            table_name: (
                spark.read.parquet(build_storage_path(paths["silver"], profile, table_name))
                if batch_id is None
                else spark.read.parquet(build_storage_path(paths["silver"], profile, table_name)).filter(
                    f"_pipeline_batch_id = '{batch_id}'"
                )
            )
            for table_name in required_source_tables
        }
        source_sizes_by_enriched_table = {
            enriched_table: get_storage_path_size_bytes(
                spark,
                silver_table_path(SOURCE_TABLE_BY_ENRICHED_TABLE[enriched_table]),
            )
            for enriched_table in selected_enriched_tables
        }
        for table_name in DIMENSION_TABLES:
            silver_tables[table_name] = read_dimension_table(spark, paths, profile, table_name)
        controlled_tables, source_metrics = build_controlled_sources(
            silver_tables,
            selected_enriched_tables,
            count_outputs=count_silver_outputs,
        )
        enriched_tables = build_enriched_event_tables(
            silver_tables,
            controlled_tables,
            enriched_table_names=selected_enriched_tables,
        )
        return [
            write_enriched_table(
                result=result,
                paths=paths,
                profile=profile,
                source_count=source_metrics[result.table_name]["input_count"],
                deduplicated_source_count=source_metrics[result.table_name]["deduplicated_input_count"],
                duplicates_removed=source_metrics[result.table_name]["duplicates_removed"],
                late_arriving_count=source_metrics[result.table_name]["late_arriving_count"],
                control_runtime_seconds=source_metrics[result.table_name]["control_runtime_seconds"],
                count_outputs=count_silver_outputs,
                partition_config=partition_config,
                source_size_bytes=source_sizes_by_enriched_table.get(result.table_name),
            )
            for result in enriched_tables
        ]
    finally:
        if should_stop_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build enriched Silver event tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline batch ID to transform.")
    parser.add_argument("--source-tables", default=None, help="Optional comma-separated valid Silver source tables.")
    args = parser.parse_args()

    results = run_silver_transformations(
        config_path=args.config,
        profile=args.profile,
        batch_id=args.batch_id,
        source_table_names=parse_table_names(args.source_tables),
    )
    print("Silver transformations completed")
    for result in results:
        print(
            f"{result.table_name}: input={result.input_count}, "
            f"deduplicated_input={result.deduplicated_input_count}, "
            f"duplicates_removed={result.duplicates_removed}, "
            f"late_arriving={result.late_arriving_count}, output={result.output_count}, "
            f"path={result.output_path}, audit_count={result.audit_count_enabled}, "
            f"planned_output_partitions={result.planned_output_partitions}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
