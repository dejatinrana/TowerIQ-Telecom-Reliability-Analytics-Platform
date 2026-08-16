"""Run Gold KPI transformations."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F

from src.ingestion.bronze_ingestion import build_storage_path
from src.transformations.gold_kpis import GoldKpiResult, build_selected_gold_kpis
from src.utils.config import load_config
from src.utils.spark import create_spark_session


ENRICHED_SILVER_TABLES = [
    "network_events_enriched",
    "calls_enriched",
    "data_sessions_enriched",
    "tower_alarms_enriched",
]

KPI_DEPENDENCIES = {
    "tower_daily_kpis": {
        "network_events_enriched",
        "calls_enriched",
        "data_sessions_enriched",
        "tower_alarms_enriched",
    },
    "region_daily_kpis": {
        "network_events_enriched",
        "calls_enriched",
        "data_sessions_enriched",
        "tower_alarms_enriched",
    },
    "network_type_daily_kpis": {"network_events_enriched", "data_sessions_enriched"},
    "subscriber_segment_daily_kpis": {"calls_enriched", "data_sessions_enriched"},
}


def parse_table_names(value: str | None) -> list[str] | None:
    """Parse a comma-separated table list from CLI/config input."""
    if value is None or value.strip() == "":
        return None
    return [table.strip() for table in value.split(",") if table.strip()]


def affected_gold_tables(enriched_table_names: list[str] | None) -> list[str]:
    """Return Gold KPI tables affected by changed enriched Silver tables."""
    if enriched_table_names is None:
        return list(KPI_DEPENDENCIES)
    unknown = sorted(set(enriched_table_names) - set(ENRICHED_SILVER_TABLES))
    if unknown:
        raise ValueError(f"Unknown enriched Silver table names: {unknown}")
    changed = set(enriched_table_names)
    return [
        kpi_table
        for kpi_table, dependencies in KPI_DEPENDENCIES.items()
        if dependencies & changed
    ]


@dataclass(frozen=True)
class GoldKpiJobResult:
    """Counts and output path for one Gold KPI table."""

    table_name: str
    output_count: int | None
    output_path: str
    runtime_seconds: float
    audit_count_enabled: bool
    persisted_shared_dependency: bool


def write_gold_table(
    result: GoldKpiResult,
    gold_base_path: str,
    profile: str,
    count_outputs: bool = True,
    persisted_shared_dependency: bool = False,
) -> GoldKpiJobResult:
    """Write one Gold KPI table by replacing only affected event_date partitions."""
    started = time.perf_counter()
    output_path = build_storage_path(gold_base_path, profile, result.table_name)
    output_count = None
    if count_outputs:
        output_count = result.dataframe.count()
        if output_count == 0:
            raise RuntimeError(f"{result.table_name} produced zero rows")
    result.dataframe.write.mode("overwrite").partitionBy("event_date").parquet(output_path)
    return GoldKpiJobResult(
        table_name=result.table_name,
        output_count=output_count,
        output_path=output_path,
        runtime_seconds=round(time.perf_counter() - started, 3),
        audit_count_enabled=count_outputs,
        persisted_shared_dependency=persisted_shared_dependency,
    )


def should_persist_tower_daily(selected_gold_tables: list[str], gold_config: dict) -> bool:
    """Return true when tower daily KPIs are reused by tower and region writes."""
    return bool(gold_config.get("persist_tower_daily_for_region", False)) and {
        "tower_daily_kpis",
        "region_daily_kpis",
    }.issubset(set(selected_gold_tables))


def affected_event_dates(
    enriched_tables: dict[str, object],
    batch_id: str | None,
    enriched_table_names: list[str] | None = None,
) -> list:
    """Return event dates affected by the current batch."""
    if batch_id is None:
        return []

    selected_tables = enriched_table_names or list(enriched_tables)
    date_frames = [
        table.filter(F.col("_pipeline_batch_id") == batch_id).select("event_date").distinct()
        for table_name, table in enriched_tables.items()
        if table_name in selected_tables
    ]
    if not date_frames:
        return []
    affected = date_frames[0]
    for date_frame in date_frames[1:]:
        affected = affected.unionByName(date_frame)
    return [row["event_date"] for row in affected.distinct().collect()]


def run_gold_kpis(
    config_path: str,
    profile: str,
    batch_id: str | None = None,
    spark=None,
    enriched_table_names: list[str] | None = None,
) -> list[GoldKpiJobResult]:
    """Build and write Gold KPI tables from enriched Silver tables."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    audit_config = config.get("audit", {})
    gold_config = config.get("gold", {})
    count_gold_outputs = bool(audit_config.get("count_gold_outputs", True))
    selected_gold_tables = affected_gold_tables(enriched_table_names)
    persist_tower_daily = should_persist_tower_daily(selected_gold_tables, gold_config)

    should_stop_spark = spark is None
    if spark is None:
        spark = create_spark_session(
            app_name=f"{spark_config['app_name']}-GoldKpis-{profile}",
            master=spark_config["master"],
            aqe_enabled=bool(spark_config["adaptive_query_execution"]),
            use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
            shuffle_partitions=spark_config.get("shuffle_partitions"),
        )

    try:
        enriched_tables = {
            table_name: spark.read.parquet(build_storage_path(paths["silver"], profile, "enriched", table_name))
            for table_name in ENRICHED_SILVER_TABLES
        }
        dates = affected_event_dates(enriched_tables, batch_id, enriched_table_names=enriched_table_names)
        if batch_id is not None and enriched_table_names is not None and not dates:
            return []
        if dates:
            enriched_tables = {
                table_name: table.filter(F.col("event_date").isin(dates))
                for table_name, table in enriched_tables.items()
            }
        gold_results = build_selected_gold_kpis(enriched_tables, selected_gold_tables)
        tower_daily_result = next(
            (result for result in gold_results if result.table_name == "tower_daily_kpis"),
            None,
        )
        if persist_tower_daily and tower_daily_result is not None:
            tower_daily_result.dataframe.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            return [
                write_gold_table(
                    result=result,
                    gold_base_path=paths["gold"],
                    profile=profile,
                    count_outputs=count_gold_outputs,
                    persisted_shared_dependency=(
                        persist_tower_daily
                        and result.table_name in {"tower_daily_kpis", "region_daily_kpis"}
                    ),
                )
                for result in gold_results
            ]
        finally:
            if persist_tower_daily and tower_daily_result is not None:
                tower_daily_result.dataframe.unpersist()
    finally:
        if should_stop_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TowerIQ Gold KPI tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline batch ID for affected-date recomputation.")
    parser.add_argument("--enriched-tables", default=None, help="Optional comma-separated affected enriched Silver tables.")
    args = parser.parse_args()

    results = run_gold_kpis(
        config_path=args.config,
        profile=args.profile,
        batch_id=args.batch_id,
        enriched_table_names=parse_table_names(args.enriched_tables),
    )
    print("Gold KPI transformations completed")
    for result in results:
        output_count = result.output_count if result.audit_count_enabled else "skipped"
        print(
            f"{result.table_name}: rows={output_count}, path={result.output_path}, "
            f"audit_count={result.audit_count_enabled}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
