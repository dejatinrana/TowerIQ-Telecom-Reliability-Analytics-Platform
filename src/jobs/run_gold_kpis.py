"""Run Gold KPI transformations."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from src.ingestion.bronze_ingestion import build_storage_path
from src.transformations.gold_kpis import GoldKpiResult, build_gold_kpis
from src.utils.config import load_config
from src.utils.spark import create_spark_session
from pyspark.sql import functions as F


ENRICHED_SILVER_TABLES = [
    "network_events_enriched",
    "calls_enriched",
    "data_sessions_enriched",
    "tower_alarms_enriched",
]


@dataclass(frozen=True)
class GoldKpiJobResult:
    """Counts and output path for one Gold KPI table."""

    table_name: str
    output_count: int | None
    output_path: str
    runtime_seconds: float
    audit_count_enabled: bool


def write_gold_table(
    result: GoldKpiResult,
    gold_base_path: str,
    profile: str,
    count_outputs: bool = True,
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
    )


def affected_event_dates(enriched_tables: dict[str, object], batch_id: str | None) -> list:
    """Return event dates affected by the current batch."""
    if batch_id is None:
        return []

    date_frames = [
        table.filter(F.col("_pipeline_batch_id") == batch_id).select("event_date").distinct()
        for table in enriched_tables.values()
    ]
    affected = date_frames[0]
    for date_frame in date_frames[1:]:
        affected = affected.unionByName(date_frame)
    return [row["event_date"] for row in affected.distinct().collect()]


def run_gold_kpis(
    config_path: str,
    profile: str,
    batch_id: str | None = None,
    spark=None,
) -> list[GoldKpiJobResult]:
    """Build and write Gold KPI tables from enriched Silver tables."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    audit_config = config.get("audit", {})
    count_gold_outputs = bool(audit_config.get("count_gold_outputs", True))

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
        dates = affected_event_dates(enriched_tables, batch_id)
        if dates:
            enriched_tables = {
                table_name: table.filter(F.col("event_date").isin(dates))
                for table_name, table in enriched_tables.items()
            }
        gold_results = build_gold_kpis(enriched_tables)
        return [
            write_gold_table(
                result=result,
                gold_base_path=paths["gold"],
                profile=profile,
                count_outputs=count_gold_outputs,
            )
            for result in gold_results
        ]
    finally:
        if should_stop_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TowerIQ Gold KPI tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline batch ID for affected-date recomputation.")
    args = parser.parse_args()

    results = run_gold_kpis(config_path=args.config, profile=args.profile, batch_id=args.batch_id)
    print("Gold KPI transformations completed")
    for result in results:
        output_count = result.output_count if result.audit_count_enabled else "skipped"
        print(
            f"{result.table_name}: rows={output_count}, path={result.output_path}, "
            f"audit_count={result.audit_count_enabled}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
