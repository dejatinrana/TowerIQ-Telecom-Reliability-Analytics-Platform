"""Run Gold KPI transformations."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from src.ingestion.bronze_ingestion import build_storage_path
from src.transformations.gold_kpis import GoldKpiResult, build_gold_kpis
from src.utils.config import load_config
from src.utils.spark import create_spark_session


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
    output_count: int
    output_path: str


def write_gold_table(result: GoldKpiResult, gold_base_path: str, profile: str) -> GoldKpiJobResult:
    """Write one Gold KPI table."""
    output_path = build_storage_path(gold_base_path, profile, result.table_name)
    result.dataframe.write.mode("overwrite").parquet(output_path)
    output_count = result.dataframe.sparkSession.read.parquet(output_path).count()
    if output_count == 0:
        raise RuntimeError(f"{result.table_name} produced zero rows")
    return GoldKpiJobResult(table_name=result.table_name, output_count=output_count, output_path=output_path)


def run_gold_kpis(config_path: str, profile: str) -> list[GoldKpiJobResult]:
    """Build and write Gold KPI tables from enriched Silver tables."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]

    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-GoldKpis-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
    )

    try:
        enriched_tables = {
            table_name: spark.read.parquet(build_storage_path(paths["silver"], profile, "enriched", table_name))
            for table_name in ENRICHED_SILVER_TABLES
        }
        gold_results = build_gold_kpis(enriched_tables)
        return [write_gold_table(result, paths["gold"], profile) for result in gold_results]
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TowerIQ Gold KPI tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    args = parser.parse_args()

    results = run_gold_kpis(config_path=args.config, profile=args.profile)
    print("Gold KPI transformations completed")
    for result in results:
        print(f"{result.table_name}: rows={result.output_count}, path={result.output_path}")


if __name__ == "__main__":
    main()
