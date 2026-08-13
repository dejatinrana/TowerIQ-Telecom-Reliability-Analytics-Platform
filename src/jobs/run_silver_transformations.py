"""Run enriched Silver transformations."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from src.ingestion.bronze_ingestion import build_storage_path
from src.ingestion.schemas import RAW_SCHEMAS
from src.transformations.silver_events import SilverEnrichmentResult, build_enriched_event_tables
from src.utils.config import load_config
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class SilverTransformationJobResult:
    """Counts and output path for one enriched Silver table."""

    table_name: str
    input_count: int
    output_count: int
    output_path: str


SOURCE_TABLE_BY_ENRICHED_TABLE = {
    "network_events_enriched": "network_events",
    "calls_enriched": "calls",
    "data_sessions_enriched": "data_sessions",
    "tower_alarms_enriched": "tower_alarms",
}


def write_enriched_table(
    result: SilverEnrichmentResult,
    paths: dict[str, str],
    profile: str,
    source_count: int,
) -> SilverTransformationJobResult:
    """Write one enriched Silver table and verify row-count preservation."""
    output_path = build_storage_path(paths["silver"], profile, "enriched", result.table_name)
    result.dataframe.write.mode("overwrite").parquet(output_path)
    output_count = result.dataframe.sparkSession.read.parquet(output_path).count()

    if output_count != source_count:
        raise RuntimeError(
            f"{result.table_name} row-count mismatch: input={source_count}, output={output_count}"
        )

    return SilverTransformationJobResult(
        table_name=result.table_name,
        input_count=source_count,
        output_count=output_count,
        output_path=output_path,
    )


def run_silver_transformations(config_path: str, profile: str) -> list[SilverTransformationJobResult]:
    """Build enriched Silver event tables from valid Silver tables."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]

    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-SilverTransformations-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
    )

    try:
        silver_tables = {
            table_name: spark.read.parquet(build_storage_path(paths["silver"], profile, table_name))
            for table_name in RAW_SCHEMAS
        }
        source_counts = {
            table_name: silver_tables[table_name].count()
            for table_name in SOURCE_TABLE_BY_ENRICHED_TABLE.values()
        }
        enriched_tables = build_enriched_event_tables(silver_tables)
        return [
            write_enriched_table(
                result=result,
                paths=paths,
                profile=profile,
                source_count=source_counts[SOURCE_TABLE_BY_ENRICHED_TABLE[result.table_name]],
            )
            for result in enriched_tables
        ]
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build enriched Silver event tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    args = parser.parse_args()

    results = run_silver_transformations(config_path=args.config, profile=args.profile)
    print("Silver transformations completed")
    for result in results:
        print(
            f"{result.table_name}: input={result.input_count}, output={result.output_count}, "
            f"path={result.output_path}"
        )


if __name__ == "__main__":
    main()
