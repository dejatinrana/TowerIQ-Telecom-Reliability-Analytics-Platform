"""Run data quality checks for all first-version TowerIQ Bronze tables."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from src.ingestion.bronze_ingestion import build_storage_path
from src.ingestion.schemas import RAW_SCHEMAS
from src.quality.table_validators import TableQualityResult, validate_all_tables
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


def write_quality_result(result: TableQualityResult, paths: dict[str, str], profile: str) -> TableQualityJobResult:
    """Write one table's valid and invalid records."""
    silver_output_path = build_storage_path(paths["silver"], profile, result.table_name)
    quarantine_output_path = build_storage_path(paths["quarantine"], profile, result.table_name)

    result.valid.write.mode("overwrite").parquet(silver_output_path)
    result.invalid.write.mode("overwrite").parquet(quarantine_output_path)

    bronze_count = result.valid.sparkSession.read.parquet(
        build_storage_path(paths["bronze"], profile, result.table_name)
    ).count()
    valid_count = result.valid.sparkSession.read.parquet(silver_output_path).count()
    invalid_count = result.invalid.sparkSession.read.parquet(quarantine_output_path).count()

    if bronze_count != valid_count + invalid_count:
        raise RuntimeError(
            f"{result.table_name} quality count mismatch: "
            f"bronze={bronze_count}, valid={valid_count}, invalid={invalid_count}"
        )

    return TableQualityJobResult(
        table_name=result.table_name,
        bronze_count=bronze_count,
        valid_count=valid_count,
        invalid_count=invalid_count,
        silver_output_path=silver_output_path,
        quarantine_output_path=quarantine_output_path,
    )


def run_quality_checks(config_path: str, profile: str) -> list[TableQualityJobResult]:
    """Validate all configured Bronze tables and write Silver/Quarantine outputs."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]

    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-QualityChecks-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
    )

    try:
        bronze_tables = {
            table_name: spark.read.parquet(build_storage_path(paths["bronze"], profile, table_name))
            for table_name in RAW_SCHEMAS
        }
        quality_results = validate_all_tables(bronze_tables)
        return [write_quality_result(result, paths, profile) for result in quality_results]
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate all first-version TowerIQ Bronze tables.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to validate.")
    args = parser.parse_args()

    results = run_quality_checks(config_path=args.config, profile=args.profile)
    print("Data quality checks completed")
    for result in results:
        print(
            f"{result.table_name}: bronze={result.bronze_count}, valid={result.valid_count}, "
            f"invalid={result.invalid_count}, silver={result.silver_output_path}, "
            f"quarantine={result.quarantine_output_path}"
        )


if __name__ == "__main__":
    main()
