"""Command-line entry point for Bronze ingestion."""

from __future__ import annotations

import argparse

from src.ingestion.bronze_ingestion import ingest_all_tables
from src.ingestion.bronze_ingestion import BronzeIngestionResult
from src.utils.config import load_config
from src.utils.spark import create_spark_session


def run_bronze_ingestion(
    config_path: str,
    profile: str,
    batch_id: str | None = None,
    spark=None,
) -> list[BronzeIngestionResult]:
    """Run Bronze ingestion for one dataset profile."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]
    audit_config = config.get("audit", {})
    bronze_config = config.get("bronze", {})
    count_bronze_inputs = bool(audit_config.get("count_bronze_inputs", True))

    should_stop_spark = spark is None
    if spark is None:
        spark = create_spark_session(
            app_name=f"{spark_config['app_name']}-BronzeIngestion-{profile}",
            master=spark_config["master"],
            aqe_enabled=bool(spark_config["adaptive_query_execution"]),
            use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
            shuffle_partitions=spark_config.get("shuffle_partitions"),
        )

    try:
        results = ingest_all_tables(
            spark=spark,
            raw_base_path=paths["raw"],
            bronze_base_path=paths["bronze"],
            profile=profile,
            batch_id=batch_id,
            count_inputs=count_bronze_inputs,
            partition_config=bronze_config.get("output_partitioning"),
        )
    finally:
        if should_stop_spark:
            spark.stop()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw TowerIQ CSV tables into Bronze Parquet.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to local YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to ingest.")
    parser.add_argument("--batch-id", default=None, help="Optional pipeline batch ID for partitioned Bronze writes.")
    args = parser.parse_args()

    results = run_bronze_ingestion(config_path=args.config, profile=args.profile, batch_id=args.batch_id)

    print("Bronze ingestion completed")
    for result in results:
        status = "OK" if result.raw_count == result.bronze_count else "MISMATCH"
        if not result.audit_count_enabled:
            status = "COUNT_SKIPPED"
        print(
            f"{result.table_name}: raw={result.raw_count}, "
            f"bronze={result.bronze_count}, status={status}, output={result.output_path}, "
            f"audit_count={result.audit_count_enabled}, "
            f"source_size_bytes={result.source_size_bytes}, "
            f"planned_output_partitions={result.planned_output_partitions}, "
            f"runtime={result.runtime_seconds:.3f}s"
        )

    mismatches = [
        result
        for result in results
        if result.audit_count_enabled and result.raw_count != result.bronze_count
    ]
    if mismatches:
        raise RuntimeError(f"Bronze row-count mismatch for {[r.table_name for r in mismatches]}")


if __name__ == "__main__":
    main()
