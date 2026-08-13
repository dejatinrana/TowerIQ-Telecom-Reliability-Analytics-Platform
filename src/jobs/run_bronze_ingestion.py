"""Command-line entry point for Bronze ingestion."""

from __future__ import annotations

import argparse

from src.ingestion.bronze_ingestion import ingest_all_tables
from src.utils.config import load_config
from src.utils.spark import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw TowerIQ CSV tables into Bronze Parquet.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to local YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to ingest.")
    args = parser.parse_args()

    config = load_config(args.config)
    spark_config = config["spark"]
    paths = config["paths"]

    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-BronzeIngestion-{args.profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
    )

    try:
        results = ingest_all_tables(
            spark=spark,
            raw_base_path=paths["raw"],
            bronze_base_path=paths["bronze"],
            profile=args.profile,
        )

        print("Bronze ingestion completed")
        for result in results:
            status = "OK" if result.raw_count == result.bronze_count else "MISMATCH"
            print(
                f"{result.table_name}: raw={result.raw_count}, "
                f"bronze={result.bronze_count}, status={status}, output={result.output_path}"
            )

        mismatches = [result for result in results if result.raw_count != result.bronze_count]
        if mismatches:
            raise RuntimeError(f"Bronze row-count mismatch for {[r.table_name for r in mismatches]}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
