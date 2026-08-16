"""Run data quality checks for Bronze network events."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from src.ingestion.bronze_ingestion import build_storage_path
from src.quality.network_events import validate_network_events
from src.utils.config import load_config
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class NetworkEventsQualityJobResult:
    """Counts produced by the network events quality job."""

    bronze_count: int
    valid_count: int
    invalid_count: int
    silver_output_path: str
    quarantine_output_path: str


def run_network_events_quality(config_path: str, profile: str) -> NetworkEventsQualityJobResult:
    """Validate Bronze network events and write Silver/Quarantine outputs."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]

    spark = create_spark_session(
        app_name=f"{spark_config['app_name']}-NetworkEventsQuality-{profile}",
        master=spark_config["master"],
        aqe_enabled=bool(spark_config["adaptive_query_execution"]),
        use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
        shuffle_partitions=spark_config.get("shuffle_partitions"),
    )

    try:
        network_events_path = build_storage_path(paths["bronze"], profile, "network_events")
        towers_path = build_storage_path(paths["bronze"], profile, "towers")
        subscribers_path = build_storage_path(paths["bronze"], profile, "subscribers")
        devices_path = build_storage_path(paths["bronze"], profile, "devices")

        network_events = spark.read.parquet(network_events_path)
        towers = spark.read.parquet(towers_path)
        subscribers = spark.read.parquet(subscribers_path)
        devices = spark.read.parquet(devices_path)

        result = validate_network_events(
            network_events=network_events,
            towers=towers,
            subscribers=subscribers,
            devices=devices,
        )

        silver_output_path = build_storage_path(paths["silver"], profile, "network_events")
        quarantine_output_path = build_storage_path(paths["quarantine"], profile, "network_events")

        result.valid.write.mode("overwrite").parquet(silver_output_path)
        result.invalid.write.mode("overwrite").parquet(quarantine_output_path)

        bronze_count = network_events.count()
        valid_count = spark.read.parquet(silver_output_path).count()
        invalid_count = spark.read.parquet(quarantine_output_path).count()

        if bronze_count != valid_count + invalid_count:
            raise RuntimeError(
                "Network events quality count mismatch: "
                f"bronze={bronze_count}, valid={valid_count}, invalid={invalid_count}"
            )

        return NetworkEventsQualityJobResult(
            bronze_count=bronze_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            silver_output_path=silver_output_path,
            quarantine_output_path=quarantine_output_path,
        )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Bronze network events.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to validate.")
    args = parser.parse_args()

    result = run_network_events_quality(config_path=args.config, profile=args.profile)
    print("Network events quality completed")
    print(f"bronze={result.bronze_count}")
    print(f"valid={result.valid_count}, output={result.silver_output_path}")
    print(f"invalid={result.invalid_count}, output={result.quarantine_output_path}")


if __name__ == "__main__":
    main()
