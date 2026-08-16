"""Build SCD Type 2 dimension tables."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from pyspark.sql import functions as F

from src.ingestion.bronze_ingestion import build_storage_path
from src.transformations.scd2 import SCD2_DIMENSION_SPECS, build_scd2_dimensions
from src.utils.config import load_config
from src.utils.spark import create_spark_session


@dataclass(frozen=True)
class Scd2DimensionJobResult:
    """Counts and output path for one SCD2 dimension table."""

    table_name: str
    output_count: int
    current_count: int
    output_path: str
    runtime_seconds: float


def write_scd2_dimension(table_name: str, dataframe, silver_base_path: str, profile: str) -> Scd2DimensionJobResult:
    """Write one SCD2 dimension table."""
    started = time.perf_counter()
    output_path = build_storage_path(silver_base_path, profile, "scd2", table_name)
    metrics = dataframe.agg(
        F.count(F.lit(1)).alias("output_count"),
        F.sum(F.col("_scd_is_current").cast("int")).alias("current_count"),
    ).first()
    output_count = metrics["output_count"]
    current_count = metrics["current_count"] or 0
    if output_count == 0:
        raise RuntimeError(f"{table_name} SCD2 dimension produced zero rows")

    dataframe.write.mode("overwrite").parquet(output_path)
    return Scd2DimensionJobResult(
        table_name=table_name,
        output_count=output_count,
        current_count=current_count,
        output_path=output_path,
        runtime_seconds=round(time.perf_counter() - started, 3),
    )


def run_scd2_dimensions(config_path: str, profile: str, spark=None) -> list[Scd2DimensionJobResult]:
    """Build and write SCD2 dimension tables from valid Silver dimensions."""
    config = load_config(config_path)
    spark_config = config["spark"]
    paths = config["paths"]

    should_stop_spark = spark is None
    if spark is None:
        spark = create_spark_session(
            app_name=f"{spark_config['app_name']}-Scd2Dimensions-{profile}",
            master=spark_config["master"],
            aqe_enabled=bool(spark_config["adaptive_query_execution"]),
            use_pyspark_package=bool(spark_config.get("use_pyspark_package", True)),
            shuffle_partitions=spark_config.get("shuffle_partitions"),
        )

    try:
        silver_dimensions = {
            table_name: spark.read.parquet(build_storage_path(paths["silver"], profile, table_name))
            for table_name in SCD2_DIMENSION_SPECS
        }
        scd2_dimensions = build_scd2_dimensions(silver_dimensions)
        return [
            write_scd2_dimension(table_name, dataframe, paths["silver"], profile)
            for table_name, dataframe in scd2_dimensions.items()
        ]
    finally:
        if should_stop_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TowerIQ SCD Type 2 dimensions.")
    parser.add_argument("--config", default="configs/local.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="tiny", help="Dataset profile to transform.")
    args = parser.parse_args()

    results = run_scd2_dimensions(config_path=args.config, profile=args.profile)
    print("SCD2 dimensions completed")
    for result in results:
        print(
            f"{result.table_name}: rows={result.output_count}, current={result.current_count}, "
            f"path={result.output_path}, runtime={result.runtime_seconds:.3f}s"
        )


if __name__ == "__main__":
    main()
