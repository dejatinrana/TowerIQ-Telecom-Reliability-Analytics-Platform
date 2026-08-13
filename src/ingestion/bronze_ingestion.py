"""Bronze ingestion for raw TowerIQ source files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ingestion.schemas import RAW_SCHEMAS


def build_storage_path(base_path: str, *parts: str) -> str:
    """Build a production-portable storage path.

    Local paths are resolved as absolute filesystem paths. URI paths such as
    s3://bucket/path are joined without using pathlib, which would corrupt the
    scheme.
    """
    parsed = urlparse(base_path)
    if parsed.scheme:
        return "/".join([base_path.rstrip("/"), *[part.strip("/") for part in parts]])
    return str(Path(base_path, *parts).resolve())


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Row-count result for one ingested table."""

    table_name: str
    raw_count: int
    bronze_count: int
    output_path: str


def read_raw_table(spark: SparkSession, raw_base_path: str, profile: str, table_name: str) -> DataFrame:
    """Read one raw CSV table with its explicit schema."""
    schema = RAW_SCHEMAS[table_name]
    input_path = build_storage_path(raw_base_path, profile, f"{table_name}.csv")
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(input_path)
    )


def add_bronze_metadata(df: DataFrame, table_name: str, profile: str) -> DataFrame:
    """Add standard Bronze tracking metadata to a raw DataFrame."""
    return (
        df.withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_bronze_table", F.lit(table_name))
        .withColumn("_dataset_profile", F.lit(profile))
        .withColumn("_source_format", F.lit("csv"))
    )


def write_bronze_table(df: DataFrame, bronze_base_path: str, profile: str, table_name: str) -> str:
    """Write one Bronze table as Parquet and return the output path."""
    output_path = build_storage_path(bronze_base_path, profile, table_name)
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def ingest_table(
    spark: SparkSession,
    raw_base_path: str,
    bronze_base_path: str,
    profile: str,
    table_name: str,
) -> BronzeIngestionResult:
    """Ingest one raw CSV table to Bronze Parquet."""
    raw_df = read_raw_table(spark, raw_base_path, profile, table_name)
    raw_count = raw_df.count()

    bronze_df = add_bronze_metadata(raw_df, table_name, profile)
    output_path = write_bronze_table(bronze_df, bronze_base_path, profile, table_name)
    bronze_count = spark.read.parquet(output_path).count()

    return BronzeIngestionResult(
        table_name=table_name,
        raw_count=raw_count,
        bronze_count=bronze_count,
        output_path=output_path,
    )


def ingest_all_tables(
    spark: SparkSession,
    raw_base_path: str,
    bronze_base_path: str,
    profile: str,
    table_names: list[str] | None = None,
) -> list[BronzeIngestionResult]:
    """Ingest all configured raw tables to Bronze Parquet."""
    selected_tables = table_names or list(RAW_SCHEMAS.keys())
    return [
        ingest_table(
            spark=spark,
            raw_base_path=raw_base_path,
            bronze_base_path=bronze_base_path,
            profile=profile,
            table_name=table_name,
        )
        for table_name in selected_tables
    ]
