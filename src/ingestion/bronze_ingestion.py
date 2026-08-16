"""Bronze ingestion for raw TowerIQ source files."""

from __future__ import annotations

import time
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
    raw_count: int | None
    bronze_count: int | None
    output_path: str
    runtime_seconds: float
    audit_count_enabled: bool


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


def add_bronze_metadata(df: DataFrame, table_name: str, profile: str, batch_id: str) -> DataFrame:
    """Add standard Bronze tracking metadata to a raw DataFrame."""
    return (
        df.withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_bronze_table", F.lit(table_name))
        .withColumn("_dataset_profile", F.lit(profile))
        .withColumn("_pipeline_batch_id", F.lit(batch_id))
        .withColumn("_source_format", F.lit("csv"))
    )


def write_bronze_table(df: DataFrame, bronze_base_path: str, profile: str, table_name: str) -> str:
    """Write one Bronze table as Parquet and return the output path."""
    output_path = build_storage_path(bronze_base_path, profile, table_name)
    df.write.mode("append").partitionBy("_pipeline_batch_id").parquet(output_path)
    return output_path


def ingest_table(
    spark: SparkSession,
    raw_base_path: str,
    bronze_base_path: str,
    profile: str,
    table_name: str,
    batch_id: str | None = None,
    count_inputs: bool = True,
) -> BronzeIngestionResult:
    """Ingest one raw CSV table to Bronze Parquet."""
    started = time.perf_counter()
    resolved_batch_id = batch_id or f"{profile}_manual"
    raw_df = read_raw_table(spark, raw_base_path, profile, table_name)
    bronze_df = add_bronze_metadata(raw_df, table_name, profile, resolved_batch_id)
    raw_count = raw_df.count() if count_inputs else None
    output_path = write_bronze_table(bronze_df, bronze_base_path, profile, table_name)

    return BronzeIngestionResult(
        table_name=table_name,
        raw_count=raw_count,
        bronze_count=raw_count,
        output_path=output_path,
        runtime_seconds=round(time.perf_counter() - started, 3),
        audit_count_enabled=count_inputs,
    )


def ingest_all_tables(
    spark: SparkSession,
    raw_base_path: str,
    bronze_base_path: str,
    profile: str,
    table_names: list[str] | None = None,
    batch_id: str | None = None,
    count_inputs: bool = True,
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
            batch_id=batch_id,
            count_inputs=count_inputs,
        )
        for table_name in selected_tables
    ]
