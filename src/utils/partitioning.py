"""Helpers for planning Spark output partitions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class OutputPartitionPlan:
    """Decision for how many files Spark should target for an output write."""

    strategy: str
    source_size_bytes: int | None
    target_file_size_mb: int
    planned_partitions: int | None
    tiny_file_threshold_mb: int | None = None
    tiny_file_partitions: int | None = None


def get_storage_path_size_bytes(spark: SparkSession, path: str) -> int | None:
    """Return file size for local or Hadoop-compatible storage paths."""
    parsed = urlparse(path)
    if not parsed.scheme:
        local_path = Path(path)
        if local_path.is_file():
            return local_path.stat().st_size
        if local_path.is_dir():
            return sum(file.stat().st_size for file in local_path.rglob("*") if file.is_file())
        return None

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        jvm = spark._jvm
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(hadoop_path):
            return None
        status = fs.getFileStatus(hadoop_path)
        if status.isFile():
            return int(status.getLen())
        iterator = fs.listFiles(hadoop_path, True)
        total_size = 0
        while iterator.hasNext():
            total_size += int(iterator.next().getLen())
        return total_size
    except Exception:
        return None


def plan_output_partitions(
    source_size_bytes: int | None,
    strategy: str = "auto",
    target_file_size_mb: int = 128,
    min_partitions: int = 1,
    max_partitions: int = 64,
    tiny_file_threshold_mb: int | None = None,
    tiny_file_partitions: int | None = None,
) -> OutputPartitionPlan:
    """Plan output partitions from source file size metadata."""
    normalized_strategy = strategy.lower()
    if normalized_strategy == "none":
        return OutputPartitionPlan(
            strategy=normalized_strategy,
            source_size_bytes=source_size_bytes,
            target_file_size_mb=target_file_size_mb,
            planned_partitions=None,
            tiny_file_threshold_mb=tiny_file_threshold_mb,
            tiny_file_partitions=tiny_file_partitions,
        )
    if normalized_strategy != "auto":
        raise ValueError(f"Unsupported partition strategy: {strategy}")
    if target_file_size_mb <= 0:
        raise ValueError("target_file_size_mb must be greater than zero")
    if min_partitions <= 0:
        raise ValueError("min_partitions must be greater than zero")
    if max_partitions < min_partitions:
        raise ValueError("max_partitions must be greater than or equal to min_partitions")
    if tiny_file_threshold_mb is not None and tiny_file_threshold_mb < 0:
        raise ValueError("tiny_file_threshold_mb must be greater than or equal to zero")
    if tiny_file_partitions is not None and tiny_file_partitions <= 0:
        raise ValueError("tiny_file_partitions must be greater than zero")
    if source_size_bytes is None:
        return OutputPartitionPlan(
            strategy=normalized_strategy,
            source_size_bytes=source_size_bytes,
            target_file_size_mb=target_file_size_mb,
            planned_partitions=None,
            tiny_file_threshold_mb=tiny_file_threshold_mb,
            tiny_file_partitions=tiny_file_partitions,
        )

    if tiny_file_threshold_mb is not None and tiny_file_partitions is not None:
        tiny_threshold_bytes = tiny_file_threshold_mb * 1024 * 1024
        if source_size_bytes <= tiny_threshold_bytes:
            planned_partitions = min(max(tiny_file_partitions, 1), max_partitions)
            return OutputPartitionPlan(
                strategy=normalized_strategy,
                source_size_bytes=source_size_bytes,
                target_file_size_mb=target_file_size_mb,
                planned_partitions=planned_partitions,
                tiny_file_threshold_mb=tiny_file_threshold_mb,
                tiny_file_partitions=tiny_file_partitions,
            )

    target_size_bytes = target_file_size_mb * 1024 * 1024
    estimated_partitions = math.ceil(source_size_bytes / target_size_bytes)
    planned_partitions = min(max(estimated_partitions, min_partitions), max_partitions)
    return OutputPartitionPlan(
        strategy=normalized_strategy,
        source_size_bytes=source_size_bytes,
        target_file_size_mb=target_file_size_mb,
        planned_partitions=planned_partitions,
        tiny_file_threshold_mb=tiny_file_threshold_mb,
        tiny_file_partitions=tiny_file_partitions,
    )


def apply_output_partition_plan(df: DataFrame, planned_partitions: int | None) -> DataFrame:
    """Apply an output partition plan without triggering a Spark action."""
    if planned_partitions is None:
        return df

    current_partitions = df.rdd.getNumPartitions()
    if current_partitions > planned_partitions:
        return df.coalesce(planned_partitions)
    if current_partitions < planned_partitions:
        return df.repartition(planned_partitions)
    return df
