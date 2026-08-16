"""Reusable hardening controls for Silver event transformations."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass(frozen=True)
class SilverEventSpec:
    """Control metadata for one Silver event table."""

    source_table: str
    enriched_table: str
    primary_key: str
    event_timestamp_column: str
    ingestion_timestamp_column: str = "ingestion_timestamp"
    late_arrival_threshold_hours: int = 24


SILVER_EVENT_SPECS: dict[str, SilverEventSpec] = {
    "network_events_enriched": SilverEventSpec(
        source_table="network_events",
        enriched_table="network_events_enriched",
        primary_key="event_id",
        event_timestamp_column="event_timestamp",
    ),
    "calls_enriched": SilverEventSpec(
        source_table="calls",
        enriched_table="calls_enriched",
        primary_key="call_id",
        event_timestamp_column="call_start_timestamp",
    ),
    "data_sessions_enriched": SilverEventSpec(
        source_table="data_sessions",
        enriched_table="data_sessions_enriched",
        primary_key="session_id",
        event_timestamp_column="session_start_timestamp",
    ),
    "tower_alarms_enriched": SilverEventSpec(
        source_table="tower_alarms",
        enriched_table="tower_alarms_enriched",
        primary_key="alarm_id",
        event_timestamp_column="alarm_timestamp",
    ),
}

SCHEMA_EVOLUTION_OPTIONAL_COLUMNS: dict[str, StringType] = {
    "source_system": StringType(),
    "producer_schema_version": StringType(),
}


def add_missing_optional_columns(df: DataFrame) -> DataFrame:
    """Add known optional columns when older input batches do not contain them."""
    aligned = df
    for column_name, data_type in SCHEMA_EVOLUTION_OPTIONAL_COLUMNS.items():
        if column_name not in aligned.columns:
            aligned = aligned.withColumn(column_name, F.lit(None).cast(data_type))
    return aligned


def deduplicate_by_primary_key(df: DataFrame, spec: SilverEventSpec) -> DataFrame:
    """Keep the latest record for each event primary key."""
    window = Window.partitionBy(spec.primary_key).orderBy(
        F.col(spec.ingestion_timestamp_column).desc_nulls_last(),
        F.col(spec.event_timestamp_column).desc_nulls_last(),
    )
    return (
        df.withColumn("_silver_duplicate_rank", F.row_number().over(window))
        .withColumn("_silver_duplicate_count", F.count(F.lit(1)).over(Window.partitionBy(spec.primary_key)))
        .filter(F.col("_silver_duplicate_rank") == 1)
        .withColumn("_silver_was_deduplicated", F.col("_silver_duplicate_count") > 1)
        .drop("_silver_duplicate_rank", "_silver_duplicate_count")
    )


def add_late_arrival_columns(df: DataFrame, spec: SilverEventSpec) -> DataFrame:
    """Add late-arrival tracking based on ingestion time minus event time."""
    arrival_delay_seconds = (
        F.unix_timestamp(F.col(spec.ingestion_timestamp_column))
        - F.unix_timestamp(F.col(spec.event_timestamp_column))
    )
    return (
        df.withColumn("_arrival_delay_hours", F.round(arrival_delay_seconds / F.lit(3600), 3))
        .withColumn("_late_arrival_threshold_hours", F.lit(spec.late_arrival_threshold_hours))
        .withColumn(
            "_is_late_arriving",
            F.col("_arrival_delay_hours") > F.col("_late_arrival_threshold_hours"),
        )
    )


def apply_silver_event_controls(df: DataFrame, spec: SilverEventSpec) -> DataFrame:
    """Apply schema evolution, deduplication, and late-arrival controls."""
    aligned = add_missing_optional_columns(df)
    deduplicated = deduplicate_by_primary_key(aligned, spec)
    return add_late_arrival_columns(deduplicated, spec)
