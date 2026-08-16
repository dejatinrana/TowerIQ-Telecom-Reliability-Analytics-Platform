"""SCD Type 2 dimension transformations for TowerIQ."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


OPEN_ENDED_DATE = "9999-12-31"
DEFAULT_VALID_FROM = "1900-01-01"


@dataclass(frozen=True)
class Scd2DimensionSpec:
    """Configuration for building one SCD Type 2 dimension."""

    table_name: str
    natural_key: str
    tracked_columns: tuple[str, ...]
    effective_from_column: str | None = None


SCD2_DIMENSION_SPECS: dict[str, Scd2DimensionSpec] = {
    "regions": Scd2DimensionSpec(
        table_name="regions",
        natural_key="region_id",
        tracked_columns=(
            "region_name",
            "country",
            "state",
            "city",
            "zone",
            "region_type",
            "timezone",
            "is_active",
        ),
    ),
    "service_plans": Scd2DimensionSpec(
        table_name="service_plans",
        natural_key="plan_id",
        tracked_columns=(
            "plan_name",
            "plan_type",
            "monthly_data_limit_gb",
            "voice_limit_minutes",
            "priority_level",
            "is_5g_enabled",
            "monthly_price",
        ),
    ),
    "towers": Scd2DimensionSpec(
        table_name="towers",
        natural_key="tower_id",
        tracked_columns=(
            "region_id",
            "tower_name",
            "city",
            "latitude",
            "longitude",
            "tower_type",
            "supported_networks",
            "capacity_score",
            "tower_status",
        ),
        effective_from_column="activation_date",
    ),
    "subscribers": Scd2DimensionSpec(
        table_name="subscribers",
        natural_key="subscriber_id",
        tracked_columns=(
            "home_region_id",
            "plan_id",
            "subscriber_status",
            "customer_segment",
            "age_band",
        ),
        effective_from_column="activation_date",
    ),
    "devices": Scd2DimensionSpec(
        table_name="devices",
        natural_key="device_id",
        tracked_columns=(
            "subscriber_id",
            "manufacturer",
            "model",
            "os_type",
            "supports_5g",
            "release_year",
            "device_status",
        ),
    ),
}


def add_record_hash(df: DataFrame, tracked_columns: tuple[str, ...]) -> DataFrame:
    """Add a stable hash for tracked dimension attributes."""
    hash_input = F.concat_ws(
        "||",
        *[F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in tracked_columns],
    )
    return df.withColumn("_scd_record_hash", F.sha2(hash_input, 256))


def add_valid_from(df: DataFrame, spec: Scd2DimensionSpec) -> DataFrame:
    """Add the start date for a dimension version."""
    batch_effective_date = F.to_date(F.regexp_extract(F.col("_pipeline_batch_id"), r"(20\d{6})", 1), "yyyyMMdd")
    if spec.effective_from_column and spec.effective_from_column in df.columns:
        return df.withColumn(
            "_scd_valid_from",
            F.coalesce(F.to_date(spec.effective_from_column), batch_effective_date, F.to_date(F.lit(DEFAULT_VALID_FROM))),
        )
    return df.withColumn("_scd_valid_from", F.coalesce(batch_effective_date, F.to_date(F.lit(DEFAULT_VALID_FROM))))


def build_scd2_dimension(df: DataFrame, spec: Scd2DimensionSpec) -> DataFrame:
    """Build an SCD Type 2 dimension table from valid Silver dimension records."""
    versioned = add_valid_from(add_record_hash(df, spec.tracked_columns), spec)
    versioned = versioned.dropDuplicates([spec.natural_key, "_scd_record_hash", "_scd_valid_from"])

    window = Window.partitionBy(spec.natural_key).orderBy(F.col("_scd_valid_from"), F.col("_silver_loaded_at"))
    next_valid_from = F.lead("_scd_valid_from").over(window)

    return (
        versioned.withColumn("_scd_valid_to", F.date_sub(next_valid_from, 1))
        .withColumn("_scd_valid_to", F.coalesce(F.col("_scd_valid_to"), F.to_date(F.lit(OPEN_ENDED_DATE))))
        .withColumn("_scd_is_current", next_valid_from.isNull())
        .withColumn("_scd_loaded_at", F.current_timestamp())
    )


def current_scd2_records(df: DataFrame) -> DataFrame:
    """Return current records when an SCD2 dimension is provided."""
    if "_scd_is_current" not in df.columns:
        return df
    return df.filter(F.col("_scd_is_current"))


def build_scd2_dimensions(silver_tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build all configured SCD2 dimensions."""
    return {
        table_name: build_scd2_dimension(silver_tables[table_name], spec)
        for table_name, spec in SCD2_DIMENSION_SPECS.items()
    }
