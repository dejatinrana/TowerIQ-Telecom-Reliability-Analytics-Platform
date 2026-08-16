"""Table-specific quality validators for TowerIQ Bronze tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from src.quality.rules import (
    MAX_SIGNAL_STRENGTH_DBM,
    MIN_SIGNAL_STRENGTH_DBM,
    SUPPORTED_ALARM_SEVERITIES,
    SUPPORTED_ALARM_STATUSES,
    SUPPORTED_ALARM_TYPES,
    SUPPORTED_CALL_STATUSES,
    SUPPORTED_CALL_TYPES,
    SUPPORTED_DEVICE_OS_TYPES,
    SUPPORTED_DEVICE_STATUSES,
    SUPPORTED_NETWORK_EVENT_STATUSES,
    SUPPORTED_NETWORK_EVENT_TYPES,
    SUPPORTED_NETWORK_TYPES,
    SUPPORTED_PLAN_TYPES,
    SUPPORTED_REGION_TYPES,
    SUPPORTED_SESSION_STATUSES,
    SUPPORTED_SUBSCRIBER_STATUSES,
    SUPPORTED_TOWER_STATUSES,
    SUPPORTED_TOWER_TYPES,
    collect_rejection_reasons,
    is_blank,
    reason_when,
)


@dataclass(frozen=True)
class TableQualityResult:
    """Valid and invalid records for one table."""

    table_name: str
    valid: DataFrame
    invalid: DataFrame
    classified: DataFrame | None = None


def add_key_count(df: DataFrame, key_column: str) -> DataFrame:
    """Attach duplicate-count metadata for a primary key column."""
    counts = df.groupBy(key_column).count().withColumnRenamed("count", f"_count_{key_column}")
    return df.join(counts, on=key_column, how="left")


def add_reference_key(
    df: DataFrame,
    ref_df: DataFrame,
    left_column: str,
    ref_column: str,
    alias: str,
) -> DataFrame:
    """Attach a reference key column for existence validation."""
    keys = ref_df.select(F.col(ref_column).alias(alias)).dropDuplicates()
    return df.join(F.broadcast(keys), F.col(left_column) == F.col(alias), "left")


def persist_valid_parent(result: TableQualityResult) -> TableQualityResult:
    """Persist reusable valid parent rows for cascading child-table validation."""
    result.valid.persist(StorageLevel.MEMORY_AND_DISK)
    return result


def unpersist_valid_parents(results: list[TableQualityResult]) -> None:
    """Release cached valid parent rows after cascading validation finishes."""
    for result in results:
        result.valid.unpersist()


def finalize_validation(df: DataFrame, table_name: str, drop_columns: set[str]) -> TableQualityResult:
    """Split a validation DataFrame into valid and invalid records."""
    selected = df.select(*[column for column in df.columns if column not in drop_columns])
    classified = selected.withColumn("_is_quality_valid", F.size(F.col("_rejection_reasons")) == 0)
    valid = (
        classified.filter(F.col("_is_quality_valid"))
        .drop("_rejection_reasons", "_is_quality_valid")
        .withColumn("_silver_loaded_at", F.current_timestamp())
    )
    invalid = (
        classified.filter(~F.col("_is_quality_valid"))
        .drop("_is_quality_valid")
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_name))
    )
    return TableQualityResult(table_name=table_name, valid=valid, invalid=invalid, classified=classified)


def validate_regions(regions: DataFrame) -> TableQualityResult:
    df = add_key_count(regions, "region_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("region_id"), "region_id_missing"),
            reason_when(F.col("_count_region_id") > 1, "region_id_duplicate"),
            reason_when(is_blank("region_name"), "region_name_missing"),
            reason_when(is_blank("country"), "country_missing"),
            reason_when(is_blank("state"), "state_missing"),
            reason_when(is_blank("city"), "city_missing"),
            reason_when(is_blank("timezone"), "timezone_missing"),
            reason_when(~F.col("region_type").isin(SUPPORTED_REGION_TYPES), "region_type_unsupported"),
            reason_when(F.col("is_active").isNull(), "is_active_missing"),
        ),
    )
    return finalize_validation(df, "regions", {"_count_region_id"})


def validate_service_plans(service_plans: DataFrame) -> TableQualityResult:
    df = add_key_count(service_plans, "plan_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("plan_id"), "plan_id_missing"),
            reason_when(F.col("_count_plan_id") > 1, "plan_id_duplicate"),
            reason_when(is_blank("plan_name"), "plan_name_missing"),
            reason_when(~F.col("plan_type").isin(SUPPORTED_PLAN_TYPES), "plan_type_unsupported"),
            reason_when(F.col("monthly_data_limit_gb") < 0, "monthly_data_limit_negative"),
            reason_when(F.col("voice_limit_minutes") < 0, "voice_limit_negative"),
            reason_when((F.col("priority_level") < 1) | (F.col("priority_level") > 5), "priority_level_invalid"),
            reason_when(F.col("is_5g_enabled").isNull(), "is_5g_enabled_missing"),
            reason_when(F.col("monthly_price") < 0, "monthly_price_negative"),
        ),
    )
    return finalize_validation(df, "service_plans", {"_count_plan_id"})


def validate_towers(towers: DataFrame, regions: DataFrame) -> TableQualityResult:
    df = add_key_count(towers, "tower_id")
    df = add_reference_key(df, regions, "region_id", "region_id", "_ref_region_id")
    supported_networks = F.split(F.col("supported_networks"), "\\|")
    unsupported_networks = F.array_except(
        supported_networks,
        F.array(*[F.lit(network_type) for network_type in SUPPORTED_NETWORK_TYPES]),
    )
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("tower_id"), "tower_id_missing"),
            reason_when(F.col("_count_tower_id") > 1, "tower_id_duplicate"),
            reason_when(is_blank("region_id"), "region_id_missing"),
            reason_when(F.col("region_id").isNotNull() & F.col("_ref_region_id").isNull(), "region_id_unknown"),
            reason_when(is_blank("tower_name"), "tower_name_missing"),
            reason_when((F.col("latitude") < -90) | (F.col("latitude") > 90), "latitude_invalid"),
            reason_when((F.col("longitude") < -180) | (F.col("longitude") > 180), "longitude_invalid"),
            reason_when(~F.col("tower_type").isin(SUPPORTED_TOWER_TYPES), "tower_type_unsupported"),
            reason_when(is_blank("supported_networks"), "supported_networks_missing"),
            reason_when(F.size(unsupported_networks) > 0, "supported_networks_invalid"),
            reason_when((F.col("capacity_score") < 0) | (F.col("capacity_score") > 100), "capacity_score_invalid"),
            reason_when(F.col("activation_date").isNull(), "activation_date_missing"),
            reason_when(~F.col("tower_status").isin(SUPPORTED_TOWER_STATUSES), "tower_status_unsupported"),
        ),
    )
    return finalize_validation(df, "towers", {"_count_tower_id", "_ref_region_id"})


def validate_subscribers(subscribers: DataFrame, regions: DataFrame, service_plans: DataFrame) -> TableQualityResult:
    df = add_key_count(subscribers, "subscriber_id")
    df = add_reference_key(df, regions, "home_region_id", "region_id", "_ref_home_region_id")
    df = add_reference_key(df, service_plans, "plan_id", "plan_id", "_ref_plan_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("subscriber_id"), "subscriber_id_missing"),
            reason_when(F.col("_count_subscriber_id") > 1, "subscriber_id_duplicate"),
            reason_when(is_blank("home_region_id"), "home_region_id_missing"),
            reason_when(
                F.col("home_region_id").isNotNull() & F.col("_ref_home_region_id").isNull(),
                "home_region_id_unknown",
            ),
            reason_when(is_blank("plan_id"), "plan_id_missing"),
            reason_when(F.col("plan_id").isNotNull() & F.col("_ref_plan_id").isNull(), "plan_id_unknown"),
            reason_when(F.col("activation_date").isNull(), "activation_date_missing"),
            reason_when(~F.col("subscriber_status").isin(SUPPORTED_SUBSCRIBER_STATUSES), "subscriber_status_unsupported"),
            reason_when(is_blank("customer_segment"), "customer_segment_missing"),
            reason_when(is_blank("age_band"), "age_band_missing"),
        ),
    )
    return finalize_validation(df, "subscribers", {"_count_subscriber_id", "_ref_home_region_id", "_ref_plan_id"})


def validate_devices(devices: DataFrame, subscribers: DataFrame) -> TableQualityResult:
    df = add_key_count(devices, "device_id")
    df = add_reference_key(df, subscribers, "subscriber_id", "subscriber_id", "_ref_subscriber_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("device_id"), "device_id_missing"),
            reason_when(F.col("_count_device_id") > 1, "device_id_duplicate"),
            reason_when(is_blank("subscriber_id"), "subscriber_id_missing"),
            reason_when(
                F.col("subscriber_id").isNotNull() & F.col("_ref_subscriber_id").isNull(),
                "subscriber_id_unknown",
            ),
            reason_when(is_blank("manufacturer"), "manufacturer_missing"),
            reason_when(is_blank("model"), "model_missing"),
            reason_when(~F.col("os_type").isin(SUPPORTED_DEVICE_OS_TYPES), "os_type_unsupported"),
            reason_when(F.col("supports_5g").isNull(), "supports_5g_missing"),
            reason_when((F.col("release_year") < 2007) | (F.col("release_year") > 2026), "release_year_invalid"),
            reason_when(~F.col("device_status").isin(SUPPORTED_DEVICE_STATUSES), "device_status_unsupported"),
        ),
    )
    return finalize_validation(df, "devices", {"_count_device_id", "_ref_subscriber_id"})


def validate_network_events(
    network_events: DataFrame,
    towers: DataFrame,
    subscribers: DataFrame,
    devices: DataFrame,
) -> TableQualityResult:
    df = network_events
    df = add_reference_key(df, towers, "tower_id", "tower_id", "_ref_tower_id")
    df = add_reference_key(df, subscribers, "subscriber_id", "subscriber_id", "_ref_subscriber_id")
    df = add_reference_key(df, devices, "device_id", "device_id", "_ref_device_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("event_id"), "event_id_missing"),
            reason_when(is_blank("subscriber_id"), "subscriber_id_missing"),
            reason_when(is_blank("device_id"), "device_id_missing"),
            reason_when(is_blank("tower_id"), "tower_id_missing"),
            reason_when(F.col("event_timestamp").isNull(), "event_timestamp_missing"),
            reason_when(F.col("ingestion_timestamp").isNull(), "ingestion_timestamp_missing"),
            reason_when(~F.col("network_type").isin(SUPPORTED_NETWORK_TYPES), "network_type_unsupported"),
            reason_when(~F.col("event_type").isin(SUPPORTED_NETWORK_EVENT_TYPES), "event_type_unsupported"),
            reason_when(~F.col("status").isin(SUPPORTED_NETWORK_EVENT_STATUSES), "status_unsupported"),
            reason_when(F.col("signal_strength_dbm") < MIN_SIGNAL_STRENGTH_DBM, "signal_strength_too_low"),
            reason_when(F.col("signal_strength_dbm") > MAX_SIGNAL_STRENGTH_DBM, "signal_strength_too_high"),
            reason_when(F.col("latency_ms") < 0, "latency_negative"),
            reason_when(F.col("ingestion_timestamp") < F.col("event_timestamp"), "ingestion_before_event"),
            reason_when(F.col("tower_id").isNotNull() & F.col("_ref_tower_id").isNull(), "tower_id_unknown"),
            reason_when(
                F.col("subscriber_id").isNotNull() & F.col("_ref_subscriber_id").isNull(),
                "subscriber_id_unknown",
            ),
            reason_when(F.col("device_id").isNotNull() & F.col("_ref_device_id").isNull(), "device_id_unknown"),
        ),
    )
    return finalize_validation(
        df,
        "network_events",
        {"_ref_tower_id", "_ref_subscriber_id", "_ref_device_id"},
    )


def validate_calls(calls: DataFrame, towers: DataFrame, subscribers: DataFrame, devices: DataFrame) -> TableQualityResult:
    df = calls
    df = add_reference_key(df, towers, "tower_id", "tower_id", "_ref_tower_id")
    df = add_reference_key(df, subscribers, "subscriber_id", "subscriber_id", "_ref_subscriber_id")
    df = add_reference_key(df, devices, "device_id", "device_id", "_ref_device_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("call_id"), "call_id_missing"),
            reason_when(is_blank("subscriber_id"), "subscriber_id_missing"),
            reason_when(is_blank("device_id"), "device_id_missing"),
            reason_when(is_blank("tower_id"), "tower_id_missing"),
            reason_when(F.col("call_start_timestamp").isNull(), "call_start_timestamp_missing"),
            reason_when(F.col("call_end_timestamp").isNull(), "call_end_timestamp_missing"),
            reason_when(F.col("duration_seconds") < 0, "duration_negative"),
            reason_when(F.col("call_end_timestamp") < F.col("call_start_timestamp"), "call_end_before_start"),
            reason_when(~F.col("call_type").isin(SUPPORTED_CALL_TYPES), "call_type_unsupported"),
            reason_when(~F.col("network_type").isin(SUPPORTED_NETWORK_TYPES), "network_type_unsupported"),
            reason_when(~F.col("call_status").isin(SUPPORTED_CALL_STATUSES), "call_status_unsupported"),
            reason_when((F.col("call_status") != "completed") & is_blank("drop_reason"), "drop_reason_missing"),
            reason_when(F.col("tower_id").isNotNull() & F.col("_ref_tower_id").isNull(), "tower_id_unknown"),
            reason_when(
                F.col("subscriber_id").isNotNull() & F.col("_ref_subscriber_id").isNull(),
                "subscriber_id_unknown",
            ),
            reason_when(F.col("device_id").isNotNull() & F.col("_ref_device_id").isNull(), "device_id_unknown"),
        ),
    )
    return finalize_validation(df, "calls", {"_ref_tower_id", "_ref_subscriber_id", "_ref_device_id"})


def validate_data_sessions(
    data_sessions: DataFrame,
    towers: DataFrame,
    subscribers: DataFrame,
    devices: DataFrame,
) -> TableQualityResult:
    df = data_sessions
    df = add_reference_key(df, towers, "tower_id", "tower_id", "_ref_tower_id")
    df = add_reference_key(df, subscribers, "subscriber_id", "subscriber_id", "_ref_subscriber_id")
    df = add_reference_key(df, devices, "device_id", "device_id", "_ref_device_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("session_id"), "session_id_missing"),
            reason_when(is_blank("subscriber_id"), "subscriber_id_missing"),
            reason_when(is_blank("device_id"), "device_id_missing"),
            reason_when(is_blank("tower_id"), "tower_id_missing"),
            reason_when(F.col("session_start_timestamp").isNull(), "session_start_timestamp_missing"),
            reason_when(F.col("session_end_timestamp").isNull(), "session_end_timestamp_missing"),
            reason_when(F.col("duration_seconds") < 0, "duration_negative"),
            reason_when(F.col("session_end_timestamp") < F.col("session_start_timestamp"), "session_end_before_start"),
            reason_when(~F.col("network_type").isin(SUPPORTED_NETWORK_TYPES), "network_type_unsupported"),
            reason_when(F.col("bytes_uploaded") < 0, "bytes_uploaded_negative"),
            reason_when(F.col("bytes_downloaded") < 0, "bytes_downloaded_negative"),
            reason_when(F.col("latency_ms") < 0, "latency_negative"),
            reason_when(~F.col("session_status").isin(SUPPORTED_SESSION_STATUSES), "session_status_unsupported"),
            reason_when((F.col("session_status") != "completed") & is_blank("failure_reason"), "failure_reason_missing"),
            reason_when(F.col("tower_id").isNotNull() & F.col("_ref_tower_id").isNull(), "tower_id_unknown"),
            reason_when(
                F.col("subscriber_id").isNotNull() & F.col("_ref_subscriber_id").isNull(),
                "subscriber_id_unknown",
            ),
            reason_when(F.col("device_id").isNotNull() & F.col("_ref_device_id").isNull(), "device_id_unknown"),
        ),
    )
    return finalize_validation(
        df,
        "data_sessions",
        {"_ref_tower_id", "_ref_subscriber_id", "_ref_device_id"},
    )


def validate_tower_alarms(tower_alarms: DataFrame, towers: DataFrame) -> TableQualityResult:
    df = tower_alarms
    df = add_reference_key(df, towers, "tower_id", "tower_id", "_ref_tower_id")
    df = df.withColumn(
        "_rejection_reasons",
        collect_rejection_reasons(
            reason_when(is_blank("alarm_id"), "alarm_id_missing"),
            reason_when(is_blank("tower_id"), "tower_id_missing"),
            reason_when(F.col("tower_id").isNotNull() & F.col("_ref_tower_id").isNull(), "tower_id_unknown"),
            reason_when(F.col("alarm_timestamp").isNull(), "alarm_timestamp_missing"),
            reason_when(~F.col("alarm_type").isin(SUPPORTED_ALARM_TYPES), "alarm_type_unsupported"),
            reason_when(~F.col("severity").isin(SUPPORTED_ALARM_SEVERITIES), "severity_unsupported"),
            reason_when(~F.col("alarm_status").isin(SUPPORTED_ALARM_STATUSES), "alarm_status_unsupported"),
            reason_when(
                (F.col("alarm_status") == "resolved") & F.col("resolved_timestamp").isNull(),
                "resolved_timestamp_missing",
            ),
            reason_when(F.col("resolved_timestamp") < F.col("alarm_timestamp"), "resolved_before_alarm"),
            reason_when(is_blank("description"), "description_missing"),
        ),
    )
    return finalize_validation(df, "tower_alarms", {"_ref_tower_id"})


def validate_all_tables(bronze_tables: dict[str, DataFrame]) -> list[TableQualityResult]:
    """Validate every first-version TowerIQ table.

    Parent dimension tables are validated first. Child tables then validate
    foreign keys against the valid parent records, not against dirty Bronze
    records that may later be quarantined.
    """
    regions_result = validate_regions(bronze_tables["regions"])
    service_plans_result = validate_service_plans(bronze_tables["service_plans"])
    towers_result = validate_towers(bronze_tables["towers"], regions_result.valid)
    subscribers_result = validate_subscribers(
        bronze_tables["subscribers"],
        regions_result.valid,
        service_plans_result.valid,
    )
    devices_result = validate_devices(bronze_tables["devices"], subscribers_result.valid)

    network_events_result = validate_network_events(
        bronze_tables["network_events"],
        towers_result.valid,
        subscribers_result.valid,
        devices_result.valid,
    )
    calls_result = validate_calls(
        bronze_tables["calls"],
        towers_result.valid,
        subscribers_result.valid,
        devices_result.valid,
    )
    data_sessions_result = validate_data_sessions(
        bronze_tables["data_sessions"],
        towers_result.valid,
        subscribers_result.valid,
        devices_result.valid,
    )
    tower_alarms_result = validate_tower_alarms(bronze_tables["tower_alarms"], towers_result.valid)

    return [
        regions_result,
        service_plans_result,
        towers_result,
        subscribers_result,
        devices_result,
        network_events_result,
        calls_result,
        data_sessions_result,
        tower_alarms_result,
    ]
