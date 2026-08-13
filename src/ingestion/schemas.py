"""Explicit schemas for TowerIQ raw source tables."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


RAW_SCHEMAS: dict[str, StructType] = {
    "regions": StructType(
        [
            StructField("region_id", StringType(), False),
            StructField("region_name", StringType(), False),
            StructField("country", StringType(), False),
            StructField("state", StringType(), False),
            StructField("city", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("region_type", StringType(), False),
            StructField("timezone", StringType(), False),
            StructField("is_active", BooleanType(), False),
        ]
    ),
    "towers": StructType(
        [
            StructField("tower_id", StringType(), False),
            StructField("region_id", StringType(), False),
            StructField("tower_name", StringType(), False),
            StructField("city", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("tower_type", StringType(), False),
            StructField("supported_networks", StringType(), False),
            StructField("capacity_score", IntegerType(), False),
            StructField("activation_date", DateType(), False),
            StructField("tower_status", StringType(), False),
        ]
    ),
    "service_plans": StructType(
        [
            StructField("plan_id", StringType(), False),
            StructField("plan_name", StringType(), False),
            StructField("plan_type", StringType(), False),
            StructField("monthly_data_limit_gb", IntegerType(), False),
            StructField("voice_limit_minutes", IntegerType(), False),
            StructField("priority_level", IntegerType(), False),
            StructField("is_5g_enabled", BooleanType(), False),
            StructField("monthly_price", IntegerType(), False),
        ]
    ),
    "subscribers": StructType(
        [
            StructField("subscriber_id", StringType(), False),
            StructField("home_region_id", StringType(), False),
            StructField("plan_id", StringType(), False),
            StructField("activation_date", DateType(), False),
            StructField("subscriber_status", StringType(), False),
            StructField("customer_segment", StringType(), False),
            StructField("age_band", StringType(), False),
        ]
    ),
    "devices": StructType(
        [
            StructField("device_id", StringType(), False),
            StructField("subscriber_id", StringType(), False),
            StructField("manufacturer", StringType(), False),
            StructField("model", StringType(), False),
            StructField("os_type", StringType(), False),
            StructField("supports_5g", BooleanType(), False),
            StructField("release_year", IntegerType(), False),
            StructField("device_status", StringType(), False),
        ]
    ),
    "network_events": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("subscriber_id", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("tower_id", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("event_type", StringType(), False),
            StructField("network_type", StringType(), False),
            StructField("signal_strength_dbm", IntegerType(), False),
            StructField("latency_ms", IntegerType(), False),
            StructField("status", StringType(), False),
            StructField("error_code", StringType(), True),
            StructField("batch_id", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("schema_version", StringType(), False),
        ]
    ),
    "calls": StructType(
        [
            StructField("call_id", StringType(), False),
            StructField("subscriber_id", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("tower_id", StringType(), False),
            StructField("call_start_timestamp", TimestampType(), False),
            StructField("call_end_timestamp", TimestampType(), False),
            StructField("duration_seconds", IntegerType(), False),
            StructField("call_type", StringType(), False),
            StructField("network_type", StringType(), False),
            StructField("call_status", StringType(), False),
            StructField("drop_reason", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("batch_id", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("schema_version", StringType(), False),
        ]
    ),
    "data_sessions": StructType(
        [
            StructField("session_id", StringType(), False),
            StructField("subscriber_id", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("tower_id", StringType(), False),
            StructField("session_start_timestamp", TimestampType(), False),
            StructField("session_end_timestamp", TimestampType(), False),
            StructField("duration_seconds", IntegerType(), False),
            StructField("network_type", StringType(), False),
            StructField("bytes_uploaded", LongType(), False),
            StructField("bytes_downloaded", LongType(), False),
            StructField("latency_ms", IntegerType(), False),
            StructField("session_status", StringType(), False),
            StructField("failure_reason", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("batch_id", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("schema_version", StringType(), False),
        ]
    ),
    "tower_alarms": StructType(
        [
            StructField("alarm_id", StringType(), False),
            StructField("tower_id", StringType(), False),
            StructField("alarm_timestamp", TimestampType(), False),
            StructField("alarm_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("alarm_status", StringType(), False),
            StructField("resolved_timestamp", TimestampType(), True),
            StructField("description", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("batch_id", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("schema_version", StringType(), False),
        ]
    ),
}
