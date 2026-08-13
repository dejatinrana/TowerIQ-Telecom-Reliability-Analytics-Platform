"""Silver enrichment transformations for telecom event tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class SilverEnrichmentResult:
    """One enriched Silver table."""

    table_name: str
    dataframe: DataFrame


def tower_region_context(towers: DataFrame, regions: DataFrame) -> DataFrame:
    """Build reusable tower and region context."""
    return (
        towers.alias("tower")
        .join(regions.alias("region"), F.col("tower.region_id") == F.col("region.region_id"), "left")
        .select(
            F.col("tower.tower_id"),
            F.col("tower.region_id"),
            F.col("region.region_name"),
            F.col("region.state"),
            F.col("region.city").alias("region_city"),
            F.col("region.zone"),
            F.col("region.region_type"),
            F.col("tower.tower_name"),
            F.col("tower.city").alias("tower_city"),
            F.col("tower.latitude"),
            F.col("tower.longitude"),
            F.col("tower.tower_type"),
            F.col("tower.supported_networks"),
            F.col("tower.capacity_score"),
            F.col("tower.tower_status"),
        )
    )


def subscriber_device_plan_context(
    subscribers: DataFrame,
    devices: DataFrame,
    service_plans: DataFrame,
) -> DataFrame:
    """Build reusable subscriber, device, and service plan context."""
    return (
        devices.alias("device")
        .join(subscribers.alias("subscriber"), F.col("device.subscriber_id") == F.col("subscriber.subscriber_id"), "left")
        .join(service_plans.alias("plan"), F.col("subscriber.plan_id") == F.col("plan.plan_id"), "left")
        .select(
            F.col("device.device_id"),
            F.col("subscriber.subscriber_id"),
            F.col("subscriber.home_region_id"),
            F.col("subscriber.plan_id"),
            F.col("subscriber.subscriber_status"),
            F.col("subscriber.customer_segment"),
            F.col("subscriber.age_band"),
            F.col("device.manufacturer"),
            F.col("device.model"),
            F.col("device.os_type"),
            F.col("device.supports_5g"),
            F.col("device.release_year"),
            F.col("device.device_status"),
            F.col("plan.plan_name"),
            F.col("plan.plan_type"),
            F.col("plan.monthly_data_limit_gb"),
            F.col("plan.voice_limit_minutes"),
            F.col("plan.priority_level"),
            F.col("plan.is_5g_enabled").alias("plan_is_5g_enabled"),
            F.col("plan.monthly_price"),
        )
    )


def add_event_date_parts(df: DataFrame, timestamp_column: str) -> DataFrame:
    """Add common date/time columns for analytics."""
    return (
        df.withColumn("event_date", F.to_date(F.col(timestamp_column)))
        .withColumn("event_hour", F.hour(F.col(timestamp_column)))
        .withColumn("event_day_of_week", F.date_format(F.col(timestamp_column), "E"))
    )


def enrich_network_events(
    network_events: DataFrame,
    towers: DataFrame,
    regions: DataFrame,
    subscribers: DataFrame,
    devices: DataFrame,
    service_plans: DataFrame,
) -> DataFrame:
    """Enrich valid network events with tower, region, subscriber, device, and plan context."""
    tower_context = tower_region_context(towers, regions)
    subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)

    enriched = (
        network_events.alias("event")
        .join(tower_context.alias("tower_ctx"), F.col("event.tower_id") == F.col("tower_ctx.tower_id"), "left")
        .join(
            subscriber_context.alias("sub_ctx"),
            (F.col("event.subscriber_id") == F.col("sub_ctx.subscriber_id"))
            & (F.col("event.device_id") == F.col("sub_ctx.device_id")),
            "left",
        )
        .select(
            F.col("event.*"),
            F.col("tower_ctx.region_id"),
            F.col("tower_ctx.region_name"),
            F.col("tower_ctx.state"),
            F.col("tower_ctx.region_city"),
            F.col("tower_ctx.zone"),
            F.col("tower_ctx.region_type"),
            F.col("tower_ctx.tower_name"),
            F.col("tower_ctx.tower_city"),
            F.col("tower_ctx.latitude"),
            F.col("tower_ctx.longitude"),
            F.col("tower_ctx.tower_type"),
            F.col("tower_ctx.capacity_score"),
            F.col("tower_ctx.tower_status"),
            F.col("sub_ctx.home_region_id"),
            F.col("sub_ctx.plan_id"),
            F.col("sub_ctx.subscriber_status"),
            F.col("sub_ctx.customer_segment"),
            F.col("sub_ctx.age_band"),
            F.col("sub_ctx.manufacturer"),
            F.col("sub_ctx.model"),
            F.col("sub_ctx.os_type"),
            F.col("sub_ctx.supports_5g"),
            F.col("sub_ctx.release_year"),
            F.col("sub_ctx.device_status"),
            F.col("sub_ctx.plan_name"),
            F.col("sub_ctx.plan_type"),
            F.col("sub_ctx.monthly_data_limit_gb"),
            F.col("sub_ctx.priority_level"),
            F.col("sub_ctx.plan_is_5g_enabled"),
        )
    )
    return add_event_date_parts(enriched, "event_timestamp").withColumn("_silver_enriched_at", F.current_timestamp())


def enrich_calls(
    calls: DataFrame,
    towers: DataFrame,
    regions: DataFrame,
    subscribers: DataFrame,
    devices: DataFrame,
    service_plans: DataFrame,
) -> DataFrame:
    """Enrich valid calls with tower, region, subscriber, device, and plan context."""
    tower_context = tower_region_context(towers, regions)
    subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)
    enriched = (
        calls.alias("call")
        .join(tower_context.alias("tower_ctx"), F.col("call.tower_id") == F.col("tower_ctx.tower_id"), "left")
        .join(
            subscriber_context.alias("sub_ctx"),
            (F.col("call.subscriber_id") == F.col("sub_ctx.subscriber_id"))
            & (F.col("call.device_id") == F.col("sub_ctx.device_id")),
            "left",
        )
        .select(
            F.col("call.*"),
            F.col("tower_ctx.region_id"),
            F.col("tower_ctx.region_name"),
            F.col("tower_ctx.state"),
            F.col("tower_ctx.region_city"),
            F.col("tower_ctx.zone"),
            F.col("tower_ctx.region_type"),
            F.col("tower_ctx.tower_name"),
            F.col("tower_ctx.tower_type"),
            F.col("tower_ctx.capacity_score"),
            F.col("sub_ctx.home_region_id"),
            F.col("sub_ctx.plan_id"),
            F.col("sub_ctx.subscriber_status"),
            F.col("sub_ctx.customer_segment"),
            F.col("sub_ctx.manufacturer"),
            F.col("sub_ctx.model"),
            F.col("sub_ctx.os_type"),
            F.col("sub_ctx.supports_5g"),
            F.col("sub_ctx.plan_name"),
            F.col("sub_ctx.plan_type"),
            F.col("sub_ctx.priority_level"),
        )
    )
    return (
        add_event_date_parts(enriched, "call_start_timestamp")
        .withColumn("is_dropped_call", F.col("call_status") == "dropped")
        .withColumn("is_failed_call", F.col("call_status") == "failed")
        .withColumn("_silver_enriched_at", F.current_timestamp())
    )


def enrich_data_sessions(
    data_sessions: DataFrame,
    towers: DataFrame,
    regions: DataFrame,
    subscribers: DataFrame,
    devices: DataFrame,
    service_plans: DataFrame,
) -> DataFrame:
    """Enrich valid data sessions with tower, region, subscriber, device, and plan context."""
    tower_context = tower_region_context(towers, regions)
    subscriber_context = subscriber_device_plan_context(subscribers, devices, service_plans)
    enriched = (
        data_sessions.alias("session")
        .join(tower_context.alias("tower_ctx"), F.col("session.tower_id") == F.col("tower_ctx.tower_id"), "left")
        .join(
            subscriber_context.alias("sub_ctx"),
            (F.col("session.subscriber_id") == F.col("sub_ctx.subscriber_id"))
            & (F.col("session.device_id") == F.col("sub_ctx.device_id")),
            "left",
        )
        .select(
            F.col("session.*"),
            F.col("tower_ctx.region_id"),
            F.col("tower_ctx.region_name"),
            F.col("tower_ctx.state"),
            F.col("tower_ctx.region_city"),
            F.col("tower_ctx.zone"),
            F.col("tower_ctx.region_type"),
            F.col("tower_ctx.tower_name"),
            F.col("tower_ctx.tower_type"),
            F.col("tower_ctx.capacity_score"),
            F.col("sub_ctx.home_region_id"),
            F.col("sub_ctx.plan_id"),
            F.col("sub_ctx.subscriber_status"),
            F.col("sub_ctx.customer_segment"),
            F.col("sub_ctx.manufacturer"),
            F.col("sub_ctx.model"),
            F.col("sub_ctx.os_type"),
            F.col("sub_ctx.supports_5g"),
            F.col("sub_ctx.plan_name"),
            F.col("sub_ctx.plan_type"),
            F.col("sub_ctx.priority_level"),
        )
    )
    return (
        add_event_date_parts(enriched, "session_start_timestamp")
        .withColumn("total_bytes", F.col("bytes_uploaded") + F.col("bytes_downloaded"))
        .withColumn("total_mb", F.round(F.col("total_bytes") / F.lit(1024 * 1024), 3))
        .withColumn("is_failed_session", F.col("session_status") != "completed")
        .withColumn("_silver_enriched_at", F.current_timestamp())
    )


def enrich_tower_alarms(tower_alarms: DataFrame, towers: DataFrame, regions: DataFrame) -> DataFrame:
    """Enrich tower alarms with tower and region context."""
    tower_context = tower_region_context(towers, regions)
    enriched = (
        tower_alarms.alias("alarm")
        .join(tower_context.alias("tower_ctx"), F.col("alarm.tower_id") == F.col("tower_ctx.tower_id"), "left")
        .select(
            F.col("alarm.*"),
            F.col("tower_ctx.region_id"),
            F.col("tower_ctx.region_name"),
            F.col("tower_ctx.state"),
            F.col("tower_ctx.region_city"),
            F.col("tower_ctx.zone"),
            F.col("tower_ctx.region_type"),
            F.col("tower_ctx.tower_name"),
            F.col("tower_ctx.tower_type"),
            F.col("tower_ctx.capacity_score"),
            F.col("tower_ctx.tower_status"),
        )
    )
    return (
        add_event_date_parts(enriched, "alarm_timestamp")
        .withColumn("is_critical_alarm", F.col("severity") == "critical")
        .withColumn("_silver_enriched_at", F.current_timestamp())
    )


def build_enriched_event_tables(silver_tables: dict[str, DataFrame]) -> list[SilverEnrichmentResult]:
    """Build all enriched Silver event tables."""
    regions = silver_tables["regions"]
    towers = silver_tables["towers"]
    service_plans = silver_tables["service_plans"]
    subscribers = silver_tables["subscribers"]
    devices = silver_tables["devices"]

    return [
        SilverEnrichmentResult(
            "network_events_enriched",
            enrich_network_events(
                silver_tables["network_events"],
                towers,
                regions,
                subscribers,
                devices,
                service_plans,
            ),
        ),
        SilverEnrichmentResult(
            "calls_enriched",
            enrich_calls(
                silver_tables["calls"],
                towers,
                regions,
                subscribers,
                devices,
                service_plans,
            ),
        ),
        SilverEnrichmentResult(
            "data_sessions_enriched",
            enrich_data_sessions(
                silver_tables["data_sessions"],
                towers,
                regions,
                subscribers,
                devices,
                service_plans,
            ),
        ),
        SilverEnrichmentResult(
            "tower_alarms_enriched",
            enrich_tower_alarms(silver_tables["tower_alarms"], towers, regions),
        ),
    ]
