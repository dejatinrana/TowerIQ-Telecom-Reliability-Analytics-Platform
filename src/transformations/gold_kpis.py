"""Gold KPI transformations for TowerIQ telecom analytics."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class GoldKpiResult:
    """One Gold KPI table."""

    table_name: str
    dataframe: DataFrame


def safe_rate(numerator: Column, denominator: Column) -> Column:
    """Return a percentage rate, guarding against divide-by-zero."""
    return F.when(denominator > 0, F.round((numerator / denominator) * F.lit(100.0), 3)).otherwise(F.lit(0.0))


def build_network_event_tower_daily(network_events: DataFrame) -> DataFrame:
    """Aggregate network events by tower and date."""
    return (
        network_events.groupBy(
            "event_date",
            "tower_id",
            "region_id",
            "region_name",
            "zone",
            "tower_type",
            "capacity_score",
        )
        .agg(
            F.count("*").alias("total_network_events"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_network_events"),
            F.avg("latency_ms").alias("avg_network_latency_ms"),
            F.avg("signal_strength_dbm").alias("avg_signal_strength_dbm"),
        )
        .withColumn(
            "network_failure_rate",
            safe_rate(F.col("failed_network_events"), F.col("total_network_events")),
        )
    )


def build_calls_tower_daily(calls: DataFrame) -> DataFrame:
    """Aggregate calls by tower and date."""
    return (
        calls.groupBy("event_date", "tower_id")
        .agg(
            F.count("*").alias("total_calls"),
            F.sum(F.when(F.col("is_dropped_call"), 1).otherwise(0)).alias("dropped_calls"),
            F.sum(F.when(F.col("is_failed_call"), 1).otherwise(0)).alias("failed_calls"),
            F.avg("duration_seconds").alias("avg_call_duration_seconds"),
        )
        .withColumn("dropped_call_rate", safe_rate(F.col("dropped_calls"), F.col("total_calls")))
        .withColumn("failed_call_rate", safe_rate(F.col("failed_calls"), F.col("total_calls")))
    )


def build_sessions_tower_daily(data_sessions: DataFrame) -> DataFrame:
    """Aggregate data sessions by tower and date."""
    return (
        data_sessions.groupBy("event_date", "tower_id")
        .agg(
            F.count("*").alias("total_data_sessions"),
            F.sum(F.when(F.col("is_failed_session"), 1).otherwise(0)).alias("failed_data_sessions"),
            F.avg("latency_ms").alias("avg_session_latency_ms"),
            F.sum("total_mb").alias("total_data_mb"),
        )
        .withColumn("failed_session_rate", safe_rate(F.col("failed_data_sessions"), F.col("total_data_sessions")))
    )


def build_alarms_tower_daily(tower_alarms: DataFrame) -> DataFrame:
    """Aggregate tower alarms by tower and date."""
    return (
        tower_alarms.groupBy("event_date", "tower_id")
        .agg(
            F.count("*").alias("total_alarms"),
            F.sum(F.when(F.col("is_critical_alarm"), 1).otherwise(0)).alias("critical_alarms"),
        )
    )


def build_tower_daily_kpis(
    network_events: DataFrame,
    calls: DataFrame,
    data_sessions: DataFrame,
    tower_alarms: DataFrame,
) -> DataFrame:
    """Build daily tower reliability KPIs."""
    network_daily = build_network_event_tower_daily(network_events)
    call_daily = build_calls_tower_daily(calls)
    session_daily = build_sessions_tower_daily(data_sessions)
    alarm_daily = build_alarms_tower_daily(tower_alarms)

    joined = (
        network_daily.join(call_daily, ["event_date", "tower_id"], "left")
        .join(session_daily, ["event_date", "tower_id"], "left")
        .join(alarm_daily, ["event_date", "tower_id"], "left")
    )

    fill_zero_columns = [
        "total_calls",
        "dropped_calls",
        "failed_calls",
        "dropped_call_rate",
        "failed_call_rate",
        "total_data_sessions",
        "failed_data_sessions",
        "failed_session_rate",
        "total_data_mb",
        "total_alarms",
        "critical_alarms",
    ]
    filled = joined.fillna(0, subset=fill_zero_columns)

    health_penalty = (
        F.col("network_failure_rate") * F.lit(1.5)
        + F.col("dropped_call_rate") * F.lit(2.0)
        + F.col("failed_session_rate") * F.lit(1.5)
        + F.col("critical_alarms") * F.lit(5.0)
    )

    return (
        filled.withColumn("tower_health_score", F.greatest(F.lit(0.0), F.round(F.lit(100.0) - health_penalty, 3)))
        .withColumn("_gold_created_at", F.current_timestamp())
    )


def build_region_daily_kpis(tower_daily_kpis: DataFrame) -> DataFrame:
    """Build daily region-level reliability KPIs from tower KPIs."""
    return (
        tower_daily_kpis.groupBy("event_date", "region_id", "region_name", "zone")
        .agg(
            F.countDistinct("tower_id").alias("active_towers"),
            F.sum("total_network_events").alias("total_network_events"),
            F.sum("failed_network_events").alias("failed_network_events"),
            F.sum("total_calls").alias("total_calls"),
            F.sum("dropped_calls").alias("dropped_calls"),
            F.sum("total_data_sessions").alias("total_data_sessions"),
            F.sum("failed_data_sessions").alias("failed_data_sessions"),
            F.sum("total_data_mb").alias("total_data_mb"),
            F.sum("critical_alarms").alias("critical_alarms"),
            F.avg("tower_health_score").alias("avg_tower_health_score"),
        )
        .withColumn("network_failure_rate", safe_rate(F.col("failed_network_events"), F.col("total_network_events")))
        .withColumn("dropped_call_rate", safe_rate(F.col("dropped_calls"), F.col("total_calls")))
        .withColumn("failed_session_rate", safe_rate(F.col("failed_data_sessions"), F.col("total_data_sessions")))
        .withColumn("avg_tower_health_score", F.round("avg_tower_health_score", 3))
        .withColumn("total_data_mb", F.round("total_data_mb", 3))
        .withColumn("_gold_created_at", F.current_timestamp())
    )


def build_network_type_daily_kpis(network_events: DataFrame, data_sessions: DataFrame) -> DataFrame:
    """Build daily KPI table by network type."""
    event_daily = (
        network_events.groupBy("event_date", "network_type")
        .agg(
            F.count("*").alias("total_network_events"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_network_events"),
            F.avg("latency_ms").alias("avg_network_latency_ms"),
            F.avg("signal_strength_dbm").alias("avg_signal_strength_dbm"),
        )
        .withColumn("network_failure_rate", safe_rate(F.col("failed_network_events"), F.col("total_network_events")))
    )

    session_daily = (
        data_sessions.groupBy("event_date", "network_type")
        .agg(
            F.count("*").alias("total_data_sessions"),
            F.sum(F.when(F.col("is_failed_session"), 1).otherwise(0)).alias("failed_data_sessions"),
            F.avg("latency_ms").alias("avg_session_latency_ms"),
            F.sum("total_mb").alias("total_data_mb"),
        )
        .withColumn("failed_session_rate", safe_rate(F.col("failed_data_sessions"), F.col("total_data_sessions")))
    )

    return (
        event_daily.join(session_daily, ["event_date", "network_type"], "full")
        .fillna(0)
        .withColumn("total_data_mb", F.round("total_data_mb", 3))
        .withColumn("_gold_created_at", F.current_timestamp())
    )


def build_subscriber_segment_daily_kpis(calls: DataFrame, data_sessions: DataFrame) -> DataFrame:
    """Build daily KPI table by subscriber segment and plan type."""
    call_daily = (
        calls.groupBy("event_date", "customer_segment", "plan_type")
        .agg(
            F.count("*").alias("total_calls"),
            F.sum(F.when(F.col("is_dropped_call"), 1).otherwise(0)).alias("dropped_calls"),
            F.sum(F.when(F.col("is_failed_call"), 1).otherwise(0)).alias("failed_calls"),
        )
        .withColumn("dropped_call_rate", safe_rate(F.col("dropped_calls"), F.col("total_calls")))
        .withColumn("failed_call_rate", safe_rate(F.col("failed_calls"), F.col("total_calls")))
    )

    session_daily = (
        data_sessions.groupBy("event_date", "customer_segment", "plan_type")
        .agg(
            F.count("*").alias("total_data_sessions"),
            F.sum(F.when(F.col("is_failed_session"), 1).otherwise(0)).alias("failed_data_sessions"),
            F.sum("total_mb").alias("total_data_mb"),
        )
        .withColumn("failed_session_rate", safe_rate(F.col("failed_data_sessions"), F.col("total_data_sessions")))
    )

    return (
        call_daily.join(session_daily, ["event_date", "customer_segment", "plan_type"], "full")
        .fillna(0)
        .withColumn("total_data_mb", F.round("total_data_mb", 3))
        .withColumn("_gold_created_at", F.current_timestamp())
    )


def build_gold_kpis(enriched_silver_tables: dict[str, DataFrame]) -> list[GoldKpiResult]:
    """Build the first set of TowerIQ Gold KPI tables."""
    network_events = enriched_silver_tables["network_events_enriched"]
    calls = enriched_silver_tables["calls_enriched"]
    data_sessions = enriched_silver_tables["data_sessions_enriched"]
    tower_alarms = enriched_silver_tables["tower_alarms_enriched"]

    tower_daily = build_tower_daily_kpis(network_events, calls, data_sessions, tower_alarms)

    return [
        GoldKpiResult("tower_daily_kpis", tower_daily),
        GoldKpiResult("region_daily_kpis", build_region_daily_kpis(tower_daily)),
        GoldKpiResult("network_type_daily_kpis", build_network_type_daily_kpis(network_events, data_sessions)),
        GoldKpiResult("subscriber_segment_daily_kpis", build_subscriber_segment_daily_kpis(calls, data_sessions)),
    ]
