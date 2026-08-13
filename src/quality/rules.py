"""Reusable data quality rules for TowerIQ datasets."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


SUPPORTED_NETWORK_TYPES = ["4G", "5G", "LTE"]
SUPPORTED_REGION_TYPES = ["metro", "urban", "coastal"]
SUPPORTED_TOWER_TYPES = ["urban", "rural", "stadium", "airport", "business_district"]
SUPPORTED_TOWER_STATUSES = ["active", "maintenance", "retired"]
SUPPORTED_PLAN_TYPES = ["prepaid", "postpaid", "enterprise", "iot"]
SUPPORTED_SUBSCRIBER_STATUSES = ["active", "suspended", "churned"]
SUPPORTED_DEVICE_OS_TYPES = ["Android", "iOS"]
SUPPORTED_DEVICE_STATUSES = ["active", "inactive"]
SUPPORTED_NETWORK_EVENT_TYPES = [
    "connection_attempt",
    "handover",
    "signal_sample",
    "network_failure",
    "retry",
]
SUPPORTED_NETWORK_EVENT_STATUSES = ["success", "failed"]
SUPPORTED_CALL_TYPES = ["voice", "volte", "wifi_call"]
SUPPORTED_CALL_STATUSES = ["completed", "dropped", "failed"]
SUPPORTED_SESSION_STATUSES = ["completed", "failed", "timeout"]
SUPPORTED_ALARM_TYPES = [
    "POWER_FAILURE",
    "BACKHAUL_DOWN",
    "HIGH_TEMPERATURE",
    "EQUIPMENT_FAULT",
    "CONGESTION_ALERT",
]
SUPPORTED_ALARM_SEVERITIES = ["info", "warning", "major", "critical"]
SUPPORTED_ALARM_STATUSES = ["open", "resolved"]
MIN_SIGNAL_STRENGTH_DBM = -130
MAX_SIGNAL_STRENGTH_DBM = -40


def is_blank(column_name: str) -> Column:
    """Return true when a string column is null or empty after trimming."""
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def reason_when(condition: Column, reason: str) -> Column:
    """Return a rejection reason when a validation condition is true."""
    return F.when(condition, F.lit(reason))


def collect_rejection_reasons(*reason_columns: Column) -> Column:
    """Collect non-null rejection reasons into an array."""
    return F.filter(F.array(*reason_columns), lambda reason: reason.isNotNull())
