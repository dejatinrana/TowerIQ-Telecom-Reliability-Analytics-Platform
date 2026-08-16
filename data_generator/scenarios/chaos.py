"""Messy-data scenario injection for TowerIQ synthetic datasets.

The goal is to create deterministic records that exercise quality and
quarantine behavior without changing the clean baseline generator.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta


def with_duplicate(row: dict[str, object], **updates: object) -> dict[str, object]:
    """Copy an existing row and optionally override selected fields."""
    duplicate = deepcopy(row)
    duplicate.update(updates)
    return duplicate


def delayed_timestamp(timestamp_text: object, hours: int) -> str:
    """Return an ingestion timestamp delayed from an existing event timestamp."""
    event_time = datetime.strptime(str(timestamp_text), "%Y-%m-%d %H:%M:%S")
    return (event_time + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")


def trusted_context(tables: dict[str, list[dict[str, object]]]) -> tuple[set[object], set[object], set[object]]:
    """Find clean parent keys that remain valid after parent-dimension chaos."""
    valid_region_ids = {
        row["region_id"]
        for row in tables["regions"]
        if row["region_id"] != tables["regions"][0]["region_id"]
    }
    valid_plan_ids = {
        row["plan_id"]
        for row in tables["service_plans"]
        if row["plan_id"] != tables["service_plans"][0]["plan_id"]
    }
    valid_tower_ids = {
        row["tower_id"]
        for row in tables["towers"]
        if row["region_id"] in valid_region_ids
    }
    valid_subscriber_ids = {
        row["subscriber_id"]
        for row in tables["subscribers"]
        if row["home_region_id"] in valid_region_ids and row["plan_id"] in valid_plan_ids
    }
    valid_device_ids = {
        row["device_id"]
        for row in tables["devices"]
        if row["subscriber_id"] in valid_subscriber_ids
    }
    return valid_tower_ids, valid_subscriber_ids, valid_device_ids


def first_valid_fact(
    rows: list[dict[str, object]],
    valid_tower_ids: set[object],
    valid_subscriber_ids: set[object],
    valid_device_ids: set[object],
) -> dict[str, object]:
    """Find a fact row that should remain valid after cascading validation."""
    for row in rows:
        if (
            row["tower_id"] in valid_tower_ids
            and row["subscriber_id"] in valid_subscriber_ids
            and row["device_id"] in valid_device_ids
        ):
            return row
    raise ValueError("No valid fact row found for late-arrival injection.")


def first_valid_alarm(rows: list[dict[str, object]], valid_tower_ids: set[object]) -> dict[str, object]:
    """Find an alarm row that should remain valid after cascading validation."""
    for row in rows:
        if row["tower_id"] in valid_tower_ids:
            return row
    raise ValueError("No valid alarm row found for late-arrival injection.")


def inject_messy_records(tables: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    """Return a copy of the input tables with deterministic bad records added."""
    messy = {table_name: [deepcopy(row) for row in rows] for table_name, rows in tables.items()}
    valid_tower_ids, valid_subscriber_ids, valid_device_ids = trusted_context(tables)

    messy["regions"].extend(
        [
            with_duplicate(messy["regions"][0], region_name="Duplicate Region For Quarantine"),
            with_duplicate(
                messy["regions"][1],
                region_id="REG_BAD_TYPE",
                region_name="Unsupported Region Type",
                region_type="space_station",
            ),
            with_duplicate(
                messy["regions"][2],
                region_id="REG_MISSING_COUNTRY",
                country="",
            ),
        ]
    )

    messy["service_plans"].extend(
        [
            with_duplicate(messy["service_plans"][0], plan_name="Duplicate Plan For Quarantine"),
            with_duplicate(
                messy["service_plans"][1],
                plan_id="PLAN_BAD_TYPE",
                plan_type="lifetime_free",
            ),
            with_duplicate(
                messy["service_plans"][2],
                plan_id="PLAN_NEGATIVE_PRICE",
                monthly_price=-100,
            ),
        ]
    )

    messy["towers"].extend(
        [
            with_duplicate(messy["towers"][0], tower_name="Duplicate Tower For Quarantine"),
            with_duplicate(
                messy["towers"][1],
                tower_id="TOWER_UNKNOWN_REGION",
                region_id="REG_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["towers"][2],
                tower_id="TOWER_BAD_GEO",
                latitude=123.45,
                longitude=222.22,
            ),
            with_duplicate(
                messy["towers"][3],
                tower_id="TOWER_BAD_NETWORK",
                supported_networks="4G|SATELLITE",
            ),
        ]
    )

    messy["subscribers"].extend(
        [
            with_duplicate(messy["subscribers"][0], customer_segment="duplicate"),
            with_duplicate(
                messy["subscribers"][1],
                subscriber_id="SUB_UNKNOWN_REGION",
                home_region_id="REG_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["subscribers"][2],
                subscriber_id="SUB_UNKNOWN_PLAN",
                plan_id="PLAN_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["subscribers"][3],
                subscriber_id="SUB_BAD_STATUS",
                subscriber_status="paused_forever",
            ),
        ]
    )

    messy["devices"].extend(
        [
            with_duplicate(messy["devices"][0], model="Duplicate Device For Quarantine"),
            with_duplicate(
                messy["devices"][1],
                device_id="DEV_UNKNOWN_SUBSCRIBER",
                subscriber_id="SUB_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["devices"][2],
                device_id="DEV_BAD_OS",
                os_type="Symbian",
            ),
            with_duplicate(
                messy["devices"][3],
                device_id="DEV_BAD_RELEASE_YEAR",
                release_year=2035,
            ),
        ]
    )

    messy["network_events"].extend(
        [
            with_duplicate(messy["network_events"][0], error_code="DUPLICATE_EVENT"),
            with_duplicate(
                first_valid_fact(tables["network_events"], valid_tower_ids, valid_subscriber_ids, valid_device_ids),
                event_id="EVT_VALID_LATE_0001",
                ingestion_timestamp=delayed_timestamp(
                    first_valid_fact(
                        tables["network_events"],
                        valid_tower_ids,
                        valid_subscriber_ids,
                        valid_device_ids,
                    )["event_timestamp"],
                    49,
                ),
                error_code="",
            ),
            with_duplicate(
                messy["network_events"][1],
                event_id="EVT_UNKNOWN_TOWER",
                tower_id="TOWER_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["network_events"][2],
                event_id="EVT_BAD_NETWORK",
                network_type="6G",
            ),
            with_duplicate(
                messy["network_events"][3],
                event_id="EVT_BAD_LATENCY",
                latency_ms=-10,
            ),
            with_duplicate(
                messy["network_events"][4],
                event_id="EVT_BAD_TIME_ORDER",
                event_timestamp="2026-08-02 10:00:00",
                ingestion_timestamp="2026-08-02 09:59:00",
            ),
            with_duplicate(
                messy["network_events"][5],
                event_id="EVT_BAD_SIGNAL",
                signal_strength_dbm=-200,
            ),
        ]
    )

    messy["calls"].extend(
        [
            with_duplicate(messy["calls"][0], drop_reason="DUPLICATE_CALL"),
            with_duplicate(
                first_valid_fact(tables["calls"], valid_tower_ids, valid_subscriber_ids, valid_device_ids),
                call_id="CALL_VALID_LATE_0001",
                ingestion_timestamp=delayed_timestamp(
                    first_valid_fact(tables["calls"], valid_tower_ids, valid_subscriber_ids, valid_device_ids)[
                        "call_start_timestamp"
                    ],
                    50,
                ),
                call_status="completed",
                drop_reason="",
            ),
            with_duplicate(
                messy["calls"][1],
                call_id="CALL_UNKNOWN_DEVICE",
                device_id="DEV_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["calls"][2],
                call_id="CALL_BAD_TIME_ORDER",
                call_start_timestamp="2026-08-02 10:00:00",
                call_end_timestamp="2026-08-02 09:58:00",
            ),
            with_duplicate(
                messy["calls"][3],
                call_id="CALL_MISSING_DROP_REASON",
                call_status="dropped",
                drop_reason="",
            ),
            with_duplicate(
                messy["calls"][4],
                call_id="CALL_BAD_STATUS",
                call_status="abandoned",
            ),
        ]
    )

    messy["data_sessions"].extend(
        [
            with_duplicate(messy["data_sessions"][0], failure_reason="DUPLICATE_SESSION"),
            with_duplicate(
                first_valid_fact(tables["data_sessions"], valid_tower_ids, valid_subscriber_ids, valid_device_ids),
                session_id="SESS_VALID_LATE_0001",
                ingestion_timestamp=delayed_timestamp(
                    first_valid_fact(
                        tables["data_sessions"],
                        valid_tower_ids,
                        valid_subscriber_ids,
                        valid_device_ids,
                    )["session_start_timestamp"],
                    51,
                ),
                session_status="completed",
                failure_reason="",
            ),
            with_duplicate(
                messy["data_sessions"][1],
                session_id="SESS_UNKNOWN_SUBSCRIBER",
                subscriber_id="SUB_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["data_sessions"][2],
                session_id="SESS_BAD_TIME_ORDER",
                session_start_timestamp="2026-08-02 10:00:00",
                session_end_timestamp="2026-08-02 09:55:00",
            ),
            with_duplicate(
                messy["data_sessions"][3],
                session_id="SESS_NEGATIVE_BYTES",
                bytes_uploaded=-1,
            ),
            with_duplicate(
                messy["data_sessions"][4],
                session_id="SESS_MISSING_FAILURE_REASON",
                session_status="failed",
                failure_reason="",
            ),
        ]
    )

    messy["tower_alarms"].extend(
        [
            with_duplicate(messy["tower_alarms"][0], description="Duplicate Alarm For Quarantine"),
            with_duplicate(
                first_valid_alarm(tables["tower_alarms"], valid_tower_ids),
                alarm_id="ALARM_VALID_LATE_0001",
                ingestion_timestamp=delayed_timestamp(
                    first_valid_alarm(tables["tower_alarms"], valid_tower_ids)["alarm_timestamp"],
                    52,
                ),
            ),
            with_duplicate(
                messy["tower_alarms"][1],
                alarm_id="ALARM_UNKNOWN_TOWER",
                tower_id="TOWER_DOES_NOT_EXIST",
            ),
            with_duplicate(
                messy["tower_alarms"][2],
                alarm_id="ALARM_BAD_SEVERITY",
                severity="catastrophic",
            ),
            with_duplicate(
                messy["tower_alarms"][3],
                alarm_id="ALARM_BAD_TIME_ORDER",
                alarm_status="resolved",
                alarm_timestamp="2026-08-02 10:00:00",
                resolved_timestamp="2026-08-02 09:30:00",
            ),
            with_duplicate(
                messy["tower_alarms"][4],
                alarm_id="ALARM_MISSING_DESCRIPTION",
                description="",
            ),
        ]
    )

    return messy
