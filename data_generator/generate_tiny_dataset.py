"""Generate the first tiny TowerIQ raw telecom dataset.

This generator intentionally uses only the Python standard library so the first
dataset can be created before project dependencies are installed.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


SEED = 42
BASE_TIME = datetime(2026, 8, 1, 0, 0, 0)
BATCH_ID = "BATCH_TINY_20260801_001"
SCHEMA_VERSION = "v1"

COUNTS = {
    "regions": 8,
    "towers": 80,
    "service_plans": 6,
    "subscribers": 5000,
    "devices": 5500,
    "network_events": 50000,
    "calls": 20000,
    "data_sessions": 30000,
    "tower_alarms": 800,
}

NETWORK_TYPES = ["4G", "5G", "LTE"]
CITY_PROFILES = [
    ("Mumbai", "Maharashtra", "West", "metro"),
    ("Delhi", "Delhi", "North", "metro"),
    ("Bengaluru", "Karnataka", "South", "metro"),
    ("Hyderabad", "Telangana", "South", "metro"),
    ("Pune", "Maharashtra", "West", "urban"),
    ("Jaipur", "Rajasthan", "North", "urban"),
    ("Indore", "Madhya Pradesh", "Central", "urban"),
    ("Kochi", "Kerala", "South", "coastal"),
]


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def random_timestamp(rng: random.Random, days: int = 7) -> datetime:
    return BASE_TIME + timedelta(seconds=rng.randint(0, days * 24 * 60 * 60 - 1))


def iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def generate_regions() -> list[dict[str, object]]:
    rows = []
    for index, (city, state, zone, region_type) in enumerate(CITY_PROFILES, start=1):
        rows.append(
            {
                "region_id": f"REG_{index:03d}",
                "region_name": f"{city} {zone}",
                "country": "India",
                "state": state,
                "city": city,
                "zone": zone,
                "region_type": region_type,
                "timezone": "Asia/Kolkata",
                "is_active": True,
            }
        )
    return rows


def generate_towers(rng: random.Random, regions: list[dict[str, object]]) -> list[dict[str, object]]:
    tower_types = ["urban", "rural", "stadium", "airport", "business_district"]
    rows = []
    for index in range(1, COUNTS["towers"] + 1):
        region = rng.choice(regions)
        tower_type = rng.choices(tower_types, weights=[45, 25, 5, 5, 20], k=1)[0]
        capacity_score = {
            "rural": rng.randint(30, 60),
            "urban": rng.randint(55, 85),
            "business_district": rng.randint(75, 95),
            "airport": rng.randint(80, 98),
            "stadium": rng.randint(85, 100),
        }[tower_type]
        supports_5g = tower_type in {"urban", "business_district", "airport", "stadium"} and rng.random() < 0.8
        rows.append(
            {
                "tower_id": f"TOWER_{index:04d}",
                "region_id": region["region_id"],
                "tower_name": f"{region['city']}_Tower_{index:04d}",
                "city": region["city"],
                "latitude": round(rng.uniform(8.0, 29.0), 6),
                "longitude": round(rng.uniform(72.0, 88.0), 6),
                "tower_type": tower_type,
                "supported_networks": "4G|LTE|5G" if supports_5g else "4G|LTE",
                "capacity_score": capacity_score,
                "activation_date": (BASE_TIME.date() - timedelta(days=rng.randint(180, 3000))).isoformat(),
                "tower_status": rng.choices(["active", "maintenance"], weights=[96, 4], k=1)[0],
            }
        )
    return rows


def generate_service_plans() -> list[dict[str, object]]:
    return [
        {
            "plan_id": "PLAN_PREPAID_BASIC",
            "plan_name": "Prepaid Basic",
            "plan_type": "prepaid",
            "monthly_data_limit_gb": 20,
            "voice_limit_minutes": 1000,
            "priority_level": 1,
            "is_5g_enabled": False,
            "monthly_price": 199,
        },
        {
            "plan_id": "PLAN_PREPAID_PLUS",
            "plan_name": "Prepaid Plus",
            "plan_type": "prepaid",
            "monthly_data_limit_gb": 60,
            "voice_limit_minutes": 3000,
            "priority_level": 2,
            "is_5g_enabled": True,
            "monthly_price": 399,
        },
        {
            "plan_id": "PLAN_POSTPAID_STANDARD",
            "plan_name": "Postpaid Standard",
            "plan_type": "postpaid",
            "monthly_data_limit_gb": 100,
            "voice_limit_minutes": 5000,
            "priority_level": 3,
            "is_5g_enabled": True,
            "monthly_price": 699,
        },
        {
            "plan_id": "PLAN_PREMIUM_5G",
            "plan_name": "Premium 5G",
            "plan_type": "postpaid",
            "monthly_data_limit_gb": 250,
            "voice_limit_minutes": 10000,
            "priority_level": 4,
            "is_5g_enabled": True,
            "monthly_price": 999,
        },
        {
            "plan_id": "PLAN_ENTERPRISE",
            "plan_name": "Enterprise Priority",
            "plan_type": "enterprise",
            "monthly_data_limit_gb": 500,
            "voice_limit_minutes": 50000,
            "priority_level": 5,
            "is_5g_enabled": True,
            "monthly_price": 2499,
        },
        {
            "plan_id": "PLAN_IOT",
            "plan_name": "IoT Low Usage",
            "plan_type": "iot",
            "monthly_data_limit_gb": 5,
            "voice_limit_minutes": 0,
            "priority_level": 1,
            "is_5g_enabled": False,
            "monthly_price": 99,
        },
    ]


def generate_subscribers(
    rng: random.Random, regions: list[dict[str, object]], plans: list[dict[str, object]]
) -> list[dict[str, object]]:
    segments = ["consumer", "student", "professional", "enterprise", "iot"]
    rows = []
    for index in range(1, COUNTS["subscribers"] + 1):
        segment = rng.choices(segments, weights=[45, 15, 25, 10, 5], k=1)[0]
        possible_plans = plans if segment != "iot" else [plan for plan in plans if plan["plan_type"] == "iot"]
        plan = rng.choice(possible_plans)
        rows.append(
            {
                "subscriber_id": f"SUB_{index:06d}",
                "home_region_id": rng.choice(regions)["region_id"],
                "plan_id": plan["plan_id"],
                "activation_date": (BASE_TIME.date() - timedelta(days=rng.randint(1, 2500))).isoformat(),
                "subscriber_status": rng.choices(["active", "suspended", "churned"], weights=[94, 4, 2], k=1)[0],
                "customer_segment": segment,
                "age_band": rng.choice(["18-25", "26-35", "36-50", "51-65", "65+"]),
            }
        )
    return rows


def generate_devices(rng: random.Random, subscribers: list[dict[str, object]]) -> list[dict[str, object]]:
    manufacturers = ["Apple", "Samsung", "Xiaomi", "OnePlus", "Motorola", "Nokia"]
    os_by_maker = {"Apple": "iOS", "Samsung": "Android", "Xiaomi": "Android", "OnePlus": "Android", "Motorola": "Android", "Nokia": "Android"}
    rows = []
    for index in range(1, COUNTS["devices"] + 1):
        subscriber = subscribers[index - 1] if index <= len(subscribers) else rng.choice(subscribers)
        maker = rng.choice(manufacturers)
        release_year = rng.randint(2018, 2026)
        rows.append(
            {
                "device_id": f"DEV_{index:06d}",
                "subscriber_id": subscriber["subscriber_id"],
                "manufacturer": maker,
                "model": f"{maker}_Model_{rng.randint(1, 20)}",
                "os_type": os_by_maker[maker],
                "supports_5g": release_year >= 2021 and rng.random() < 0.75,
                "release_year": release_year,
                "device_status": rng.choices(["active", "inactive"], weights=[97, 3], k=1)[0],
            }
        )
    return rows


def choose_network_type(rng: random.Random, device: dict[str, object], tower: dict[str, object]) -> str:
    tower_networks = str(tower["supported_networks"]).split("|")
    if device["supports_5g"] and "5G" in tower_networks:
        return rng.choices(["5G", "4G", "LTE"], weights=[65, 25, 10], k=1)[0]
    return rng.choices(["4G", "LTE"], weights=[75, 25], k=1)[0]


def ingestion_time(rng: random.Random, event_time: datetime) -> datetime:
    delay_minutes = rng.choices([rng.randint(1, 15), rng.randint(60, 360)], weights=[96, 4], k=1)[0]
    return event_time + timedelta(minutes=delay_minutes)


def generate_network_events(
    rng: random.Random, subscribers: list[dict[str, object]], devices: list[dict[str, object]], towers: list[dict[str, object]]
) -> list[dict[str, object]]:
    devices_by_subscriber = {}
    for device in devices:
        devices_by_subscriber.setdefault(device["subscriber_id"], []).append(device)

    event_types = ["connection_attempt", "handover", "signal_sample", "network_failure", "retry"]
    rows = []
    for index in range(1, COUNTS["network_events"] + 1):
        subscriber = rng.choice(subscribers)
        device = rng.choice(devices_by_subscriber[subscriber["subscriber_id"]])
        tower = rng.choice(towers)
        event_time = random_timestamp(rng)
        event_type = rng.choices(event_types, weights=[35, 20, 30, 10, 5], k=1)[0]
        status = "failed" if event_type == "network_failure" or rng.random() < 0.035 else "success"
        error_code = rng.choice(["TIMEOUT", "WEAK_SIGNAL", "HANDOVER_FAILED", "NETWORK_BUSY"]) if status == "failed" else ""
        rows.append(
            {
                "event_id": f"EVT_{index:08d}",
                "subscriber_id": subscriber["subscriber_id"],
                "device_id": device["device_id"],
                "tower_id": tower["tower_id"],
                "event_timestamp": iso(event_time),
                "ingestion_timestamp": iso(ingestion_time(rng, event_time)),
                "event_type": event_type,
                "network_type": choose_network_type(rng, device, tower),
                "signal_strength_dbm": rng.randint(-120, -55),
                "latency_ms": rng.randint(15, 500 if status == "failed" else 160),
                "status": status,
                "error_code": error_code,
                "batch_id": BATCH_ID,
                "source_file": "network_events.csv",
                "schema_version": SCHEMA_VERSION,
            }
        )
    return rows


def generate_calls(
    rng: random.Random, subscribers: list[dict[str, object]], devices: list[dict[str, object]], towers: list[dict[str, object]]
) -> list[dict[str, object]]:
    devices_by_subscriber = {}
    for device in devices:
        devices_by_subscriber.setdefault(device["subscriber_id"], []).append(device)

    rows = []
    for index in range(1, COUNTS["calls"] + 1):
        subscriber = rng.choice(subscribers)
        device = rng.choice(devices_by_subscriber[subscriber["subscriber_id"]])
        tower = rng.choice(towers)
        start_time = random_timestamp(rng)
        duration = rng.randint(5, 3600)
        status = rng.choices(["completed", "dropped", "failed"], weights=[91, 6, 3], k=1)[0]
        rows.append(
            {
                "call_id": f"CALL_{index:08d}",
                "subscriber_id": subscriber["subscriber_id"],
                "device_id": device["device_id"],
                "tower_id": tower["tower_id"],
                "call_start_timestamp": iso(start_time),
                "call_end_timestamp": iso(start_time + timedelta(seconds=duration)),
                "duration_seconds": duration,
                "call_type": rng.choice(["voice", "volte", "wifi_call"]),
                "network_type": choose_network_type(rng, device, tower),
                "call_status": status,
                "drop_reason": rng.choice(["weak_signal", "handover_failed", "network_congestion", "tower_fault"]) if status != "completed" else "",
                "ingestion_timestamp": iso(ingestion_time(rng, start_time)),
                "batch_id": BATCH_ID,
                "source_file": "calls.csv",
                "schema_version": SCHEMA_VERSION,
            }
        )
    return rows


def generate_data_sessions(
    rng: random.Random, subscribers: list[dict[str, object]], devices: list[dict[str, object]], towers: list[dict[str, object]]
) -> list[dict[str, object]]:
    devices_by_subscriber = {}
    for device in devices:
        devices_by_subscriber.setdefault(device["subscriber_id"], []).append(device)

    rows = []
    for index in range(1, COUNTS["data_sessions"] + 1):
        subscriber = rng.choice(subscribers)
        device = rng.choice(devices_by_subscriber[subscriber["subscriber_id"]])
        tower = rng.choice(towers)
        start_time = random_timestamp(rng)
        duration = rng.randint(30, 7200)
        status = rng.choices(["completed", "failed", "timeout"], weights=[94, 4, 2], k=1)[0]
        rows.append(
            {
                "session_id": f"SESS_{index:08d}",
                "subscriber_id": subscriber["subscriber_id"],
                "device_id": device["device_id"],
                "tower_id": tower["tower_id"],
                "session_start_timestamp": iso(start_time),
                "session_end_timestamp": iso(start_time + timedelta(seconds=duration)),
                "duration_seconds": duration,
                "network_type": choose_network_type(rng, device, tower),
                "bytes_uploaded": rng.randint(1000, 50_000_000),
                "bytes_downloaded": rng.randint(10_000, 750_000_000),
                "latency_ms": rng.randint(15, 450 if status != "completed" else 140),
                "session_status": status,
                "failure_reason": rng.choice(["timeout", "network_busy", "weak_signal", "tower_unavailable"]) if status != "completed" else "",
                "ingestion_timestamp": iso(ingestion_time(rng, start_time)),
                "batch_id": BATCH_ID,
                "source_file": "data_sessions.csv",
                "schema_version": SCHEMA_VERSION,
            }
        )
    return rows


def generate_tower_alarms(rng: random.Random, towers: list[dict[str, object]]) -> list[dict[str, object]]:
    alarm_types = ["POWER_FAILURE", "BACKHAUL_DOWN", "HIGH_TEMPERATURE", "EQUIPMENT_FAULT", "CONGESTION_ALERT"]
    rows = []
    for index in range(1, COUNTS["tower_alarms"] + 1):
        tower = rng.choice(towers)
        alarm_time = random_timestamp(rng)
        severity = rng.choices(["info", "warning", "major", "critical"], weights=[20, 45, 25, 10], k=1)[0]
        status = rng.choices(["open", "resolved"], weights=[12, 88], k=1)[0]
        resolved_time = "" if status == "open" else iso(alarm_time + timedelta(minutes=rng.randint(10, 480)))
        rows.append(
            {
                "alarm_id": f"ALARM_{index:08d}",
                "tower_id": tower["tower_id"],
                "alarm_timestamp": iso(alarm_time),
                "alarm_type": rng.choice(alarm_types),
                "severity": severity,
                "alarm_status": status,
                "resolved_timestamp": resolved_time,
                "description": f"{severity} alarm reported by {tower['tower_id']}",
                "ingestion_timestamp": iso(ingestion_time(rng, alarm_time)),
                "batch_id": BATCH_ID,
                "source_file": "tower_alarms.csv",
                "schema_version": SCHEMA_VERSION,
            }
        )
    return rows


def generate(output_dir: Path) -> dict[str, int]:
    rng = random.Random(SEED)

    regions = generate_regions()
    towers = generate_towers(rng, regions)
    plans = generate_service_plans()
    subscribers = generate_subscribers(rng, regions, plans)
    devices = generate_devices(rng, subscribers)
    network_events = generate_network_events(rng, subscribers, devices, towers)
    calls = generate_calls(rng, subscribers, devices, towers)
    data_sessions = generate_data_sessions(rng, subscribers, devices, towers)
    tower_alarms = generate_tower_alarms(rng, towers)

    tables = {
        "regions": regions,
        "towers": towers,
        "service_plans": plans,
        "subscribers": subscribers,
        "devices": devices,
        "network_events": network_events,
        "calls": calls,
        "data_sessions": data_sessions,
        "tower_alarms": tower_alarms,
    }

    for table_name, rows in tables.items():
        write_csv(output_dir / f"{table_name}.csv", rows)

    return {table_name: len(rows) for table_name, rows in tables.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the tiny TowerIQ raw dataset.")
    parser.add_argument("--output-dir", default="data/raw/tiny", help="Directory for generated CSV files.")
    args = parser.parse_args()

    counts = generate(Path(args.output_dir))
    print(f"Generated tiny TowerIQ dataset in {args.output_dir}")
    for table_name, row_count in counts.items():
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
