"""Generate TowerIQ raw data with intentional quality problems."""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from data_generator.generate_tiny_dataset import (
    SEED,
    generate_calls,
    generate_data_sessions,
    generate_devices,
    generate_network_events,
    generate_regions,
    generate_service_plans,
    generate_subscribers,
    generate_tower_alarms,
    generate_towers,
    write_csv,
)
from data_generator.scenarios.chaos import inject_messy_records


def build_clean_tables() -> dict[str, list[dict[str, object]]]:
    """Build the same deterministic clean tables used by the tiny generator."""
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

    return {
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


def generate(output_dir: Path) -> dict[str, int]:
    """Generate messy raw CSV files and return row counts by table."""
    tables = inject_messy_records(build_clean_tables())

    for table_name, rows in tables.items():
        write_csv(output_dir / f"{table_name}.csv", rows)

    return {table_name: len(rows) for table_name, rows in tables.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the messy TowerIQ raw dataset.")
    parser.add_argument("--output-dir", default="data/raw/tiny_messy", help="Directory for generated CSV files.")
    args = parser.parse_args()

    counts = generate(Path(args.output_dir))
    print(f"Generated messy TowerIQ dataset in {args.output_dir}")
    for table_name, row_count in counts.items():
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
