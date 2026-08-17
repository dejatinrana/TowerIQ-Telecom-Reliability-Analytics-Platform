"""Generate TowerIQ raw data with intentional quality problems."""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from data_generator.generate_tiny_dataset import (
    COUNTS,
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


MEDIUM_COUNTS = {
    "regions": 8,
    "towers": 240,
    "service_plans": 6,
    "subscribers": 25000,
    "devices": 30000,
    "network_events": 250000,
    "calls": 100000,
    "data_sessions": 150000,
    "tower_alarms": 4000,
}

PROFILE_COUNTS = {
    "tiny_messy": dict(COUNTS),
    "medium_messy": MEDIUM_COUNTS,
}


def profile_from_output_dir(output_dir: Path) -> str:
    """Infer the dataset profile name from the output directory."""
    return output_dir.name


def counts_for_profile(profile: str) -> dict[str, int]:
    """Return row-count settings for a supported messy dataset profile."""
    if profile not in PROFILE_COUNTS:
        supported = ", ".join(sorted(PROFILE_COUNTS))
        raise ValueError(f"Unsupported messy profile '{profile}'. Supported profiles: {supported}")
    return dict(PROFILE_COUNTS[profile])


def build_clean_tables(counts: dict[str, int] | None = None) -> dict[str, list[dict[str, object]]]:
    """Build deterministic clean tables for the requested profile size."""
    original_counts = dict(COUNTS)
    if counts is not None:
        COUNTS.update(counts)
    rng = random.Random(SEED)

    try:
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
    finally:
        COUNTS.clear()
        COUNTS.update(original_counts)


def generate(output_dir: Path, profile: str | None = None) -> dict[str, int]:
    """Generate messy raw CSV files and return row counts by table."""
    resolved_profile = profile or profile_from_output_dir(output_dir)
    tables = inject_messy_records(build_clean_tables(counts_for_profile(resolved_profile)))

    for table_name, rows in tables.items():
        write_csv(output_dir / f"{table_name}.csv", rows)

    return {table_name: len(rows) for table_name, rows in tables.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the messy TowerIQ raw dataset.")
    parser.add_argument("--output-dir", default="data/raw/tiny_messy", help="Directory for generated CSV files.")
    parser.add_argument(
        "--profile",
        default=None,
        help="Dataset profile size to generate. Defaults to the output directory name.",
    )
    args = parser.parse_args()

    counts = generate(Path(args.output_dir), profile=args.profile)
    print(f"Generated messy TowerIQ dataset in {args.output_dir}")
    for table_name, row_count in counts.items():
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
