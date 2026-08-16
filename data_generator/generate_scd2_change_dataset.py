"""Generate TowerIQ raw data with deterministic SCD Type 2 dimension changes."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from data_generator.generate_messy_dataset import build_clean_tables
from data_generator.generate_tiny_dataset import write_csv
from data_generator.scenarios.scd2_changes import inject_scd2_dimension_changes


def generate(output_dir: Path) -> dict[str, int]:
    """Generate changed raw CSV files and return row counts by table."""
    tables = inject_scd2_dimension_changes(build_clean_tables())

    for table_name, rows in tables.items():
        write_csv(output_dir / f"{table_name}.csv", rows)

    return {table_name: len(rows) for table_name, rows in tables.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate TowerIQ data with SCD2 dimension changes.")
    parser.add_argument("--output-dir", default="data/raw/tiny_scd2", help="Directory for generated CSV files.")
    args = parser.parse_args()

    counts = generate(Path(args.output_dir))
    print(f"Generated SCD2 change dataset in {args.output_dir}")
    for table_name, row_count in counts.items():
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
