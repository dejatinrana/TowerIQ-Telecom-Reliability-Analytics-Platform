"""Folder-specific runner for Experiment 01 baseline measurements."""

from __future__ import annotations

import argparse

from experiments.baseline_pipeline_runtime import run_baseline_experiment


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Experiment 01 baseline runtime measurement.")
    parser.add_argument("--config", default="configs/local_fast.yaml", help="Path to YAML config.")
    parser.add_argument("--profile", default="medium_messy", help="Dataset profile to benchmark.")
    parser.add_argument("--batch-id", required=True, help="Unique batch ID for this baseline run.")
    parser.add_argument("--output-dir", default="experiments/01_baseline", help="Experiment output directory.")
    args = parser.parse_args()

    report = run_baseline_experiment(
        config_path=args.config,
        profile=args.profile,
        batch_id=args.batch_id,
        output_dir=args.output_dir,
    )
    print("Experiment 01 baseline completed")
    print(f"profile={report['profile']}, batch_id={report['batch_id']}")
    print(f"total_runtime_seconds={report['total_runtime_seconds']:.3f}")
    print(f"output={report['output_path']}")


if __name__ == "__main__":
    main()
