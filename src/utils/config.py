"""Configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load a YAML configuration file."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)
