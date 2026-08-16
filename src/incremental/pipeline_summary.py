"""Helpers for serializing pipeline job results into audit-friendly summaries."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any


def summarize_results(results: list[Any]) -> list[dict[str, Any]]:
    """Convert dataclass job results into JSON-serializable dictionaries."""
    summary = []
    for result in results:
        if is_dataclass(result):
            summary.append(asdict(result))
        elif isinstance(result, dict):
            summary.append(result)
        else:
            summary.append({"value": str(result)})
    return summary
