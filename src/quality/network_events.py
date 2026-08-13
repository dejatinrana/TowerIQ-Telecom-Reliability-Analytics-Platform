"""Compatibility wrapper for network event quality validation."""

from __future__ import annotations

from src.quality.table_validators import TableQualityResult, validate_network_events


NetworkEventQualityResult = TableQualityResult

__all__ = ["NetworkEventQualityResult", "validate_network_events"]
