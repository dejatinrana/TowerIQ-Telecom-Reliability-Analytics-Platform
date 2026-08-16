"""File-based batch registry for local incremental pipeline runs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


COMPLETED = "completed"
FAILED = "failed"
RUNNING = "running"


def utc_now_iso() -> str:
    """Return an ISO timestamp for audit metadata."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class BatchRunRecord:
    """Audit record for one pipeline batch run."""

    run_id: str
    profile: str
    batch_id: str
    status: str
    started_at: str
    completed_at: str | None = None
    failed_at: str | None = None
    stages: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None


class BatchRegistry:
    """Track processed batches in a local JSON registry."""

    def __init__(self, registry_path: str | Path) -> None:
        self.registry_path = Path(registry_path)

    def load(self) -> dict[str, Any]:
        """Load the registry file, returning an empty structure if missing."""
        if not self.registry_path.exists():
            return {"runs": []}
        with self.registry_path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def save(self, registry: dict[str, Any]) -> None:
        """Persist the registry file."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        with self.registry_path.open("w", encoding="utf-8") as file:
            json.dump(registry, file, indent=2, sort_keys=True)
            file.write("\n")

    def has_completed_batch(self, profile: str, batch_id: str) -> bool:
        """Return True if this profile and batch_id already completed."""
        return any(
            run["profile"] == profile and run["batch_id"] == batch_id and run["status"] == COMPLETED
            for run in self.load()["runs"]
        )

    def start_run(self, profile: str, batch_id: str) -> BatchRunRecord:
        """Create and save a running batch record."""
        record = BatchRunRecord(
            run_id=str(uuid4()),
            profile=profile,
            batch_id=batch_id,
            status=RUNNING,
            started_at=utc_now_iso(),
        )
        registry = self.load()
        registry["runs"].append(asdict(record))
        self.save(registry)
        return record

    def complete_run(self, run_id: str, stages: dict[str, Any]) -> BatchRunRecord:
        """Mark a run as completed and attach stage summaries."""
        return self._update_run(
            run_id=run_id,
            status=COMPLETED,
            stages=stages,
            completed_at=utc_now_iso(),
        )

    def fail_run(self, run_id: str, error_message: str, stages: dict[str, Any] | None = None) -> BatchRunRecord:
        """Mark a run as failed."""
        return self._update_run(
            run_id=run_id,
            status=FAILED,
            stages=stages or {},
            failed_at=utc_now_iso(),
            error_message=error_message,
        )

    def _update_run(self, run_id: str, **updates: Any) -> BatchRunRecord:
        registry = self.load()
        for run in registry["runs"]:
            if run["run_id"] == run_id:
                run.update(updates)
                self.save(registry)
                return BatchRunRecord(**run)
        raise ValueError(f"Run ID not found in batch registry: {run_id}")
