from pathlib import Path

import pytest

from src.jobs import run_dev_stage


def test_parse_tables_returns_none_for_empty_input():
    assert run_dev_stage.parse_tables(None) is None
    assert run_dev_stage.parse_tables("") is None


def test_parse_tables_splits_comma_separated_values():
    assert run_dev_stage.parse_tables("network_events, calls") == ["network_events", "calls"]


def test_dev_defaults_can_come_from_config(monkeypatch):
    calls = []

    class FakeSpark:
        def stop(self):
            calls.append(("stop",))

    config = {
        "spark": {
            "app_name": "TowerIQ",
            "master": "local[*]",
            "adaptive_query_execution": True,
            "use_pyspark_package": True,
            "shuffle_partitions": 8,
        },
        "paths": {
            "raw": "data/raw",
            "bronze": "data/bronze",
            "silver": "data/silver",
            "gold": "data/gold",
            "quarantine": "data/quarantine",
        },
        "dev_workflow": {
            "default_profile": "tiny_messy",
            "default_batch_id": "DEV_BATCH",
            "default_affected_tables": ["network_events"],
        },
    }

    monkeypatch.setattr(run_dev_stage, "load_config", lambda path: config)
    monkeypatch.setattr(run_dev_stage, "create_spark_session", lambda **kwargs: FakeSpark())
    monkeypatch.setattr(
        run_dev_stage,
        "run_quality_checks",
        lambda **kwargs: calls.append(("quality", kwargs["profile"], kwargs["batch_id"], kwargs["table_names"])) or [],
    )

    result = run_dev_stage.run_dev_stage(config_path="configs/dev_fast.yaml", stage="quality")

    assert result["profile"] == "tiny_messy"
    assert result["batch_id"] == "DEV_BATCH"
    assert result["affected_tables"] == ["network_events"]
    assert calls == [("quality", "tiny_messy", "DEV_BATCH", ["network_events"]), ("stop",)]


def test_gold_only_maps_affected_source_tables_to_enriched_tables(monkeypatch):
    calls = []

    class FakeSpark:
        def stop(self):
            calls.append(("stop",))

    config = {
        "spark": {
            "app_name": "TowerIQ",
            "master": "local[*]",
            "adaptive_query_execution": True,
            "use_pyspark_package": True,
            "shuffle_partitions": 8,
        },
        "paths": {
            "raw": "data/raw",
            "bronze": "data/bronze",
            "silver": "data/silver",
            "gold": "data/gold",
            "quarantine": "data/quarantine",
        },
        "dev_workflow": {
            "default_profile": "tiny_messy",
            "default_batch_id": "DEV_BATCH",
            "default_affected_tables": ["network_events"],
        },
    }

    monkeypatch.setattr(run_dev_stage, "load_config", lambda path: config)
    monkeypatch.setattr(run_dev_stage, "create_spark_session", lambda **kwargs: FakeSpark())
    monkeypatch.setattr(
        run_dev_stage,
        "run_gold_kpis",
        lambda **kwargs: calls.append(("gold", kwargs["enriched_table_names"])) or [],
    )

    result = run_dev_stage.run_dev_stage(config_path="configs/dev_fast.yaml", stage="gold")

    assert result["stage"] == "gold"
    assert calls == [("gold", ["network_events_enriched"]), ("stop",)]


def test_safe_remove_path_refuses_path_outside_project(tmp_path):
    outside_project = tmp_path / "outside"
    outside_project.mkdir()

    with pytest.raises(ValueError, match="outside project"):
        run_dev_stage.safe_remove_path(str(outside_project))


def test_safe_remove_path_removes_nested_project_output(tmp_path, monkeypatch):
    project = tmp_path / "TowerIQ"
    output = project / "data" / "gold" / "tiny"
    output.mkdir(parents=True)
    monkeypatch.chdir(project)

    run_dev_stage.safe_remove_path(str(output))

    assert not output.exists()
