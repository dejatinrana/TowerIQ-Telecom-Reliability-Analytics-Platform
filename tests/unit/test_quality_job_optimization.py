from dataclasses import dataclass
from types import SimpleNamespace

from src.jobs import run_quality_checks


class FakeWriter:
    def __init__(self, owner):
        self.owner = owner

    def mode(self, value):
        self.owner.write_modes.append(value)
        return self

    def partitionBy(self, value):
        self.owner.write_partitions.append(value)
        return self

    def parquet(self, path):
        self.owner.write_paths.append(path)


class FakeDataFrame:
    def __init__(self, name):
        self.name = name
        self.count_calls = 0
        self.write_paths = []
        self.write_modes = []
        self.write_partitions = []
        self.persist_calls = 0
        self.unpersist_calls = 0

    @property
    def write(self):
        return FakeWriter(self)

    def count(self):
        self.count_calls += 1
        return 10

    def persist(self, *args, **kwargs):
        self.persist_calls += 1
        return self

    def unpersist(self):
        self.unpersist_calls += 1
        return self


def test_write_quality_result_skips_counts_when_quality_audit_is_disabled(monkeypatch):
    valid = FakeDataFrame("valid")
    invalid = FakeDataFrame("invalid")
    classified = FakeDataFrame("classified")
    result = run_quality_checks.TableQualityResult(
        table_name="regions",
        valid=valid,
        invalid=invalid,
        classified=classified,
    )

    monkeypatch.setattr(run_quality_checks, "apply_output_partition_plan", lambda df, partitions: df)

    job_result = run_quality_checks.write_quality_result(
        result=result,
        paths={"silver": "data/silver", "quarantine": "data/quarantine"},
        profile="tiny",
        validation_runtime_seconds=0.1,
        count_outputs=False,
        partition_config={
            "strategy": "auto",
            "target_file_size_mb": 128,
            "min_partitions": 1,
            "max_partitions": 64,
        },
        source_size_bytes=100,
    )

    assert classified.persist_calls == 0
    assert classified.unpersist_calls == 0
    assert valid.count_calls == 0
    assert invalid.count_calls == 0
    assert valid.write_paths
    assert invalid.write_paths
    assert job_result.bronze_count is None
    assert job_result.valid_count is None
    assert job_result.invalid_count is None
    assert job_result.audit_count_enabled is False
    assert job_result.planned_output_partitions == 1
    assert job_result.persisted_for_write is False


def test_write_quality_result_can_persist_classified_without_audit_counts(monkeypatch):
    valid = FakeDataFrame("valid")
    invalid = FakeDataFrame("invalid")
    classified = FakeDataFrame("classified")
    result = run_quality_checks.TableQualityResult(
        table_name="network_events",
        valid=valid,
        invalid=invalid,
        classified=classified,
    )

    monkeypatch.setattr(run_quality_checks, "apply_output_partition_plan", lambda df, partitions: df)

    job_result = run_quality_checks.write_quality_result(
        result=result,
        paths={"silver": "data/silver", "quarantine": "data/quarantine"},
        profile="tiny",
        validation_runtime_seconds=0.1,
        count_outputs=False,
        persist_for_write=True,
        partition_config={
            "strategy": "auto",
            "target_file_size_mb": 128,
            "min_partitions": 1,
            "max_partitions": 64,
        },
        source_size_bytes=100,
    )

    assert classified.persist_calls == 1
    assert classified.unpersist_calls == 1
    assert classified.count_calls == 0
    assert job_result.audit_count_enabled is False
    assert job_result.persisted_for_write is True


def test_required_quality_tables_includes_parent_dependencies():
    required = run_quality_checks.required_quality_tables({"network_events"})

    assert required == {
        "regions",
        "service_plans",
        "towers",
        "subscribers",
        "devices",
        "network_events",
    }


@dataclass(frozen=True)
class FakeJobResult:
    table_name: str


class FakeReader:
    def parquet(self, path):
        return FakeBronzeFrame(path)


class FakeSpark:
    read = FakeReader()


class FakeBronzeFrame:
    def __init__(self, path):
        self.path = path

    def filter(self, expression):
        return f"{self.path}:{expression}"


def test_run_quality_checks_writes_only_requested_affected_table(monkeypatch, tmp_path):
    config_path = tmp_path / "local.yaml"
    config_path.write_text(
        """
spark:
  app_name: TowerIQTest
  master: local[*]
  adaptive_query_execution: true
  use_pyspark_package: true
paths:
  bronze: data/bronze
  silver: data/silver
  quarantine: data/quarantine
audit:
  count_quality_outputs: false
quality:
  output_partitioning:
    strategy: auto
    target_file_size_mb: 128
    min_partitions: 1
    max_partitions: 64
""",
        encoding="utf-8",
    )
    validations = []
    writes = []

    def result(name):
        return SimpleNamespace(table_name=name, valid=f"valid_{name}", invalid=f"invalid_{name}", classified=None)

    monkeypatch.setattr(run_quality_checks, "get_storage_path_size_bytes", lambda *args, **kwargs: 100)
    monkeypatch.setattr(run_quality_checks, "persist_valid_parent", lambda value: value)
    monkeypatch.setattr(run_quality_checks, "unpersist_valid_parents", lambda values: None)
    monkeypatch.setattr(run_quality_checks, "write_quality_result", lambda result, **kwargs: writes.append(result.table_name) or FakeJobResult(result.table_name))
    monkeypatch.setattr(run_quality_checks, "validate_regions", lambda df: validations.append("regions") or result("regions"))
    monkeypatch.setattr(run_quality_checks, "validate_service_plans", lambda df: validations.append("service_plans") or result("service_plans"))
    monkeypatch.setattr(run_quality_checks, "validate_towers", lambda df, regions: validations.append("towers") or result("towers"))
    monkeypatch.setattr(
        run_quality_checks,
        "validate_subscribers",
        lambda df, regions, service_plans: validations.append("subscribers") or result("subscribers"),
    )
    monkeypatch.setattr(run_quality_checks, "validate_devices", lambda df, subscribers: validations.append("devices") or result("devices"))
    monkeypatch.setattr(
        run_quality_checks,
        "validate_network_events",
        lambda df, towers, subscribers, devices: validations.append("network_events") or result("network_events"),
    )

    results = run_quality_checks.run_quality_checks(
        config_path=str(config_path),
        profile="tiny",
        batch_id="BATCH_001",
        spark=FakeSpark(),
        table_names=["network_events"],
    )

    assert validations == ["regions", "service_plans", "towers", "subscribers", "devices", "network_events"]
    assert writes == ["network_events"]
    assert results == [FakeJobResult("network_events")]
