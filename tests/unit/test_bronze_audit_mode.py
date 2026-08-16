from src.ingestion import bronze_ingestion


class FakeDataFrame:
    def __init__(self):
        self.count_calls = 0

    def count(self):
        self.count_calls += 1
        return 10


def test_ingest_table_skips_input_count_when_bronze_audit_is_disabled(monkeypatch):
    raw_df = FakeDataFrame()

    monkeypatch.setattr(bronze_ingestion, "read_raw_table", lambda *args, **kwargs: raw_df)
    monkeypatch.setattr(bronze_ingestion, "add_bronze_metadata", lambda df, *args, **kwargs: df)
    monkeypatch.setattr(bronze_ingestion, "write_bronze_table", lambda *args, **kwargs: "/tmp/bronze/table")

    result = bronze_ingestion.ingest_table(
        spark=None,
        raw_base_path="data/raw",
        bronze_base_path="data/bronze",
        profile="tiny",
        table_name="regions",
        count_inputs=False,
    )

    assert raw_df.count_calls == 0
    assert result.raw_count is None
    assert result.bronze_count is None
    assert result.audit_count_enabled is False


def test_ingest_table_counts_input_when_bronze_audit_is_enabled(monkeypatch):
    raw_df = FakeDataFrame()

    monkeypatch.setattr(bronze_ingestion, "read_raw_table", lambda *args, **kwargs: raw_df)
    monkeypatch.setattr(bronze_ingestion, "add_bronze_metadata", lambda df, *args, **kwargs: df)
    monkeypatch.setattr(bronze_ingestion, "write_bronze_table", lambda *args, **kwargs: "/tmp/bronze/table")

    result = bronze_ingestion.ingest_table(
        spark=None,
        raw_base_path="data/raw",
        bronze_base_path="data/bronze",
        profile="tiny",
        table_name="regions",
        count_inputs=True,
    )

    assert raw_df.count_calls == 1
    assert result.raw_count == 10
    assert result.bronze_count == 10
    assert result.audit_count_enabled is True


def test_ingest_table_passes_planned_output_partitions_to_writer(monkeypatch, tmp_path):
    raw_df = FakeDataFrame()
    captured = {}
    raw_profile_path = tmp_path / "tiny"
    raw_profile_path.mkdir()
    source_file = raw_profile_path / "regions.csv"
    source_file.write_text("region_id,region_name\nR001,North\n")

    monkeypatch.setattr(bronze_ingestion, "read_raw_table", lambda *args, **kwargs: raw_df)
    monkeypatch.setattr(bronze_ingestion, "add_bronze_metadata", lambda df, *args, **kwargs: df)

    def fake_write(*args, **kwargs):
        captured["output_partitions"] = kwargs["output_partitions"]
        return "/tmp/bronze/table"

    monkeypatch.setattr(bronze_ingestion, "write_bronze_table", fake_write)

    result = bronze_ingestion.ingest_table(
        spark=None,
        raw_base_path=str(tmp_path),
        bronze_base_path="data/bronze",
        profile="tiny",
        table_name="regions",
        count_inputs=False,
        partition_config={
            "strategy": "auto",
            "target_file_size_mb": 128,
            "min_partitions": 2,
            "max_partitions": 64,
        },
    )

    assert captured["output_partitions"] == 2
    assert result.source_size_bytes == source_file.stat().st_size
    assert result.planned_output_partitions == 2
