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
