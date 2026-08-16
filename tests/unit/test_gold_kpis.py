from types import SimpleNamespace

from src.jobs.run_gold_kpis import ENRICHED_SILVER_TABLES, write_gold_table


def test_gold_job_reads_expected_enriched_silver_tables():
    assert ENRICHED_SILVER_TABLES == [
        "network_events_enriched",
        "calls_enriched",
        "data_sessions_enriched",
        "tower_alarms_enriched",
    ]


class FakeWriter:
    def __init__(self):
        self.mode_value = None
        self.partition_columns = None
        self.output_path = None

    def mode(self, value):
        self.mode_value = value
        return self

    def partitionBy(self, *columns):
        self.partition_columns = columns
        return self

    def parquet(self, output_path):
        self.output_path = output_path


class FakeDataFrame:
    def __init__(self):
        self.count_calls = 0
        self.write = FakeWriter()

    def count(self):
        self.count_calls += 1
        return 10


def test_write_gold_table_can_skip_output_count_for_fast_audit_mode(tmp_path):
    dataframe = FakeDataFrame()
    result = SimpleNamespace(table_name="tower_daily_kpis", dataframe=dataframe)

    job_result = write_gold_table(result, str(tmp_path), "tiny", count_outputs=False)

    assert dataframe.count_calls == 0
    assert job_result.output_count is None
    assert job_result.audit_count_enabled is False
    assert dataframe.write.mode_value == "overwrite"
    assert dataframe.write.partition_columns == ("event_date",)


def test_write_gold_table_counts_outputs_when_audit_mode_is_enabled(tmp_path):
    dataframe = FakeDataFrame()
    result = SimpleNamespace(table_name="tower_daily_kpis", dataframe=dataframe)

    job_result = write_gold_table(result, str(tmp_path), "tiny", count_outputs=True)

    assert dataframe.count_calls == 1
    assert job_result.output_count == 10
    assert job_result.audit_count_enabled is True
