from types import SimpleNamespace

from src.jobs import run_silver_transformations
from src.jobs.run_silver_transformations import SOURCE_TABLE_BY_ENRICHED_TABLE, affected_enriched_tables
from src.transformations.silver_controls import SCHEMA_EVOLUTION_OPTIONAL_COLUMNS, SILVER_EVENT_SPECS


def test_enriched_tables_have_expected_source_tables():
    assert SOURCE_TABLE_BY_ENRICHED_TABLE == {
        "network_events_enriched": "network_events",
        "calls_enriched": "calls",
        "data_sessions_enriched": "data_sessions",
        "tower_alarms_enriched": "tower_alarms",
    }


def test_silver_event_specs_match_enriched_source_tables():
    assert {
        table_name: spec.source_table
        for table_name, spec in SILVER_EVENT_SPECS.items()
    } == SOURCE_TABLE_BY_ENRICHED_TABLE


def test_silver_event_specs_define_deduplication_and_late_arrival_controls():
    assert SILVER_EVENT_SPECS["network_events_enriched"].primary_key == "event_id"
    assert SILVER_EVENT_SPECS["calls_enriched"].primary_key == "call_id"
    assert SILVER_EVENT_SPECS["data_sessions_enriched"].primary_key == "session_id"
    assert SILVER_EVENT_SPECS["tower_alarms_enriched"].primary_key == "alarm_id"

    assert SILVER_EVENT_SPECS["network_events_enriched"].event_timestamp_column == "event_timestamp"
    assert SILVER_EVENT_SPECS["calls_enriched"].event_timestamp_column == "call_start_timestamp"
    assert SILVER_EVENT_SPECS["data_sessions_enriched"].event_timestamp_column == "session_start_timestamp"
    assert SILVER_EVENT_SPECS["tower_alarms_enriched"].event_timestamp_column == "alarm_timestamp"

    assert all(spec.late_arrival_threshold_hours == 24 for spec in SILVER_EVENT_SPECS.values())


def test_schema_evolution_optional_columns_are_known():
    assert set(SCHEMA_EVOLUTION_OPTIONAL_COLUMNS) == {
        "source_system",
        "producer_schema_version",
    }


def test_affected_enriched_tables_maps_event_sources_only():
    assert affected_enriched_tables(["network_events", "calls"]) == [
        "network_events_enriched",
        "calls_enriched",
    ]


def test_affected_enriched_tables_rebuilds_all_when_dimension_changes():
    assert affected_enriched_tables(["subscribers"]) == [
        "network_events_enriched",
        "calls_enriched",
        "data_sessions_enriched",
        "tower_alarms_enriched",
    ]


class FakeWriter:
    def __init__(self, owner):
        self.owner = owner

    def mode(self, value):
        self.owner.write_mode = value
        return self

    def partitionBy(self, *columns):
        self.owner.partition_columns = columns
        return self

    def parquet(self, path):
        self.owner.output_path = path


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriter(self)
        self.write_mode = None
        self.partition_columns = None
        self.output_path = None


def test_write_enriched_table_records_skipped_audit_and_partition_plan(monkeypatch, tmp_path):
    dataframe = FakeDataFrame()
    result = SimpleNamespace(table_name="network_events_enriched", dataframe=dataframe)

    monkeypatch.setattr(run_silver_transformations, "apply_output_partition_plan", lambda df, partitions: df)

    job_result = run_silver_transformations.write_enriched_table(
        result=result,
        paths={"silver": str(tmp_path)},
        profile="tiny",
        source_count=None,
        deduplicated_source_count=None,
        duplicates_removed=None,
        late_arriving_count=None,
        control_runtime_seconds=0.1,
        count_outputs=False,
        partition_config={
            "strategy": "auto",
            "tiny_file_threshold_mb": 1,
            "tiny_file_partitions": 1,
            "target_file_size_mb": 128,
            "min_partitions": 2,
            "max_partitions": 64,
        },
        source_size_bytes=10 * 1024,
    )

    assert job_result.audit_count_enabled is False
    assert job_result.input_count is None
    assert job_result.output_count is None
    assert job_result.planned_output_partitions == 1
    assert dataframe.write_mode == "append"
    assert dataframe.partition_columns == ("event_date", "_pipeline_batch_id")
