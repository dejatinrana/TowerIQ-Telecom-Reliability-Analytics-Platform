from src.jobs.run_silver_transformations import SOURCE_TABLE_BY_ENRICHED_TABLE
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
