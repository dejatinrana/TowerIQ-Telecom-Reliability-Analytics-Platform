from src.jobs.run_silver_transformations import SOURCE_TABLE_BY_ENRICHED_TABLE


def test_enriched_tables_have_expected_source_tables():
    assert SOURCE_TABLE_BY_ENRICHED_TABLE == {
        "network_events_enriched": "network_events",
        "calls_enriched": "calls",
        "data_sessions_enriched": "data_sessions",
        "tower_alarms_enriched": "tower_alarms",
    }
