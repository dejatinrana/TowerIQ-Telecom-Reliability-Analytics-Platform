from src.ingestion.schemas import RAW_SCHEMAS


def test_expected_raw_tables_have_schemas():
    assert set(RAW_SCHEMAS) == {
        "regions",
        "towers",
        "service_plans",
        "subscribers",
        "devices",
        "network_events",
        "calls",
        "data_sessions",
        "tower_alarms",
    }


def test_network_events_schema_uses_explicit_types():
    fields = {field.name: field.dataType.simpleString() for field in RAW_SCHEMAS["network_events"].fields}

    assert fields["event_timestamp"] == "timestamp"
    assert fields["ingestion_timestamp"] == "timestamp"
    assert fields["signal_strength_dbm"] == "int"
    assert fields["latency_ms"] == "int"
