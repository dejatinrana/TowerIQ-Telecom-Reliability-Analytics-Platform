from data_generator.generate_messy_dataset import build_clean_tables, counts_for_profile
from data_generator.scenarios.chaos import inject_messy_records


def test_messy_scenario_adds_records_without_changing_clean_tables():
    clean = build_clean_tables()
    messy = inject_messy_records(clean)

    assert len(messy["regions"]) == len(clean["regions"]) + 3
    assert len(messy["towers"]) == len(clean["towers"]) + 4
    assert len(messy["service_plans"]) == len(clean["service_plans"]) + 3
    assert len(messy["subscribers"]) == len(clean["subscribers"]) + 4
    assert len(messy["devices"]) == len(clean["devices"]) + 4
    assert len(messy["network_events"]) == len(clean["network_events"]) + 7
    assert len(messy["calls"]) == len(clean["calls"]) + 6
    assert len(messy["data_sessions"]) == len(clean["data_sessions"]) + 6
    assert len(messy["tower_alarms"]) == len(clean["tower_alarms"]) + 6

    assert clean["regions"][0]["region_name"] != "Duplicate Region For Quarantine"


def test_messy_scenario_contains_quality_failures_for_core_rules():
    messy = inject_messy_records(build_clean_tables())

    assert any(row["network_type"] == "6G" for row in messy["network_events"])
    assert any(row["latency_ms"] < 0 for row in messy["network_events"])
    assert any(row["event_id"] == "EVT_VALID_LATE_0001" for row in messy["network_events"])
    assert any(row["call_id"] == "CALL_VALID_LATE_0001" for row in messy["calls"])
    assert any(row["session_id"] == "SESS_VALID_LATE_0001" for row in messy["data_sessions"])
    assert any(row["alarm_id"] == "ALARM_VALID_LATE_0001" for row in messy["tower_alarms"])
    assert any(row["device_id"] == "DEV_DOES_NOT_EXIST" for row in messy["calls"])
    assert any(row["bytes_uploaded"] < 0 for row in messy["data_sessions"])
    assert any(row["severity"] == "catastrophic" for row in messy["tower_alarms"])


def test_medium_messy_profile_counts_are_larger_than_tiny_profile_counts():
    medium_counts = counts_for_profile("medium_messy")

    assert medium_counts["network_events"] == 250000
    assert medium_counts["network_events"] > counts_for_profile("tiny_messy")["network_events"]
    assert medium_counts["data_sessions"] > counts_for_profile("tiny_messy")["data_sessions"]
