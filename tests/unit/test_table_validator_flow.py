from types import SimpleNamespace

from src.quality import table_validators


def test_validate_all_tables_uses_valid_parent_dimensions_for_references(monkeypatch):
    calls = []
    bronze_tables = {
        "regions": "bronze_regions",
        "service_plans": "bronze_service_plans",
        "towers": "bronze_towers",
        "subscribers": "bronze_subscribers",
        "devices": "bronze_devices",
        "network_events": "bronze_network_events",
        "calls": "bronze_calls",
        "data_sessions": "bronze_data_sessions",
        "tower_alarms": "bronze_tower_alarms",
    }

    def result(name):
        return SimpleNamespace(table_name=name, valid=f"valid_{name}", invalid=f"invalid_{name}")

    def fake_validate_regions(df):
        calls.append(("regions", df))
        return result("regions")

    def fake_validate_service_plans(df):
        calls.append(("service_plans", df))
        return result("service_plans")

    def fake_validate_towers(df, regions):
        calls.append(("towers", df, regions))
        return result("towers")

    def fake_validate_subscribers(df, regions, service_plans):
        calls.append(("subscribers", df, regions, service_plans))
        return result("subscribers")

    def fake_validate_devices(df, subscribers):
        calls.append(("devices", df, subscribers))
        return result("devices")

    def fake_validate_network_events(df, towers, subscribers, devices):
        calls.append(("network_events", df, towers, subscribers, devices))
        return result("network_events")

    def fake_validate_calls(df, towers, subscribers, devices):
        calls.append(("calls", df, towers, subscribers, devices))
        return result("calls")

    def fake_validate_data_sessions(df, towers, subscribers, devices):
        calls.append(("data_sessions", df, towers, subscribers, devices))
        return result("data_sessions")

    def fake_validate_tower_alarms(df, towers):
        calls.append(("tower_alarms", df, towers))
        return result("tower_alarms")

    monkeypatch.setattr(table_validators, "validate_regions", fake_validate_regions)
    monkeypatch.setattr(table_validators, "validate_service_plans", fake_validate_service_plans)
    monkeypatch.setattr(table_validators, "validate_towers", fake_validate_towers)
    monkeypatch.setattr(table_validators, "validate_subscribers", fake_validate_subscribers)
    monkeypatch.setattr(table_validators, "validate_devices", fake_validate_devices)
    monkeypatch.setattr(table_validators, "validate_network_events", fake_validate_network_events)
    monkeypatch.setattr(table_validators, "validate_calls", fake_validate_calls)
    monkeypatch.setattr(table_validators, "validate_data_sessions", fake_validate_data_sessions)
    monkeypatch.setattr(table_validators, "validate_tower_alarms", fake_validate_tower_alarms)

    results = table_validators.validate_all_tables(bronze_tables)

    assert [result.table_name for result in results] == [
        "regions",
        "service_plans",
        "towers",
        "subscribers",
        "devices",
        "network_events",
        "calls",
        "data_sessions",
        "tower_alarms",
    ]
    assert ("towers", "bronze_towers", "valid_regions") in calls
    assert ("subscribers", "bronze_subscribers", "valid_regions", "valid_service_plans") in calls
    assert ("devices", "bronze_devices", "valid_subscribers") in calls
    assert ("network_events", "bronze_network_events", "valid_towers", "valid_subscribers", "valid_devices") in calls
    assert ("tower_alarms", "bronze_tower_alarms", "valid_towers") in calls
