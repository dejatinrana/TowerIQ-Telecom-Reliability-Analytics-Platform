from src.quality.rules import (
    MAX_SIGNAL_STRENGTH_DBM,
    MIN_SIGNAL_STRENGTH_DBM,
    SUPPORTED_ALARM_STATUSES,
    SUPPORTED_CALL_STATUSES,
    SUPPORTED_DEVICE_STATUSES,
    SUPPORTED_NETWORK_EVENT_STATUSES,
    SUPPORTED_NETWORK_EVENT_TYPES,
    SUPPORTED_NETWORK_TYPES,
    SUPPORTED_PLAN_TYPES,
    SUPPORTED_SESSION_STATUSES,
    SUPPORTED_SUBSCRIBER_STATUSES,
    SUPPORTED_TOWER_STATUSES,
)


def test_network_event_allowed_values_are_defined():
    assert SUPPORTED_NETWORK_TYPES == ["4G", "5G", "LTE"]
    assert "network_failure" in SUPPORTED_NETWORK_EVENT_TYPES
    assert SUPPORTED_NETWORK_EVENT_STATUSES == ["success", "failed"]


def test_signal_strength_bounds_are_realistic_for_generated_data():
    assert MIN_SIGNAL_STRENGTH_DBM == -130
    assert MAX_SIGNAL_STRENGTH_DBM == -40


def test_table_specific_allowed_values_are_defined():
    assert "active" in SUPPORTED_TOWER_STATUSES
    assert "postpaid" in SUPPORTED_PLAN_TYPES
    assert "suspended" in SUPPORTED_SUBSCRIBER_STATUSES
    assert "inactive" in SUPPORTED_DEVICE_STATUSES
    assert "dropped" in SUPPORTED_CALL_STATUSES
    assert "timeout" in SUPPORTED_SESSION_STATUSES
    assert "resolved" in SUPPORTED_ALARM_STATUSES
