from data_generator.generate_messy_dataset import build_clean_tables
from data_generator.scenarios.scd2_changes import CHANGE_EFFECTIVE_DATE, inject_scd2_dimension_changes


def test_scd2_change_scenario_changes_core_dimensions():
    clean = build_clean_tables()
    changed = inject_scd2_dimension_changes(clean)

    assert changed["regions"][0]["zone"] != clean["regions"][0]["zone"]
    assert changed["service_plans"][0]["monthly_price"] != clean["service_plans"][0]["monthly_price"]
    assert changed["towers"][0]["capacity_score"] != clean["towers"][0]["capacity_score"]
    assert changed["subscribers"][0]["plan_id"] != clean["subscribers"][0]["plan_id"]
    assert changed["devices"][0]["model"] != clean["devices"][0]["model"]


def test_scd2_change_scenario_sets_effective_dates_for_supported_dimensions():
    changed = inject_scd2_dimension_changes(build_clean_tables())

    assert changed["towers"][0]["activation_date"] == CHANGE_EFFECTIVE_DATE
    assert changed["subscribers"][0]["activation_date"] == CHANGE_EFFECTIVE_DATE
