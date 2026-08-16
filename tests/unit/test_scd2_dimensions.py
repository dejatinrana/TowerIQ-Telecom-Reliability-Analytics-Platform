from src.transformations.scd2 import DEFAULT_VALID_FROM, OPEN_ENDED_DATE, SCD2_DIMENSION_SPECS


def test_scd2_dimension_specs_cover_core_dimensions():
    assert set(SCD2_DIMENSION_SPECS) == {
        "regions",
        "service_plans",
        "towers",
        "subscribers",
        "devices",
    }


def test_scd2_dimension_specs_define_natural_keys_and_tracked_columns():
    assert SCD2_DIMENSION_SPECS["regions"].natural_key == "region_id"
    assert SCD2_DIMENSION_SPECS["service_plans"].natural_key == "plan_id"
    assert SCD2_DIMENSION_SPECS["towers"].natural_key == "tower_id"
    assert SCD2_DIMENSION_SPECS["subscribers"].natural_key == "subscriber_id"
    assert SCD2_DIMENSION_SPECS["devices"].natural_key == "device_id"

    assert "tower_status" in SCD2_DIMENSION_SPECS["towers"].tracked_columns
    assert "plan_id" in SCD2_DIMENSION_SPECS["subscribers"].tracked_columns
    assert "monthly_price" in SCD2_DIMENSION_SPECS["service_plans"].tracked_columns


def test_scd2_date_constants_are_stable():
    assert DEFAULT_VALID_FROM == "1900-01-01"
    assert OPEN_ENDED_DATE == "9999-12-31"
