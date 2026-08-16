"""SCD Type 2 change scenario for TowerIQ dimensions."""

from __future__ import annotations

from copy import deepcopy


CHANGE_EFFECTIVE_DATE = "2026-08-15"


def inject_scd2_dimension_changes(tables: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    """Return a copy of tables with deterministic dimension changes."""
    changed = {table_name: [deepcopy(row) for row in rows] for table_name, rows in tables.items()}

    changed["regions"][0].update(
        {
            "region_name": f"{changed['regions'][0]['region_name']} Priority",
            "zone": "West-Priority",
        }
    )
    changed["service_plans"][0].update(
        {
            "plan_name": "Prepaid Basic Refresh",
            "monthly_data_limit_gb": 30,
            "monthly_price": 249,
        }
    )
    changed["towers"][0].update(
        {
            "capacity_score": min(int(changed["towers"][0]["capacity_score"]) + 10, 100),
            "activation_date": CHANGE_EFFECTIVE_DATE,
            "tower_status": "maintenance",
        }
    )
    changed["subscribers"][0].update(
        {
            "plan_id": "PLAN_PREMIUM_5G",
            "activation_date": CHANGE_EFFECTIVE_DATE,
            "customer_segment": "professional",
        }
    )
    changed["devices"][0].update(
        {
            "model": f"{changed['devices'][0]['manufacturer']}_Model_Upgrade",
            "supports_5g": True,
            "device_status": "inactive",
        }
    )

    return changed
