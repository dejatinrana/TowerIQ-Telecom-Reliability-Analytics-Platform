from src.jobs.run_gold_kpis import ENRICHED_SILVER_TABLES


def test_gold_job_reads_expected_enriched_silver_tables():
    assert ENRICHED_SILVER_TABLES == [
        "network_events_enriched",
        "calls_enriched",
        "data_sessions_enriched",
        "tower_alarms_enriched",
    ]
