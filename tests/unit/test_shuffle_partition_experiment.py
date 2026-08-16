from experiments.shuffle_partition_tuning import render_markdown_report


def test_render_shuffle_partition_report_contains_partition_and_improvement(tmp_path):
    report = {
        "profile": "tiny_messy",
        "batch_id": "BATCH_TEST",
        "shuffle_partitions": 8,
        "previous_runtime_seconds": 68.487,
        "total_runtime_seconds": 40.0,
        "improvement_seconds": 28.487,
        "improvement_percent": 41.59,
        "stages": [{"stage_name": "quality", "runtime_seconds": 10.0}],
    }

    markdown = render_markdown_report(report, tmp_path / "result.json")

    assert "shuffle_partitions=8" in markdown
    assert "EXP-003 previous runtime: 68.487 seconds" in markdown
    assert "EXP-004 tuned runtime: 40.000 seconds" in markdown
    assert "41.59%" in markdown
