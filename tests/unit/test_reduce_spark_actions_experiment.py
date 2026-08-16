from experiments.reduce_spark_actions import render_markdown_report


def test_render_reduce_spark_actions_report_contains_problem_solution_and_improvement(tmp_path):
    report = {
        "profile": "tiny_messy",
        "batch_id": "BATCH_TEST",
        "previous_best_runtime_seconds": 53.884,
        "total_runtime_seconds": 45.0,
        "improvement_seconds": 8.884,
        "improvement_percent": 16.49,
        "stages": [{"stage_name": "bronze", "runtime_seconds": 2.0}],
    }

    markdown = render_markdown_report(report, tmp_path / "result.json")

    assert "## Problem" in markdown
    assert "## Solution" in markdown
    assert "EXP-002 previous best: 53.884 seconds" in markdown
    assert "EXP-003 optimized: 45.000 seconds" in markdown
    assert "16.49%" in markdown
