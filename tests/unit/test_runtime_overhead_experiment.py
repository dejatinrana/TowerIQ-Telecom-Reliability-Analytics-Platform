from experiments.runtime_overhead_reduction import render_markdown_report


def test_render_runtime_overhead_report_contains_problem_solution_and_improvement(tmp_path):
    report = {
        "profile": "tiny_messy",
        "batch_id": "BATCH_TEST",
        "baseline_runtime_seconds": 58.404,
        "total_runtime_seconds": 40.0,
        "improvement_seconds": 18.404,
        "improvement_percent": 31.51,
        "stages": [{"stage_name": "bronze", "runtime_seconds": 3.0}],
    }

    markdown = render_markdown_report(report, tmp_path / "result.json")

    assert "## Problem" in markdown
    assert "## Solution" in markdown
    assert "EXP-001 baseline: 58.404 seconds" in markdown
    assert "EXP-002 optimized: 40.000 seconds" in markdown
    assert "31.51%" in markdown
