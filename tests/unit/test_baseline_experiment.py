from experiments.baseline_pipeline_runtime import render_markdown_report


def test_render_markdown_report_contains_stage_runtime_and_evidence(tmp_path):
    report = {
        "profile": "tiny_messy",
        "batch_id": "BATCH_TEST",
        "total_runtime_seconds": 12.345,
        "stages": [
            {"stage_name": "bronze", "runtime_seconds": 1.23},
            {"stage_name": "quality", "runtime_seconds": 2.34},
        ],
    }

    markdown = render_markdown_report(report, tmp_path / "result.json")

    assert "# EXP-001 Baseline Pipeline Runtime" in markdown
    assert "| `bronze` | 1.230 |" in markdown
    assert "12.345 seconds" in markdown
    assert "result.json" in markdown
