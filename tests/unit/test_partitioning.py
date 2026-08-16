from pathlib import Path

import pytest

from src.utils.partitioning import get_storage_path_size_bytes
from src.utils.partitioning import plan_output_partitions


def test_plan_output_partitions_uses_minimum_for_small_files():
    plan = plan_output_partitions(
        source_size_bytes=10 * 1024,
        strategy="auto",
        target_file_size_mb=128,
        min_partitions=2,
        max_partitions=64,
    )

    assert plan.planned_partitions == 2


def test_plan_output_partitions_uses_tiny_file_rule_before_minimum():
    plan = plan_output_partitions(
        source_size_bytes=10 * 1024,
        strategy="auto",
        tiny_file_threshold_mb=1,
        tiny_file_partitions=1,
        target_file_size_mb=128,
        min_partitions=2,
        max_partitions=64,
    )

    assert plan.planned_partitions == 1


def test_plan_output_partitions_scales_with_source_size():
    plan = plan_output_partitions(
        source_size_bytes=512 * 1024 * 1024,
        strategy="auto",
        target_file_size_mb=128,
        min_partitions=2,
        max_partitions=64,
    )

    assert plan.planned_partitions == 4


def test_plan_output_partitions_respects_maximum():
    plan = plan_output_partitions(
        source_size_bytes=20 * 1024 * 1024 * 1024,
        strategy="auto",
        target_file_size_mb=128,
        min_partitions=2,
        max_partitions=64,
    )

    assert plan.planned_partitions == 64


def test_plan_output_partitions_can_be_disabled():
    plan = plan_output_partitions(
        source_size_bytes=512 * 1024 * 1024,
        strategy="none",
        target_file_size_mb=128,
        min_partitions=2,
        max_partitions=64,
    )

    assert plan.planned_partitions is None


def test_plan_output_partitions_rejects_bad_strategy():
    with pytest.raises(ValueError, match="Unsupported partition strategy"):
        plan_output_partitions(source_size_bytes=1, strategy="manual")


def test_get_storage_path_size_bytes_reads_local_file_size(tmp_path):
    source_file = tmp_path / "source.csv"
    source_file.write_text("abc\n")

    assert get_storage_path_size_bytes(spark=None, path=str(source_file)) == 4


def test_get_storage_path_size_bytes_reads_local_directory_size(tmp_path):
    first = tmp_path / "first.csv"
    second = Path(tmp_path, "nested", "second.csv")
    second.parent.mkdir()
    first.write_text("abc\n")
    second.write_text("defgh\n")

    assert get_storage_path_size_bytes(spark=None, path=str(tmp_path)) == 10
