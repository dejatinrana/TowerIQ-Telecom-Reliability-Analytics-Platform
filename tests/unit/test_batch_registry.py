from src.incremental.batch_registry import BatchRegistry, COMPLETED, FAILED, RUNNING


def test_batch_registry_tracks_completed_batches(tmp_path):
    registry = BatchRegistry(tmp_path / "batch_registry.json")

    record = registry.start_run(profile="tiny", batch_id="BATCH_001")

    assert record.status == RUNNING
    assert not registry.has_completed_batch(profile="tiny", batch_id="BATCH_001")

    completed = registry.complete_run(run_id=record.run_id, stages={"bronze": [{"table_name": "regions"}]})

    assert completed.status == COMPLETED
    assert registry.has_completed_batch(profile="tiny", batch_id="BATCH_001")
    assert not registry.has_completed_batch(profile="tiny_messy", batch_id="BATCH_001")


def test_batch_registry_tracks_failed_runs(tmp_path):
    registry = BatchRegistry(tmp_path / "batch_registry.json")

    record = registry.start_run(profile="tiny", batch_id="BATCH_002")
    failed = registry.fail_run(run_id=record.run_id, error_message="example failure")

    assert failed.status == FAILED
    assert failed.error_message == "example failure"
    assert not registry.has_completed_batch(profile="tiny", batch_id="BATCH_002")
