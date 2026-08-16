from src.jobs import run_incremental_pipeline


def test_incremental_pipeline_skips_completed_batch(monkeypatch, tmp_path):
    config_path = tmp_path / "local.yaml"
    config_path.write_text(
        f"""
spark:
  app_name: TowerIQTest
  master: local[*]
  adaptive_query_execution: true
  use_pyspark_package: true
paths:
  raw: data/raw
  bronze: data/bronze
  silver: data/silver
  gold: data/gold
  quarantine: data/quarantine
incremental:
  registry_path: {tmp_path / "registry.json"}
""",
        encoding="utf-8",
    )

    calls = []

    def fail_if_called(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("Pipeline stages should not run for completed batch.")

    registry = run_incremental_pipeline.BatchRegistry(tmp_path / "registry.json")
    record = registry.start_run(profile="tiny", batch_id="BATCH_DONE")
    registry.complete_run(run_id=record.run_id, stages={})

    monkeypatch.setattr(run_incremental_pipeline, "run_bronze_ingestion", fail_if_called)

    result = run_incremental_pipeline.run_incremental_pipeline(
        config_path=str(config_path),
        profile="tiny",
        batch_id="BATCH_DONE",
    )

    assert result["status"] == "skipped"
    assert calls == []


def test_incremental_pipeline_force_rerun_executes_stages(monkeypatch, tmp_path):
    config_path = tmp_path / "local.yaml"
    config_path.write_text(
        f"""
spark:
  app_name: TowerIQTest
  master: local[*]
  adaptive_query_execution: true
  use_pyspark_package: true
paths:
  raw: data/raw
  bronze: data/bronze
  silver: data/silver
  gold: data/gold
  quarantine: data/quarantine
incremental:
  registry_path: {tmp_path / "registry.json"}
""",
        encoding="utf-8",
    )

    registry = run_incremental_pipeline.BatchRegistry(tmp_path / "registry.json")
    record = registry.start_run(profile="tiny", batch_id="BATCH_DONE")
    registry.complete_run(run_id=record.run_id, stages={})

    class FakeSpark:
        def __init__(self):
            self.stopped = False

        def stop(self):
            self.stopped = True

    fake_spark = FakeSpark()
    received_spark_ids = []

    def stage_result(**kwargs):
        received_spark_ids.append(id(kwargs["spark"]))
        return []

    monkeypatch.setattr(run_incremental_pipeline, "create_spark_session", lambda **kwargs: fake_spark)
    monkeypatch.setattr(run_incremental_pipeline, "run_bronze_ingestion", stage_result)
    monkeypatch.setattr(run_incremental_pipeline, "run_quality_checks", stage_result)
    monkeypatch.setattr(run_incremental_pipeline, "run_scd2_dimensions", stage_result)
    monkeypatch.setattr(run_incremental_pipeline, "run_silver_transformations", stage_result)
    monkeypatch.setattr(run_incremental_pipeline, "run_gold_kpis", stage_result)

    result = run_incremental_pipeline.run_incremental_pipeline(
        config_path=str(config_path),
        profile="tiny",
        batch_id="BATCH_DONE",
        force_rerun=True,
    )

    assert result["status"] == "completed"
    assert result["stages"] == {
        "bronze": [],
        "quality": [],
        "scd2": [],
        "silver": [],
        "gold": [],
    }
    assert received_spark_ids == [id(fake_spark)] * 5
    assert fake_spark.stopped
