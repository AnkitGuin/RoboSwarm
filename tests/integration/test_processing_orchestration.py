"""Orchestration tests use a stub writer; they do NOT verify actual Parquet encoding."""
import json
import pytest

from robostream.database.session import session_factory
from robostream.database.models import Episode, PipelineRun
from robostream.generator.synthetic import GeneratorOptions, generate
from robostream.ingestion.service import ingest_file
from robostream.processing import run_pipeline


def test_pipeline_run_metadata_reports_and_replay(test_settings, tmp_path, monkeypatch):
    def stub_writer(path, records):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"PARQUET-WRITER-STUB")
        return path.stat().st_size

    monkeypatch.setattr("robostream.processing.write_episode", stub_writer)
    source = tmp_path / "gen"
    generate(source, GeneratorOptions(robots=1, sessions_per_robot=1,
                                     duration_seconds=2, frequency_hz=10,
                                     scenario="degraded", seed=3, defect_probability=0.1))
    with session_factory(test_settings)() as db:
        batch = ingest_file(source / "telemetry.jsonl", "test", db, test_settings)
        first = run_pipeline(batch["batch_id"], db, test_settings)
        assert first["status"] == "completed"
        assert first["episode_count"] >= 1
        assert first["output_count"] <= batch["accepted"]
        assert run_pipeline(batch["batch_id"], db, test_settings)["run_id"] == first["run_id"]
        forced = run_pipeline(batch["batch_id"], db, test_settings, force=True)
        assert forced["status"] == "completed"
        eps_a = db.query(Episode).filter_by(pipeline_run_id=first["run_id"]).all()
        eps_b = db.query(Episode).filter_by(pipeline_run_id=forced["run_id"]).all()
        assert {ep.episode_id for ep in eps_a} == {ep.episode_id for ep in eps_b}
        assert {ep.storage_uri for ep in eps_a} != {ep.storage_uri for ep in eps_b}
        report = json.loads((test_settings.data_dir / "reports" / f"{first['run_id']}.json").read_text())
        assert report["run_id"] == first["run_id"]
        assert "issue_counts_by_code" in report
        assert report["segmentation_seconds"] >= 0


def test_pipeline_failure_and_retry(test_settings, tmp_path, monkeypatch):
    source = tmp_path / "gen"
    generate(source, GeneratorOptions(robots=1, sessions_per_robot=1, duration_seconds=2,
                                     frequency_hz=5, scenario="clean"))
    with session_factory(test_settings)() as db:
        batch = ingest_file(source / "telemetry.jsonl", "test", db, test_settings)
        def bad(*args):
            raise OSError("simulated disk failure")
        monkeypatch.setattr("robostream.processing.write_episode", bad)
        with pytest.raises(OSError, match="disk failure"):
            run_pipeline(batch["batch_id"], db, test_settings)
        assert db.query(PipelineRun).filter_by(status="failed").count() == 1
        assert (test_settings.data_dir / db.get(__import__("robostream.database.models",
                                                         fromlist=["IngestBatch"]).IngestBatch,
                                                batch["batch_id"]).raw_path).is_file()
        def stub(path, records):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"stub")
            return 4
        monkeypatch.setattr("robostream.processing.write_episode", stub)
        completed = run_pipeline(batch["batch_id"], db, test_settings)
        assert completed["status"] == "completed"
