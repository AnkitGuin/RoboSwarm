import json
import pytest
from robostream.database.session import session_factory
from robostream.database.models import EventIndex, IngestBatch, Issue, PipelineRun, Episode
from robostream.ingestion.service import ingest_file, ingest_records
from robostream.generator.synthetic import GeneratorOptions, generate
from robostream.processing import run_pipeline
from robostream.analytics.queries import sensor_summary


def test_file_ingestion_is_idempotent_and_isolates_bad_lines(tmp_path, test_settings):
    source = tmp_path / "src.jsonl"
    from tests.unit.test_schemas import raw
    value = raw()
    source.write_text(json.dumps(value) + "\n" + "{BROKEN JSON\n" +
                      json.dumps(value) + "\n")
    with session_factory(test_settings)() as db:
        first = ingest_file(source, "fixture", db, test_settings)
        repeated = ingest_file(source, "fixture", db, test_settings)
        assert first["accepted"] == 1
        assert first["rejected"] == 2
        assert first["duplicates"] == 1
        assert repeated["reused"] is True
        assert db.query(EventIndex).count() == 1
        assert db.query(IngestBatch).count() == 1
        assert db.query(Issue).count() == 2
        assert list((test_settings.data_dir / "raw").glob("*.jsonl"))
        assert list((test_settings.data_dir / "rejected").glob("*.jsonl"))


def test_api_style_batch_ingestion_and_cross_batch_duplicate(test_settings):
    from tests.unit.test_schemas import raw
    rec = raw()
    with session_factory(test_settings)() as db:
        first = ingest_records([rec], "api", db, test_settings)
        second = ingest_records([rec, raw()], "api", db, test_settings)
        assert first["accepted"] == 1
        assert second["accepted"] == 1
        assert second["duplicates"] == 1
        assert db.query(EventIndex).count() == 2


def test_full_pipeline_and_analytics(tmp_path, test_settings):
    pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")
    output = tmp_path / "gen"
    generate(output, GeneratorOptions(robots=1, sessions_per_robot=1,
                                       duration_seconds=2, frequency_hz=10, scenario="degraded",
                                       seed=1, defect_probability=0.15))
    with session_factory(test_settings)() as db:
        batch = ingest_file(output / "telemetry.jsonl", "synthetic", db, test_settings)
        first = run_pipeline(batch["batch_id"], db, test_settings)
        second = run_pipeline(batch["batch_id"], db, test_settings)
        assert first["run_id"] == second["run_id"]
        assert second["reused"] is True
        forced = run_pipeline(batch["batch_id"], db, test_settings, force=True)
        assert forced["run_id"] != first["run_id"]
        eps = db.query(Episode).filter(Episode.pipeline_run_id == first["run_id"]).all()
        assert eps
        assert db.query(Episode).filter(Episode.pipeline_run_id == forced["run_id"]).count() == len(eps)
        assert all((test_settings.data_dir / e.storage_uri).is_file() for e in eps)
        assert sensor_summary(test_settings.data_dir, [e.storage_uri for e in eps])
        from robostream.storage.parquet import read_episode
        assert read_episode(test_settings.data_dir, eps[0].storage_uri)["total"] > 0


def test_pipeline_failure_records_status_and_keeps_raw(tmp_path, test_settings, monkeypatch):
    pytest.importorskip("pyarrow")
    output = tmp_path / "gen"
    generate(output, GeneratorOptions(robots=1, sessions_per_robot=1,
                                       duration_seconds=2, frequency_hz=5, scenario="clean"))
    with session_factory(test_settings)() as db:
        batch = ingest_file(output / "telemetry.jsonl", "synthetic", db, test_settings)
        def broken_writer(*args):
            raise OSError("simulated full disk")
        monkeypatch.setattr("robostream.processing.write_episode", broken_writer)
        with pytest.raises(OSError, match="simulated"):
            run_pipeline(batch["batch_id"], db, test_settings)
        assert db.query(PipelineRun).filter_by(status="failed").count() == 1
        assert list((test_settings.data_dir / "raw").glob("*.jsonl"))
