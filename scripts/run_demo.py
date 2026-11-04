"""End-to-end demo; requires migrated metadata database and project dependencies."""
from __future__ import annotations
import json
from uuid import uuid4

from robostream.config import Settings
from robostream.database.session import session_factory
from robostream.generator.synthetic import GeneratorOptions, generate
from robostream.ingestion.service import ingest_file
from robostream.processing import run_pipeline
from robostream.analytics.queries import sensor_summary
from robostream.database.models import Episode


def main():
    settings = Settings()
    out = settings.data_dir / "generated" / f"demo_{uuid4().hex[:8]}"
    manifest = generate(out, GeneratorOptions(robots=2, sessions_per_robot=2,
                                               duration_seconds=10, frequency_hz=10,
                                               seed=42, scenario="degraded"))
    with session_factory(settings)() as db:
        batch = ingest_file(out / "telemetry.jsonl", "synthetic-demo", db, settings)
        run = run_pipeline(batch["batch_id"], db, settings)
        eps = db.query(Episode).filter_by(pipeline_run_id=run["run_id"]).all()
        analysis = sensor_summary(settings.data_dir, [e.storage_uri for e in eps])
        print(json.dumps({"manifest": manifest, "batch": batch, "run": run,
                          "duckdb_sensor_summary": analysis}, indent=2))


if __name__ == "__main__":
    main()
