"""Measured local benchmarks: each repetition creates a new seeded dataset."""
from __future__ import annotations
import json
import platform
import statistics
from pathlib import Path
from uuid import uuid4
from time import perf_counter

from robostream.config import Settings
from robostream.database.session import session_factory
from robostream.generator.synthetic import generate, GeneratorOptions
from robostream.ingestion.service import ingest_file
from robostream.processing import run_pipeline
from robostream.analytics.queries import sensor_summary
from robostream.database.models import Episode
from robostream.storage.files import atomic_json


def benchmark_pipeline(record_counts: list[int], repeats: int, seed: int, output: Path,
                       settings: Settings) -> dict:
    if repeats < 1 or not record_counts or any(n < 10 for n in record_counts):
        raise ValueError("Positive repetitions and record counts >=10 required")
    measurements = []
    # Unique sample seeds prevent the persistent ingest index from turning
    # future benchmark runs into no-op reimports. Actual seeds are reported.
    benchmark_token = uuid4().hex
    seed_offset = int(benchmark_token[:6], 16)
    factory = session_factory(settings)
    for target in record_counts:
        for repeat in range(repeats):
            sample_seed = seed + repeat + 1000 * target + seed_offset
            directory = settings.data_dir / "generated" / f"bench_{benchmark_token}_{target}_{sample_seed}"
            directory.mkdir(parents=True, exist_ok=True)
            # Per-record generator overhead is measured separately from ingestion.
            options = GeneratorOptions(robots=1, sessions_per_robot=2,
                                       duration_seconds=target / 20,
                                       frequency_hz=20, tasks_per_session=2,
                                       scenario="clean", seed=sample_seed)
            start = perf_counter()
            manifest = generate(directory, options)
            generation_s = perf_counter() - start
            with factory() as db:
                start = perf_counter()
                batch = ingest_file(directory / "telemetry.jsonl", "benchmark", db, settings)
                ingestion_s = perf_counter() - start
                start = perf_counter()
                run = run_pipeline(batch["batch_id"], db, settings, force=True)
                processing_s = perf_counter() - start
                start = perf_counter()
                files = [ep.storage_uri for ep in db.query(Episode).filter_by(
                    pipeline_run_id=run["run_id"]).all()]
                analytics = sensor_summary(settings.data_dir, files)
                query_s = perf_counter() - start
                report = json.loads((settings.data_dir / "reports" /
                                     f"{run['run_id']}.json").read_text())
            actual = manifest["record_count"]
            measurements.append({"requested_records": target, "actual_records": actual,
                                 "seed": sample_seed, "generation_seconds": generation_s,
                                 "ingestion_seconds": ingestion_s,
                                 "ingestion_records_per_second": round(actual / max(ingestion_s, 1e-9), 1),
                                 "validation_seconds": report.get("validation_seconds"),
                                 "validation_records_per_second": round(batch["accepted"] /
                                     max(report["validation_seconds"], 1e-9), 1),
                                 "segmentation_seconds": report.get("segmentation_seconds"),
                                 "parquet_write_seconds": report["parquet_write_seconds"],
                                 "parquet_bytes": report["parquet_bytes"],
                                 "processing_seconds": processing_s,
                                 "processing_records_per_second": round(actual / max(processing_s, 1e-9), 1),
                                 "duckdb_query_seconds": query_s,
                                 "end_to_end_seconds": generation_s + ingestion_s + processing_s + query_s,
                                 "analytics_rows": len(analytics)})
    medians = {}
    for size in record_counts:
        rows = [r for r in measurements if r["requested_records"] == size]
        medians[str(size)] = {name: round(statistics.median([r[name] for r in rows]), 5)
                              for name in ("generation_seconds", "ingestion_seconds",
                                           "processing_seconds", "duckdb_query_seconds", "end_to_end_seconds")}
    result = {"environment": {"python": platform.python_version(),
                              "machine": platform.machine(), "platform": platform.platform()},
              "base_seed": seed, "run_token": benchmark_token, "repeats": repeats, "measurements": measurements, "medians": medians}
    atomic_json(output, result, overwrite=True)
    return result
