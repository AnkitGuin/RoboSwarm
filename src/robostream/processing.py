"""Batch processing: validation -> temporal checks -> segmentation -> Parquet -> SQL metadata."""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
import json
import logging
import shutil
import uuid
from hashlib import sha256

from sqlalchemy.orm import Session as DBSession

from robostream.config import PipelineConfig, RobotProfile, Settings
from robostream.database.models import (Episode, EventIndex, IngestBatch, Issue,
                                         PipelineRun, Session, session_identity)
from robostream.quality.scoring import assess
from robostream.schemas import TelemetryRecord, ValidationIssue
from robostream.segmentation.episodes import segment
from robostream.storage.files import atomic_json, atomic_bytes, ensure_data_dirs
from robostream.storage.parquet import write_episode
from robostream.validation.checks import physical_issues
from robostream.validation.temporal import temporal_issues

logger = logging.getLogger(__name__)


def run_view(run: PipelineRun) -> dict:
    return {"run_id": run.id, "batch_id": run.batch_id, "status": run.status,
            "config_hash": run.config_hash, "pipeline_version": run.pipeline_version,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "ended_at": run.ended_at.isoformat() if run.ended_at else None,
            "input_count": run.input_count, "output_count": run.output_count,
            "rejected_count": run.rejected_count, "episode_count": run.episode_count,
            "duration_seconds": run.duration_seconds, "failure_reason": run.failure_reason}


def run_pipeline(batch_id: str, db: DBSession, settings: Settings,
                 cfg: PipelineConfig | None = None, profile: RobotProfile | None = None,
                 *, force: bool = False) -> dict:
    cfg = cfg or PipelineConfig(gap_seconds=settings.gap_seconds,
                                expected_frequency_hz=settings.expected_frequency_hz)
    profile = profile or RobotProfile()
    config_hash = sha256((cfg.fingerprint + profile.model_dump_json()).encode()).hexdigest()[:16]
    batch = db.get(IngestBatch, batch_id)
    if batch is None:
        raise KeyError("Unknown ingestion batch")
    if not force:
        existing = db.query(PipelineRun).filter_by(batch_id=batch_id, config_hash=config_hash,
                                                   status="completed").order_by(PipelineRun.started_at.desc()).first()
        if existing:
            return run_view(existing) | {"reused": True}
    run_id = str(uuid.uuid4())
    run = PipelineRun(id=run_id, batch_id=batch_id, config_hash=config_hash,
                      pipeline_version=cfg.pipeline_version, status="running")
    db.add(run)
    db.commit()
    started = perf_counter()
    run_dir = settings.data_dir / "processed" / f"run={run_id}"
    try:
        ensure_data_dirs(settings.data_dir)
        # Accepted records are located by persistent event_index line_number, not by assuming
        # all input is valid. Raw input remains in its original, unmodified line ordering.
        t_validate = perf_counter()
        indexes = db.query(EventIndex.line_number).filter(EventIndex.batch_id == batch_id).all()
        accepted_lines = {line for (line,) in indexes}
        input_file = settings.data_dir / batch.raw_path
        valid: list[TelemetryRecord] = []
        errors: list[ValidationIssue] = []
        processing_rejections: list[str] = []
        with input_file.open("r", encoding="utf-8") as fh:
            for line_number, line in enumerate(fh, start=1):
                if line_number not in accepted_lines:
                    continue
                rec = TelemetryRecord.model_validate(json.loads(line))
                checks = physical_issues(rec, profile)
                errors.extend(checks)
                if any(i.severity == "error" for i in checks):
                    processing_rejections.append(json.dumps({"line": line_number,
                        "event_id": str(rec.event_id), "raw": line[:2048],
                        "codes": [i.issue_code for i in checks]}) + "\n")
                    continue
                valid.append(rec)
        errors.extend(temporal_issues(valid, cfg))
        ingest_issues = db.query(Issue).filter(Issue.batch_id == batch_id,
                                               Issue.pipeline_run_id.is_(None)).all()
        all_issues = errors + [ValidationIssue(issue_code=i.issue_code, severity=i.severity,
                                               event_id=i.event_id, robot_id=i.robot_id,
                                               session_id=i.session_id, task_id=i.task_id,
                                               message=i.message) for i in ingest_issues]
        validation_duration = perf_counter() - t_validate
        t_segment = perf_counter()
        segments = segment(valid, cfg, identity_hash=config_hash)
        segmentation_duration = perf_counter() - t_segment
        episode_models = []
        parquet_total_bytes = 0
        parquet_duration = 0.0
        for seg in segments:
            metrics = assess(seg, all_issues, cfg)
            # Keep the on-disk path compact for Windows MAX_PATH compatibility.
            # Robot/session/task identifiers remain indexed in SQL metadata.
            relative = (Path("processed") / f"run={run_id.replace('-', '')}" /
                        f"{seg.episode_id}.parquet")
            t0 = perf_counter()
            parquet_total_bytes += write_episode(settings.data_dir / relative, seg.records)
            parquet_duration += perf_counter() - t0
            model = Episode(
                row_id=str(uuid.uuid5(uuid.NAMESPACE_URL, run_id + seg.episode_id)),
                episode_id=seg.episode_id, pipeline_run_id=run_id,
                robot_id=seg.robot_id, session_id=seg.session_id,
                session_key=session_identity(seg.robot_id, seg.session_id), task_id=seg.task_id,
                started_at=seg.records[0].timestamp, ended_at=seg.records[-1].timestamp,
                duration_seconds=metrics["duration_seconds"], record_count=metrics["record_count"],
                sampling_rate_hz=metrics["sampling_rate_hz"],
                completion_status=seg.completion_status, quality_score=metrics["quality_score"],
                quality_status=metrics["quality_status"], storage_uri=relative.as_posix(),
                quality_metrics=metrics,
            )
            episode_models.append(model)
        for i in errors:
            db.add(Issue(batch_id=batch_id, pipeline_run_id=run_id, event_id=i.event_id,
                         robot_id=i.robot_id, session_id=i.session_id, task_id=i.task_id,
                         issue_code=i.issue_code, severity=i.severity,
                         field_name=i.field_name, message=i.message,
                         observed_value=i.observed_value))
        for episode in episode_models:
            db.add(episode)
        # Every session's derived status and times are persisted for robot/session explorer.
        per_session = defaultdict(list)
        for ep in episode_models:
            per_session[ep.session_key].append(ep)
        for key, eps in per_session.items():
            session = db.get(Session, key)
            if session:
                session.started_at = min(e.started_at for e in eps)
                session.ended_at = max(e.ended_at for e in eps)
                session.status = ("interrupted" if any(e.completion_status == "incomplete" for e in eps)
                                  else "failed" if any(e.completion_status == "failure" for e in eps)
                                  else "completed")
        counts = Counter(i.issue_code for i in all_issues)
        severities = Counter(i.severity for i in all_issues)
        quality_counts = Counter(ep.quality_status for ep in episode_models)
        duration = perf_counter() - started
        report = {"pipeline_version": cfg.pipeline_version, "config_hash": config_hash,
                  "run_id": run_id, "batch_id": batch_id, "input_count": batch.total,
                  "ingest_accepted": batch.accepted, "ingest_rejected": batch.rejected,
                  "processing_rejected": batch.accepted - len(valid),
                  "output_count": len(valid), "episode_count": len(segments),
                  "episode_counts_by_quality": dict(quality_counts),
                  "issue_counts_by_code": dict(counts),
                  "issue_counts_by_severity": dict(severities),
                  "quality_scores": [e.quality_score for e in episode_models],
                  "processing_seconds": duration, "validation_seconds": validation_duration,
                  "segmentation_seconds": segmentation_duration,
                  "parquet_write_seconds": parquet_duration,
                  "parquet_bytes": parquet_total_bytes, "segmentation_policy":
                  "task_id/start/terminal boundaries; inactivity split; sorted UTC event order"}
        rejected_rel = Path("rejected") / f"{run_id}.jsonl"
        if processing_rejections:
            atomic_bytes(settings.data_dir / rejected_rel,
                         "".join(processing_rejections).encode())
            report["processing_rejections_path"] = rejected_rel.as_posix()
        report_rel = Path("reports") / f"{run_id}.json"
        atomic_json(settings.data_dir / report_rel, report)
        run.status = "completed"
        run.ended_at = datetime.now(timezone.utc)
        run.duration_seconds = duration
        run.input_count = batch.total
        run.output_count = len(valid)
        run.rejected_count = batch.rejected + (batch.accepted - len(valid))
        run.episode_count = len(segments)
        run.report_path = report_rel.as_posix()
        db.commit()  # mark completed only after all Parquet and report writes succeed
        return run_view(run) | {"reused": False}
    except Exception as exc:
        logger.exception("Pipeline failed: %s", run_id)
        db.rollback()
        failed = db.get(PipelineRun, run_id)
        failed.status = "failed"
        failed.failure_reason = f"{type(exc).__name__}: {str(exc)[:300]}"
        failed.ended_at = datetime.now(timezone.utc)
        failed.duration_seconds = perf_counter() - started
        db.commit()
        # Roll back only uncommitted processed outputs; raw data is never deleted.
        shutil.rmtree(run_dir, ignore_errors=True)
        (settings.data_dir / "reports" / f"{run_id}.json").unlink(missing_ok=True)
        (settings.data_dir / "rejected" / f"{run_id}.jsonl").unlink(missing_ok=True)
        raise
