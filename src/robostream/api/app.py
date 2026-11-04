"""FastAPI v1: ingestion, synchronous local processing, metadata and Parquet queries."""
from __future__ import annotations
from functools import lru_cache
from uuid import uuid4
import json
import logging

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session as DBSession, sessionmaker

from robostream.config import Settings
from robostream.database.models import (Episode, IngestBatch, Issue, PipelineRun,
                                         Robot, Session)
from robostream.database.session import engine_for
from robostream.ingestion.service import ingest_records, batch_view
from robostream.processing import run_pipeline, run_view
from robostream.schemas import IngestBatchRequest, PipelineRunRequest
from robostream.storage.files import safe_relative_path
from robostream.storage.parquet import read_episode
from robostream.analytics.queries import sensor_summary

logger = logging.getLogger(__name__)
app = FastAPI(title="RoboStream", version="0.1.0",
              description="Robotics telemetry quality and episode inspection API")


def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=16)
def _factory(url: str):
    settings = Settings(database_url=url)
    return sessionmaker(engine_for(settings), expire_on_commit=False)


def get_db(settings: Settings = Depends(get_settings)):
    factory = _factory(settings.database_url)
    with factory() as db:
        yield db


@app.middleware("http")
async def request_id(request: Request, call_next):
    request_identifier = str(uuid4())
    if request.headers.get("content-length", "").isdigit():
        maximum = get_settings().max_upload_bytes
        if int(request.headers["content-length"]) > maximum:
            return JSONResponse({"error": {"code": "PAYLOAD_TOO_LARGE",
                                           "message": f"Request exceeds {maximum} bytes"}},
                                status_code=413, headers={"X-Request-ID": request_identifier})
    if request.method in {"POST", "PUT", "PATCH"}:
        body = await request.body()
        if len(body) > get_settings().max_upload_bytes:
            return JSONResponse({"error": {"code": "PAYLOAD_TOO_LARGE",
                                           "message": "Body exceeds upload size limit"}},
                                status_code=413, headers={"X-Request-ID": request_identifier})
    try:
        response = await call_next(request)
    except SQLAlchemyError:
        logger.exception("Database error request_id=%s", request_identifier)
        response = JSONResponse({"error": {"code": "DATABASE_UNAVAILABLE",
                                           "message": "Metadata database is unavailable"}}, status_code=503)
    except OSError:
        logger.exception("Storage error request_id=%s", request_identifier)
        response = JSONResponse({"error": {"code": "STORAGE_ERROR",
                                           "message": "Data storage is unavailable"}}, status_code=503)
    response.headers["X-Request-ID"] = request_identifier
    return response


def paginate(items: list[dict], limit: int, offset: int) -> dict:
    return {"items": items[offset:offset + limit], "total": len(items),
            "limit": limit, "offset": offset}


def episode_view(ep: Episode) -> dict:
    return {"episode_id": ep.episode_id, "pipeline_run_id": ep.pipeline_run_id,
            "robot_id": ep.robot_id, "session_id": ep.session_id, "task_id": ep.task_id,
            "started_at": ep.started_at.isoformat(), "ended_at": ep.ended_at.isoformat(),
            "duration_seconds": ep.duration_seconds, "record_count": ep.record_count,
            "sampling_rate_hz": ep.sampling_rate_hz,
            "completion_status": ep.completion_status, "quality_score": ep.quality_score,
            "quality_status": ep.quality_status}


def latest_episodes(db: DBSession) -> list[Episode]:
    # Reprocessing produces an independently traceable run and the same logical episode IDs.
    # The default explorer deduplicates by stable episode ID, exposing the newest revision.
    rows = db.query(Episode).join(PipelineRun, Episode.pipeline_run_id == PipelineRun.id).filter(
        PipelineRun.status == "completed").order_by(Episode.created_at.desc(),
                                                     Episode.pipeline_run_id.desc()).all()
    unique = {}
    for row in rows:
        unique.setdefault(row.episode_id, row)
    return list(unique.values())


def lookup_episode(db: DBSession, episode_id: str) -> Episode:
    rows = db.query(Episode).filter(Episode.episode_id == episode_id).order_by(
        Episode.created_at.desc(), Episode.pipeline_run_id.desc()).all()
    if not rows:
        raise HTTPException(404, detail={"code": "NOT_FOUND", "message": "Episode not found"})
    return rows[0]


@app.get("/health")
def health(db: DBSession = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ok", "service": "robostream", "version": "0.1.0"}


@app.get("/api/v1/robots")
def robots(limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
           db: DBSession = Depends(get_db)):
    rows = db.query(Robot).order_by(Robot.id).all()
    return paginate([{"robot_id": r.id, "created_at": r.created_at.isoformat()} for r in rows], limit, offset)


@app.get("/api/v1/robots/{robot_id}")
def robot(robot_id: str, db: DBSession = Depends(get_db)):
    row = db.get(Robot, robot_id)
    if not row:
        raise HTTPException(404, "Robot not found")
    count = db.query(Session).filter(Session.robot_id == robot_id).count()
    return {"robot_id": row.id, "session_count": count}


@app.get("/api/v1/sessions")
def sessions(robot_id: str | None = None, status: str | None = None,
             limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
             db: DBSession = Depends(get_db)):
    q = db.query(Session)
    if robot_id:
        q = q.filter(Session.robot_id == robot_id)
    if status:
        if status not in {"active", "completed", "interrupted", "failed"}:
            raise HTTPException(422, "Invalid session status")
        q = q.filter(Session.status == status)
    rows = q.order_by(Session.robot_id, Session.session_id).all()
    items = [{"robot_id": r.robot_id, "session_id": r.session_id,
              "started_at": r.started_at.isoformat() if r.started_at else None,
              "ended_at": r.ended_at.isoformat() if r.ended_at else None,
              "status": r.status, "source": r.source} for r in rows]
    return paginate(items, limit, offset)


@app.get("/api/v1/sessions/{session_id}")
def session(session_id: str, robot_id: str | None = None,
            db: DBSession = Depends(get_db)):
    query = db.query(Session).filter(Session.session_id == session_id)
    if robot_id:
        query = query.filter(Session.robot_id == robot_id)
    results = query.all()
    if len(results) != 1:
        raise HTTPException(404 if not results else 409, "Session not found or ambiguous; pass robot_id")
    r = results[0]
    return {"robot_id": r.robot_id, "session_id": r.session_id, "status": r.status,
            "started_at": r.started_at, "ended_at": r.ended_at}


@app.post("/api/v1/ingest/batch", status_code=201)
def ingest(payload: IngestBatchRequest, db: DBSession = Depends(get_db),
           settings: Settings = Depends(get_settings)):
    if len(payload.records) > settings.max_batch_size:
        raise HTTPException(413, {"code": "BATCH_TOO_LARGE", "max_batch_size": settings.max_batch_size})
    try:
        return ingest_records(payload.records, payload.source, db, settings)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/v1/ingest/batches/{batch_id}")
def batch_detail(batch_id: str, db: DBSession = Depends(get_db)):
    obj = db.get(IngestBatch, batch_id)
    if not obj:
        raise HTTPException(404, "Batch not found")
    return batch_view(obj)


@app.post("/api/v1/pipeline/runs", status_code=201)
def start_run(payload: PipelineRunRequest, db: DBSession = Depends(get_db),
              settings: Settings = Depends(get_settings)):
    try:
        return run_pipeline(payload.batch_id, db, settings, force=payload.force)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        logger.exception("Pipeline execution failed")
        raise HTTPException(500, {"code": "PIPELINE_FAILED", "message": "See run status and server logs"}) from exc


@app.get("/api/v1/pipeline/runs")
def runs(limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
         db: DBSession = Depends(get_db)):
    rows = db.query(PipelineRun).order_by(PipelineRun.started_at.desc()).all()
    return paginate([run_view(r) for r in rows], limit, offset)


@app.get("/api/v1/pipeline/runs/{run_id}")
def run_detail(run_id: str, db: DBSession = Depends(get_db)):
    run = db.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run_view(run)


@app.get("/api/v1/pipeline/runs/{run_id}/report")
def run_report(run_id: str, db: DBSession = Depends(get_db),
               settings: Settings = Depends(get_settings)):
    run = db.get(PipelineRun, run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    if not run.report_path:
        raise HTTPException(409, "Report not ready")
    path = (settings.data_dir / safe_relative_path(run.report_path)).resolve()
    if not path.is_relative_to(settings.data_dir.resolve()):
        raise HTTPException(400, "Unsafe storage reference")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise HTTPException(503, "Report unavailable or corrupt") from exc


@app.get("/api/v1/episodes")
def episodes(robot_id: str | None = None, session_id: str | None = None,
             task_id: str | None = None, quality_status: str | None = None,
             completion_status: str | None = None, start: str | None = None,
             end: str | None = None, pipeline_run_id: str | None = None,
             limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
             db: DBSession = Depends(get_db)):
    if quality_status and quality_status not in {"excellent", "acceptable", "poor", "rejected"}:
        raise HTTPException(422, "Invalid quality_status")
    if completion_status and completion_status not in {"success", "failure", "aborted", "incomplete"}:
        raise HTTPException(422, "Invalid completion_status")
    if pipeline_run_id:
        rows = db.query(Episode).filter(Episode.pipeline_run_id == pipeline_run_id).all()
    else:
        rows = latest_episodes(db)
    if robot_id:
        rows = [e for e in rows if e.robot_id == robot_id]
    if session_id:
        rows = [e for e in rows if e.session_id == session_id]
    if task_id:
        rows = [e for e in rows if e.task_id == task_id]
    if quality_status:
        rows = [e for e in rows if e.quality_status == quality_status]
    if completion_status:
        rows = [e for e in rows if e.completion_status == completion_status]
    if start or end:
        from datetime import datetime
        try:
            start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
            if (start_dt and start_dt.utcoffset() is None) or (end_dt and end_dt.utcoffset() is None):
                raise ValueError("Timezone required")
        except ValueError as exc:
            raise HTTPException(422, "Invalid UTC time range") from exc
        from datetime import timezone
        if start_dt:
            rows = [e for e in rows if e.started_at.replace(tzinfo=e.started_at.tzinfo or timezone.utc) >= start_dt]
        if end_dt:
            rows = [e for e in rows if e.started_at.replace(tzinfo=e.started_at.tzinfo or timezone.utc) <= end_dt]
    rows.sort(key=lambda e: (e.started_at, e.episode_id), reverse=True)
    return paginate([episode_view(e) for e in rows], limit, offset)


@app.get("/api/v1/episodes/{episode_id}")
def episode_detail(episode_id: str, db: DBSession = Depends(get_db)):
    ep = lookup_episode(db, episode_id)
    return episode_view(ep) | {"quality_metrics": ep.quality_metrics}


@app.get("/api/v1/episodes/{episode_id}/quality")
def episode_quality(episode_id: str, db: DBSession = Depends(get_db)):
    ep = lookup_episode(db, episode_id)
    return {"episode_id": ep.episode_id, **ep.quality_metrics}


@app.get("/api/v1/episodes/{episode_id}/telemetry")
def episode_telemetry(episode_id: str, limit: int = Query(500, ge=1, le=5000),
                      offset: int = Query(0, ge=0), db: DBSession = Depends(get_db),
                      settings: Settings = Depends(get_settings)):
    ep = lookup_episode(db, episode_id)
    try:
        return read_episode(settings.data_dir, ep.storage_uri, limit, offset)
    except Exception as exc:
        logger.exception("Unable to read episode Parquet episode_id=%s", episode_id)
        raise HTTPException(503, "Telemetry Parquet unavailable or corrupt") from exc


@app.get("/api/v1/issues")
def issues(run_id: str | None = None, batch_id: str | None = None,
           issue_code: str | None = None, severity: str | None = None,
           limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0),
           db: DBSession = Depends(get_db)):
    query = db.query(Issue)
    if run_id:
        query = query.filter(Issue.pipeline_run_id == run_id)
    if batch_id:
        query = query.filter(Issue.batch_id == batch_id)
    if issue_code:
        query = query.filter(Issue.issue_code == issue_code)
    if severity:
        if severity not in {"info", "warning", "error"}:
            raise HTTPException(422, "Invalid severity")
        query = query.filter(Issue.severity == severity)
    total = query.count()
    rows = query.order_by(Issue.id.desc()).offset(offset).limit(limit).all()
    return {"items": [{"issue_code": r.issue_code, "severity": r.severity,
                        "event_id": r.event_id, "robot_id": r.robot_id,
                        "session_id": r.session_id, "task_id": r.task_id,
                        "field_name": r.field_name, "message": r.message,
                        "observed_value": r.observed_value} for r in rows],
            "total": total, "limit": limit, "offset": offset}


@app.get("/api/v1/overview")
def overview(db: DBSession = Depends(get_db)):
    eps = latest_episodes(db)
    robots_n = db.query(func.count(Robot.id)).scalar() or 0
    sessions_n = db.query(func.count(Session.id)).scalar() or 0
    rejected_n = db.query(func.coalesce(func.sum(IngestBatch.rejected), 0)).scalar() or 0
    last = db.query(PipelineRun).order_by(PipelineRun.started_at.desc()).first()
    return {"robots": robots_n, "sessions": sessions_n, "episodes": len(eps),
            "processed_records": sum(e.record_count for e in eps),
            "rejected_ingestion_records": rejected_n,
            "high_quality_percent": round(100 * sum(e.quality_status == "excellent" for e in eps) /
                                          len(eps), 2) if eps else 0.0,
            "latest_run": run_view(last) if last else None}


@app.get("/api/v1/analytics/sensors")
def analytics(db: DBSession = Depends(get_db), settings: Settings = Depends(get_settings)):
    try:
        return {"items": sensor_summary(settings.data_dir, [e.storage_uri for e in latest_episodes(db)])}
    except Exception as exc:
        logger.exception("DuckDB query failed")
        raise HTTPException(503, "Analytical storage unavailable") from exc
