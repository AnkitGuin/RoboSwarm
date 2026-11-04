"""Audit-preserving JSONL ingestion and DB-backed global event-ID deduplication."""
from __future__ import annotations
from hashlib import sha256
import json
from pathlib import Path
from sqlalchemy.orm import Session as DBSession
from pydantic import ValidationError

from robostream.config import Settings
from robostream.database.models import EventIndex, IngestBatch, Issue, Robot, Session, session_identity
from robostream.schemas import TelemetryRecord, extract_issue_code
from robostream.storage.files import atomic_copy, atomic_bytes, ensure_data_dirs


def file_checksum(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_file(input_path: Path, source: str, db: DBSession, settings: Settings) -> dict:
    """Import a JSONL file incrementally; never modify source or original arrival ordering.

    Duplicate policy: first observed event_id across ALL batches is accepted; subsequent
    occurrences are rejected and recorded as DUPLICATE_EVENT_ID. Re-import of identical
    bytes reuses the original batch ID and does not create new event indexes.
    """
    ensure_data_dirs(settings.data_dir)
    if not input_path.is_file():
        raise FileNotFoundError(str(input_path))
    digest = file_checksum(input_path)
    existing = db.query(IngestBatch).filter(IngestBatch.checksum == digest).first()
    if existing:
        return batch_view(existing) | {"reused": True}
    batch_id = digest[:32]
    relative = Path("raw") / f"{digest}.jsonl"
    dest = settings.data_dir / relative
    if not dest.exists():
        atomic_copy(input_path, dest)  # persisted BEFORE attempting parsing/DB updates
    batch = IngestBatch(id=batch_id, checksum=digest, source=source,
                        raw_path=relative.as_posix())
    db.add(batch)
    db.flush()  # parent for FK indexes
    rejected_path = settings.data_dir / "rejected" / f"{batch_id}.jsonl"
    accepted, rejected, duplicates, count = 0, 0, 0, 0
    discovered_sessions: set[str] = set()
    discovered_robots: set[str] = set()
    known_event_ids: set[str] = set()
    # File created exclusively once. Each rejected line retains exact original text (bounded).
    with dest.open("r", encoding="utf-8", errors="replace") as stream, rejected_path.open(
        "x", encoding="utf-8"
    ) as bad:
        for line_number, line in enumerate(stream, start=1):
            count += 1
            parsed = None
            try:
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    raise ValueError("Telemetry line must be a JSON object")
                record = TelemetryRecord.model_validate(parsed)
            except (ValueError, ValidationError) as error:
                rejected += 1
                code = extract_issue_code(str(error))
                bad.write(json.dumps({"line": line_number, "raw": line[:2048],
                                      "code": code, "message": str(error)[:400]}) + "\n")
                db.add(Issue(batch_id=batch_id, issue_code=code, severity="error",
                             event_id=str(parsed.get("event_id", ""))[:36] if isinstance(parsed, dict) else None,
                             robot_id=str(parsed.get("robot_id", ""))[:128] if isinstance(parsed, dict) else None,
                             session_id=str(parsed.get("session_id", ""))[:128] if isinstance(parsed, dict) else None,
                             task_id=str(parsed.get("task_id", ""))[:128] if isinstance(parsed, dict) else None,
                             message=str(error)[:400], observed_value=f"line {line_number}"))
                continue
            event_id = str(record.event_id)
            if event_id in known_event_ids or db.get(EventIndex, event_id) is not None:
                duplicates += 1
                rejected += 1
                bad.write(json.dumps({"line": line_number, "raw": line[:2048],
                                      "code": "DUPLICATE_EVENT_ID"}) + "\n")
                db.add(Issue(batch_id=batch_id, issue_code="DUPLICATE_EVENT_ID",
                             severity="warning", event_id=event_id, robot_id=record.robot_id,
                             session_id=record.session_id, task_id=record.task_id,
                             message="Event ID previously ingested", observed_value=f"line {line_number}"))
                continue
            known_event_ids.add(event_id)
            accepted += 1
            if record.robot_id not in discovered_robots:
                if db.get(Robot, record.robot_id) is None:
                    db.add(Robot(id=record.robot_id))
                discovered_robots.add(record.robot_id)
            session_key = session_identity(record.robot_id, record.session_id)
            if session_key not in discovered_sessions:
                if db.get(Session, session_key) is None:
                    db.add(Session(id=session_key, robot_id=record.robot_id,
                                   session_id=record.session_id, source=source))
                discovered_sessions.add(session_key)
            db.add(EventIndex(event_id=event_id, batch_id=batch_id,
                              line_number=line_number, robot_id=record.robot_id,
                              session_id=record.session_id, task_id=record.task_id))
            if count % 1000 == 0:
                db.flush()
    batch.total = count
    batch.accepted = accepted
    batch.rejected = rejected
    batch.duplicates = duplicates
    db.commit()
    return batch_view(batch) | {"reused": False}


def ingest_records(records: list[dict], source: str, db: DBSession, settings: Settings) -> dict:
    if len(records) > settings.max_batch_size:
        raise ValueError(f"Batch too large (max {settings.max_batch_size})")
    ensure_data_dirs(settings.data_dir)
    # Stable serialization so submitting the same API payload is content-idempotent.
    content = "".join(json.dumps(record, sort_keys=True, separators=(",", ":"),
                                 allow_nan=True, default=str) + "\n" for record in records)
    digest = sha256(content.encode()).hexdigest()
    temporary = settings.data_dir / "raw" / f".upload-{digest}.jsonl"
    if not temporary.exists():
        atomic_bytes(temporary, content.encode())
    try:
        return ingest_file(temporary, source, db, settings)
    finally:
        temporary.unlink(missing_ok=True)


def batch_view(batch: IngestBatch) -> dict:
    return {"batch_id": batch.id, "source": batch.source, "total": batch.total,
            "accepted": batch.accepted, "rejected": batch.rejected,
            "duplicates": batch.duplicates, "created_at": batch.created_at.isoformat()}
