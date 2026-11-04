"""SQLAlchemy metadata: high-frequency samples live in Parquet, not this database."""
from __future__ import annotations
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5
from sqlalchemy import (DateTime, Float, ForeignKey, Index, Integer, JSON,
                        String, Text, UniqueConstraint)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def session_identity(robot_id: str, session_id: str) -> str:
    """Collision-safe stable key; neither field may change identity by containing ':' or '/' ."""
    return str(uuid5(NAMESPACE_URL, f"robostream-session:{len(robot_id)}:{robot_id}:{session_id}"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Robot(Base):
    __tablename__ = "robots"
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Session(Base):
    __tablename__ = "sessions"
    # session IDs are scoped by robot, represented as a composite portable key
    id: Mapped[str] = mapped_column(String(280), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    robot_id: Mapped[str] = mapped_column(ForeignKey("robots.id"), index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), default="active")
    source: Mapped[str] = mapped_column(String(128), default="synthetic")
    operator_id: Mapped[str | None] = mapped_column(String(128))


class IngestBatch(Base):
    __tablename__ = "ingest_batches"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    checksum: Mapped[str] = mapped_column(String(64), unique=True)
    source: Mapped[str] = mapped_column(String(128))
    raw_path: Mapped[str] = mapped_column(Text())  # internal relative path only
    total: Mapped[int] = mapped_column(Integer, default=0)
    accepted: Mapped[int] = mapped_column(Integer, default=0)
    rejected: Mapped[int] = mapped_column(Integer, default=0)
    duplicates: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class EventIndex(Base):
    __tablename__ = "event_index"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    batch_id: Mapped[str] = mapped_column(ForeignKey("ingest_batches.id"), index=True)
    line_number: Mapped[int] = mapped_column(Integer)
    robot_id: Mapped[str] = mapped_column(String(128), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    task_id: Mapped[str | None] = mapped_column(String(128))
    __table_args__ = (Index("ix_event_batch_line", "batch_id", "line_number"),)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    batch_id: Mapped[str] = mapped_column(ForeignKey("ingest_batches.id"), index=True)
    config_hash: Mapped[str] = mapped_column(String(16), index=True)
    pipeline_version: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), default="pending")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_reason: Mapped[str | None] = mapped_column(Text())
    report_path: Mapped[str | None] = mapped_column(Text())
    input_count: Mapped[int] = mapped_column(Integer, default=0)
    output_count: Mapped[int] = mapped_column(Integer, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, default=0)
    episode_count: Mapped[int] = mapped_column(Integer, default=0)
    duration_seconds: Mapped[float | None] = mapped_column(Float)
    __table_args__ = (Index("ix_run_batch_config_status", "batch_id", "config_hash", "status"),)


class Episode(Base):
    __tablename__ = "episodes"
    row_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    episode_id: Mapped[str] = mapped_column(String(36), index=True)
    pipeline_run_id: Mapped[str] = mapped_column(ForeignKey("pipeline_runs.id"), index=True)
    robot_id: Mapped[str] = mapped_column(ForeignKey("robots.id"), index=True)
    session_key: Mapped[str] = mapped_column(ForeignKey("sessions.id"), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    task_id: Mapped[str | None] = mapped_column(String(128), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float] = mapped_column(Float)
    record_count: Mapped[int] = mapped_column(Integer)
    sampling_rate_hz: Mapped[float] = mapped_column(Float)
    completion_status: Mapped[str] = mapped_column(String(32), index=True)
    quality_score: Mapped[float] = mapped_column(Float)
    quality_status: Mapped[str] = mapped_column(String(32), index=True)
    storage_uri: Mapped[str] = mapped_column(Text())
    quality_metrics: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (UniqueConstraint("pipeline_run_id", "episode_id"),)


class Issue(Base):
    __tablename__ = "issues"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_id: Mapped[str] = mapped_column(ForeignKey("ingest_batches.id"), index=True)
    pipeline_run_id: Mapped[str | None] = mapped_column(ForeignKey("pipeline_runs.id"), index=True)
    event_id: Mapped[str | None] = mapped_column(String(36), index=True)
    robot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    session_id: Mapped[str | None] = mapped_column(String(128), index=True)
    task_id: Mapped[str | None] = mapped_column(String(128))
    issue_code: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[str] = mapped_column(String(12), index=True)
    field_name: Mapped[str | None] = mapped_column(String(64))
    message: Mapped[str] = mapped_column(Text())
    observed_value: Mapped[str | None] = mapped_column(String(256))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
