"""Pydantic v2 telemetry data contracts."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID
import math

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    observation = "observation"
    action = "action"
    state = "state"
    task_event = "task_event"


class TaskStatus(str, Enum):
    running = "running"
    success = "success"
    failure = "failure"
    aborted = "aborted"


class GripperState(str, Enum):
    open = "open"
    closed = "closed"
    moving = "moving"
    unknown = "unknown"


class TelemetryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    robot_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    timestamp: datetime
    sequence_number: int = Field(ge=0)
    event_type: EventType
    joint_positions: list[float] | None = None
    joint_velocities: list[float] | None = None
    end_effector_position: list[float] | None = None
    end_effector_orientation: list[float] | None = None
    gripper_position: float | None = None
    gripper_state: GripperState | None = None
    action: dict[str, Any] | None = None
    task_id: str | None = None
    task_status: TaskStatus | None = None
    camera_frame_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("INVALID_TIMESTAMP: timezone offset is required")
        return value.astimezone(timezone.utc)

    @field_validator("end_effector_position")
    @classmethod
    def check_position_dim(cls, value: list[float] | None) -> list[float] | None:
        if value is not None and len(value) != 3:
            raise ValueError("end_effector_position must have length 3")
        return value

    @field_validator("end_effector_orientation")
    @classmethod
    def check_orientation_dim(cls, value: list[float] | None) -> list[float] | None:
        if value is not None and len(value) != 4:
            raise ValueError("end_effector_orientation must have length 4")
        return value

    @model_validator(mode="after")
    def validate_event_payload(self) -> "TelemetryRecord":
        if self.event_type == EventType.task_event and self.task_id is None:
            raise ValueError("task_event requires task_id")
        if self.task_status is not None and self.event_type != EventType.task_event:
            raise ValueError("task_status only belongs on task_event")
        if self.event_type == EventType.action and self.action is None:
            raise ValueError("action events require action details")
        return self


class ValidationIssue(BaseModel):
    issue_code: str
    severity: str
    event_id: str | None = None
    robot_id: str | None = None
    session_id: str | None = None
    task_id: str | None = None
    field_name: str | None = None
    message: str
    observed_value: str | None = None
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IngestBatchRequest(BaseModel):
    source: str = Field(default="api", min_length=1, max_length=128)
    records: list[dict[str, Any]]


class PipelineRunRequest(BaseModel):
    batch_id: str
    force: bool = False


class Page(BaseModel):
    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


def extract_issue_code(error_message: str) -> str:
    if "INVALID_TIMESTAMP" in error_message or "timestamp" in error_message:
        return "INVALID_TIMESTAMP"
    if "Field required" in error_message:
        return "MISSING_REQUIRED_FIELD"
    if "nan" in error_message.lower() or "inf" in error_message.lower():
        return "NON_FINITE_SENSOR_VALUE"
    return "SCHEMA_VALIDATION_ERROR"


def is_finite(values: list[float] | float | None) -> bool:
    if values is None:
        return True
    if isinstance(values, (float, int)):
        return math.isfinite(values)
    return all(math.isfinite(value) for value in values)
