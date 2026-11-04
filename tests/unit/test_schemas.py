from uuid import uuid4
import math
import pytest
from pydantic import ValidationError

from robostream.schemas import TelemetryRecord
from robostream.config import PipelineConfig, RobotProfile, load_config, load_profile
from robostream.validation.checks import physical_issues


def raw():
    return {"event_id": str(uuid4()), "robot_id": "r", "session_id": "s",
            "sequence_number": 0, "timestamp": "2026-01-01T06:00:00+05:30",
            "event_type": "observation", "joint_positions": [0.0] * 6,
            "joint_velocities": [0.0] * 6, "end_effector_orientation": [0, 0, 0, 1]}


def test_timezone_normalized():
    assert TelemetryRecord.model_validate(raw()).timestamp.isoformat() == "2026-01-01T00:30:00+00:00"


def test_naive_date_rejected():
    rec = raw() | {"timestamp": "2026-01-01T00:00:00"}
    with pytest.raises(ValidationError, match="timezone"):
        TelemetryRecord.model_validate(rec)


def test_missing_id_and_event_type_rules():
    rec = raw()
    del rec["event_id"]
    with pytest.raises(ValidationError):
        TelemetryRecord.model_validate(rec)
    with pytest.raises(ValidationError, match="task_id"):
        TelemetryRecord.model_validate(raw() | {"event_type": "task_event"})


def test_invalid_orientation_and_joint_count():
    with pytest.raises(ValidationError):
        TelemetryRecord.model_validate(raw() | {"end_effector_orientation": [0, 0, 1]})
    issues = physical_issues(TelemetryRecord.model_validate(raw() | {
        "joint_positions": [0.0], "end_effector_orientation": [0, 0, 0, 0.5]}), RobotProfile())
    assert {i.issue_code for i in issues} == {"JOINT_COUNT_MISMATCH", "INVALID_QUATERNION"}


def test_nonfinite_and_joint_limit():
    rec = TelemetryRecord.model_validate(raw() | {
        "joint_positions": [math.nan, 7, 0, 0, 0, 0]})
    codes = {i.issue_code for i in physical_issues(rec, RobotProfile())}
    assert "NON_FINITE_SENSOR_VALUE" in codes
    assert "JOINT_LIMIT_EXCEEDED" in codes


def test_config_hash_is_stable_and_changes_with_config(tmp_path):
    assert PipelineConfig().fingerprint == PipelineConfig().fingerprint
    assert PipelineConfig(gap_seconds=3).fingerprint != PipelineConfig().fingerprint
    (tmp_path / "config.yaml").write_text("gap_seconds: 2\n")
    (tmp_path / "profile.yaml").write_text("custom:\n  joint_count: 4\n")
    assert load_config(tmp_path / "config.yaml").gap_seconds == 2
    assert load_profile(tmp_path / "profile.yaml", "custom").joint_count == 4
