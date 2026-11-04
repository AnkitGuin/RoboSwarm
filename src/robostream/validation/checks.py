"""Independent physical validation (rejected, never silently repaired)."""
from __future__ import annotations
import math
from robostream.config import RobotProfile
from robostream.schemas import TelemetryRecord, ValidationIssue, is_finite


def issue(code: str, severity: str, message: str, rec: TelemetryRecord | None = None,
          field: str | None = None, observed: object | None = None) -> ValidationIssue:
    return ValidationIssue(issue_code=code, severity=severity, message=message,
                           event_id=str(rec.event_id) if rec else None,
                           robot_id=rec.robot_id if rec else None,
                           session_id=rec.session_id if rec else None,
                           task_id=rec.task_id if rec else None, field_name=field,
                           observed_value=str(observed)[:256] if observed is not None else None)


def physical_issues(rec: TelemetryRecord, profile: RobotProfile) -> list[ValidationIssue]:
    results = []
    fields = ("joint_positions", "joint_velocities", "end_effector_position",
              "end_effector_orientation", "gripper_position")
    for field in fields:
        value = getattr(rec, field)
        if not is_finite(value):
            results.append(issue("NON_FINITE_SENSOR_VALUE", "error", f"{field} contains NaN/Infinity",
                                 rec, field))
    if rec.joint_positions is not None:
        if len(rec.joint_positions) != profile.joint_count:
            results.append(issue("JOINT_COUNT_MISMATCH", "error", "Unexpected joint count", rec,
                                 "joint_positions", len(rec.joint_positions)))
        for value in rec.joint_positions:
            if math.isfinite(value) and not profile.joint_position_min <= value <= profile.joint_position_max:
                results.append(issue("JOINT_LIMIT_EXCEEDED", "error", "Joint position outside profile limits",
                                     rec, "joint_positions", value))
                break
    if rec.joint_velocities is not None:
        if len(rec.joint_velocities) != profile.joint_count:
            results.append(issue("JOINT_COUNT_MISMATCH", "error", "Unexpected velocity count", rec,
                                 "joint_velocities", len(rec.joint_velocities)))
        if any(math.isfinite(v) and abs(v) > profile.max_joint_velocity
               for v in rec.joint_velocities):
            results.append(issue("VELOCITY_LIMIT_EXCEEDED", "error", "Velocity exceeds profile limit",
                                 rec, "joint_velocities"))
    if rec.end_effector_position and any(math.isfinite(v) and not
        profile.workspace_min <= v <= profile.workspace_max for v in rec.end_effector_position):
        results.append(issue("WORKSPACE_LIMIT_EXCEEDED", "error", "End effector outside workspace",
                             rec, "end_effector_position"))
    if rec.end_effector_orientation and is_finite(rec.end_effector_orientation):
        norm = math.sqrt(sum(v * v for v in rec.end_effector_orientation))
        if abs(norm - 1) > profile.quaternion_tolerance:
            results.append(issue("INVALID_QUATERNION", "error", "Quaternion norm not close to 1",
                                 rec, "end_effector_orientation", round(norm, 5)))
    if rec.gripper_position is not None and math.isfinite(rec.gripper_position) and not (
        profile.gripper_min <= rec.gripper_position <= profile.gripper_max
    ):
        results.append(issue("GRIPPER_LIMIT_EXCEEDED", "error", "Gripper outside profile bounds",
                             rec, "gripper_position", rec.gripper_position))
    return results
