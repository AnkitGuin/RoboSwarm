"""Explainable, configurable heuristic (not validated demonstration fitness)."""
from __future__ import annotations
from collections import Counter
from statistics import median

from robostream.config import PipelineConfig
from robostream.schemas import ValidationIssue
from robostream.segmentation.episodes import Segment

SENSOR_FIELDS = ("joint_positions", "joint_velocities", "end_effector_position",
                 "end_effector_orientation", "gripper_position")
INVALID_CODES = {"NON_FINITE_SENSOR_VALUE", "JOINT_LIMIT_EXCEEDED", "JOINT_COUNT_MISMATCH",
                 "VELOCITY_LIMIT_EXCEEDED", "WORKSPACE_LIMIT_EXCEEDED", "INVALID_QUATERNION",
                 "GRIPPER_LIMIT_EXCEEDED", "SCHEMA_VALIDATION_ERROR"}
TEMPORAL_CODES = {"OUT_OF_ORDER_TIMESTAMP", "OUT_OF_ORDER_SEQUENCE", "TIMESTAMP_GAP",
                  "INCONSISTENT_SAMPLING_RATE", "DUPLICATE_TIMESTAMP"}


def assess(seg: Segment, issues: list[ValidationIssue], cfg: PipelineConfig) -> dict:
    records = seg.records
    count = len(records)
    # Episode boundaries and action/state messages are not sensor samples. Estimate
    # sampling metrics only from observations so simultaneous control events do not
    # create zero-length intervals or inflate the reported rate.
    observations = [r for r in records if r.event_type.value == "observation"]
    times = [(b.timestamp - a.timestamp).total_seconds()
             for a, b in zip(observations, observations[1:])]
    intervals = [dt for dt in times if dt > 0]
    duration = max(0.0, (records[-1].timestamp - records[0].timestamp).total_seconds())
    freq = 1 / median(intervals) if intervals else 0.0
    missing = {name: round(sum(getattr(r, name) is None for r in observations) / len(observations), 6)
               if observations else 0.0 for name in SENSOR_FIELDS}
    # Retain same-stream errors, including excluded invalid records, bounded by episode time and task.
    subset = [i for i in issues if i.robot_id == seg.robot_id and i.session_id == seg.session_id
              and (i.task_id is None or i.task_id == seg.task_id)]
    counts = Counter(i.issue_code for i in subset)
    duplicates = counts["DUPLICATE_EVENT_ID"] + counts["DUPLICATE_SEQUENCE_NUMBER"]
    temporal = sum(counts[code] for code in TEMPORAL_CODES)
    invalid = sum(counts[code] for code in INVALID_CODES)
    missing_rate = sum(missing.values()) / len(SENSOR_FIELDS)
    weights = cfg.quality
    # Each per-event penalty uses a per-episode normalized rate, not raw event count.
    penalties = {
        "missing": weights.missing_weight * missing_rate,
        "timestamp": weights.timestamp_weight * min(1, temporal / max(count, 1)),
        "duplicate": weights.duplicate_weight * min(1, duplicates / max(count, 1)),
        "invalid": weights.invalid_weight * min(1, invalid / max(count, 1)),
        "incomplete": weights.incomplete_penalty if seg.completion_status == "incomplete" else 0.0,
    }
    score = round(max(0, min(100, 100 - sum(penalties.values()))), 2)
    status = ("rejected" if score < weights.poor_threshold else
              "poor" if score < weights.acceptable_threshold else
              "acceptable" if score < weights.excellent_threshold else "excellent")
    if not observations:
        status = "rejected"  # structural hard failure: no telemetry observations
    flags = list(dict.fromkeys(seg.warnings + [code for code, n in counts.items() if n > 0]))
    return {
        "record_count": count, "duration_seconds": duration,
        "sampling_rate_hz": round(freq, 3), "missing_value_rates": missing,
        "duplicate_event_count": duplicates,
        "out_of_order_count": counts["OUT_OF_ORDER_TIMESTAMP"] + counts["OUT_OF_ORDER_SEQUENCE"],
        "timestamp_gap_count": counts["TIMESTAMP_GAP"],
        "max_timestamp_gap": max(times, default=0.0),
        "invalid_numerical_count": counts["NON_FINITE_SENSOR_VALUE"],
        "physical_limit_violation_count": sum(counts[c] for c in INVALID_CODES if c != "SCHEMA_VALIDATION_ERROR"),
        "has_terminal_event": seg.completion_status in TERMINAL_STATUSES,
        "completed_successfully": seg.completion_status == "success",
        "complete": seg.completion_status != "incomplete",
        "completion_status": seg.completion_status,
        "quality_score": score, "quality_status": status,
        "penalties": {k: round(v, 3) for k, v in penalties.items()},
        "quality_flags": flags, "issue_counts": dict(counts),
        "event_ids": [str(r.event_id) for r in records],
    }


TERMINAL_STATUSES = {"success", "failure", "aborted"}
