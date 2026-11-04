"""Arrival-order anomalies and normalized-time anomalies are separately recorded."""
from __future__ import annotations
from collections import defaultdict
from statistics import median
from robostream.config import PipelineConfig
from robostream.schemas import TelemetryRecord, ValidationIssue
from robostream.validation.checks import issue


def temporal_issues(records: list[TelemetryRecord], cfg: PipelineConfig) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    streams: dict[tuple[str, str], list[TelemetryRecord]] = defaultdict(list)
    for rec in records:
        streams[(rec.robot_id, rec.session_id)].append(rec)
    for stream in streams.values():
        seen_terminal: dict[str, str] = {}
        for rec in stream:
            if rec.event_type.value == "task_event" and rec.task_status is not None and rec.task_status.value in {"success", "failure", "aborted"}:
                task_key = rec.task_id or ""
                prior_status = seen_terminal.get(task_key)
                if prior_status is not None and prior_status != rec.task_status.value:
                    issues.append(issue("CONFLICTING_TASK_STATUS", "warning",
                                        "Contradictory terminal task status", rec, "task_status"))
                seen_terminal[task_key] = rec.task_status.value
        seen_sequences: set[int] = set()
        last: TelemetryRecord | None = None
        for rec in stream:  # original arrival order, NOT sorted
            if rec.sequence_number in seen_sequences:
                issues.append(issue("DUPLICATE_SEQUENCE_NUMBER", "warning", "Repeated sequence number", rec,
                                    "sequence_number", rec.sequence_number))
            seen_sequences.add(rec.sequence_number)
            if last is not None:
                if rec.timestamp < last.timestamp:
                    issues.append(issue("OUT_OF_ORDER_TIMESTAMP", "warning", "Arrival time regressed", rec,
                                        "timestamp", rec.timestamp.isoformat()))
                if rec.sequence_number < last.sequence_number:
                    issues.append(issue("OUT_OF_ORDER_SEQUENCE", "warning", "Sequence number regressed", rec,
                                        "sequence_number", rec.sequence_number))
                if rec.sequence_number > last.sequence_number + 1:
                    issues.append(issue("MISSING_SEQUENCE_NUMBER", "info", "Sequence number gap", rec,
                                        "sequence_number", rec.sequence_number - last.sequence_number - 1))
            last = rec
        # Sampling cadence describes sensor observations, not control markers. A task-start
        # event commonly shares the first sample's timestamp, so including all event types
        # creates false duplicate timestamps and biases the estimated sample interval.
        ordered = sorted(
            (rec for rec in stream if rec.event_type.value == "observation"),
            key=lambda r: (r.timestamp, r.sequence_number, str(r.event_id)),
        )
        intervals = [(b.timestamp - a.timestamp).total_seconds()
                     for a, b in zip(ordered, ordered[1:])]
        positive = [dt for dt in intervals if dt > 0]
        typical = median(positive) if positive else 1 / cfg.expected_frequency_hz
        for rec, dt in zip(ordered[1:], intervals):
            if dt == 0:
                issues.append(issue("DUPLICATE_TIMESTAMP", "warning", "Equal event timestamp", rec, "timestamp"))
            if dt > cfg.gap_seconds:
                issues.append(issue("TIMESTAMP_GAP", "warning", "Inactivity gap in normalized event time",
                                    rec, "timestamp", dt))
            elif dt > 0 and abs(dt - typical) > max(typical, 1 / cfg.expected_frequency_hz) * cfg.sampling_irregularity_tolerance:
                issues.append(issue("INCONSISTENT_SAMPLING_RATE", "info", "Irregular sampling interval", rec,
                                    "timestamp", round(dt, 6)))
    return issues
