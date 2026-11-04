"""Deterministic episode boundaries: stream/task changes, terminals, then gaps."""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from robostream.config import PipelineConfig
from robostream.schemas import TelemetryRecord

TERMINALS = {"success", "failure", "aborted"}


@dataclass
class Segment:
    episode_id: str
    robot_id: str
    session_id: str
    task_id: str | None
    records: list[TelemetryRecord]
    completion_status: str
    warnings: list[str]


def _build(records: list[TelemetryRecord], cfg: PipelineConfig, warnings: list[str],
           identity_hash: str | None = None) -> Segment:
    last = records[-1]
    terminal = [(r.task_status.value, r) for r in records if r.event_type.value == "task_event"
                and r.task_status is not None and r.task_status.value in TERMINALS]
    status = terminal[-1][0] if terminal else "incomplete"
    if not terminal:
        warnings.append("INCOMPLETE_EPISODE")
    if len({name for name, _ in terminal}) > 1:
        warnings.append("CONFLICTING_TASK_STATUS")
    # Order-independent within a fixed normalized time stream, and stable across runs.
    identity = (identity_hash or cfg.fingerprint, last.robot_id, last.session_id, last.task_id,
                tuple(str(r.event_id) for r in records))
    episode_id = str(uuid5(NAMESPACE_URL, repr(identity)))
    return Segment(episode_id, last.robot_id, last.session_id, last.task_id,
                   records.copy(), status, list(dict.fromkeys(warnings)))


def segment(records: list[TelemetryRecord], cfg: PipelineConfig,
            identity_hash: str | None = None) -> list[Segment]:
    streams = defaultdict(list)
    for r in records:
        streams[(r.robot_id, r.session_id)].append(r)
    output: list[Segment] = []
    for key in sorted(streams):
        ordered = sorted(streams[key], key=lambda r: (r.timestamp, r.sequence_number, str(r.event_id)))
        current: list[TelemetryRecord] = []
        warnings: list[str] = []
        for rec in ordered:
            if current:
                prev = current[-1]
                gap = (rec.timestamp - prev.timestamp).total_seconds()
                task_changed = rec.task_id is not None and rec.task_id != prev.task_id
                explicit_start = (rec.event_type.value == "task_event" and
                                  rec.metadata.get("boundary") == "start")
                if task_changed or gap > cfg.gap_seconds or explicit_start:
                    if gap > cfg.gap_seconds:
                        warnings.append("TIMESTAMP_GAP")
                    output.append(_build(current, cfg, warnings, identity_hash))
                    current, warnings = [], []
            current.append(rec)
            if (rec.event_type.value == "task_event" and rec.task_status is not None and
                    rec.task_status.value in TERMINALS):
                output.append(_build(current, cfg, warnings, identity_hash))
                current, warnings = [], []
        if current:
            output.append(_build(current, cfg, warnings, identity_hash))
    return output
