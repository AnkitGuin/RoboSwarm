"""Deterministic, smooth pick-and-place telemetry with reproducible defects."""
from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5
from typing import Iterator

from robostream.storage.files import atomic_json

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
DEFECTS = (
    "missing", "duplicate", "duplicate_sequence", "out_of_order", "gap", "irregular",
    "nonfinite", "joint_limit", "quaternion", "incomplete", "interrupted", "conflict"
)


@dataclass(frozen=True)
class GeneratorOptions:
    robots: int = 3
    sessions_per_robot: int = 2
    duration_seconds: float = 15.0
    frequency_hz: float = 20.0
    seed: int = 42
    scenario: str = "degraded"
    joint_count: int = 6
    noise: float = 0.005
    tasks_per_session: int = 2
    defect_probability: float = 0.015

    def __post_init__(self):
        if (self.robots < 1 or self.sessions_per_robot < 1 or self.duration_seconds <= 0
                or self.frequency_hz <= 0 or self.joint_count < 1 or self.tasks_per_session < 1):
            raise ValueError("Counts, duration, frequency and joints must be positive")
        if not 0 <= self.defect_probability <= 1 or self.noise < 0:
            raise ValueError("Invalid noise or defect_probability")
        if self.scenario not in {"clean", "degraded"}:
            raise ValueError("scenario must be clean or degraded")


def identifier(*parts: object) -> str:
    return str(uuid5(NAMESPACE_URL, "robostream:" + ":".join(map(str, parts))))


def smoothstep(t: float) -> tuple[float, float]:
    t = max(0.0, min(1.0, t))
    return 3 * t * t - 2 * t * t * t, 6 * t - 6 * t * t


def make_observation(robot: str, session: str, task: str, sequence: int,
                     timestamp: datetime, progress: float, trajectory_duration: float,
                     options: GeneratorOptions, rng: random.Random) -> dict:
    if trajectory_duration <= 0:
        raise ValueError("trajectory_duration must be positive")
    # Smooth joint trajectories and velocities from the SAME analytic path.
    motion, derivative = smoothstep(progress)
    positions = []
    velocities = []
    for j in range(options.joint_count):
        offset = 0.15 * math.sin(j * 0.7)
        amplitude = 0.65 * math.cos(j * 0.4)
        positions.append(round(offset + amplitude * motion + rng.gauss(0, options.noise), 6))
        velocities.append(round(amplitude * derivative / trajectory_duration, 6))
    gripper = 1.0 if progress < 0.32 or progress > 0.78 else 0.0
    state = "open" if gripper == 1 else "closed"
    return {
        "event_id": identifier(options.seed, robot, session, sequence),
        "robot_id": robot, "session_id": session,
        "timestamp": timestamp.isoformat(), "sequence_number": sequence,
        "event_type": "observation", "joint_positions": positions,
        "joint_velocities": velocities,
        "end_effector_position": [round(0.2 + 0.3 * motion, 6),
                                  round(0.1 * math.sin(math.pi * progress), 6),
                                  round(0.3 + 0.25 * motion, 6)],
        "end_effector_orientation": [0.0, 0.0, 0.0, 1.0],
        "gripper_position": gripper, "gripper_state": state,
        "task_id": task, "metadata": {"phase": "pick" if progress < 0.5 else "place"},
    }


def task_event(robot: str, session: str, task: str, seq: int,
               timestamp: datetime, status: str, seed: int, boundary: str) -> dict:
    return {"event_id": identifier(seed, robot, session, seq), "robot_id": robot,
            "session_id": session, "timestamp": timestamp.isoformat(),
            "sequence_number": seq, "event_type": "task_event", "task_id": task,
            "task_status": status, "metadata": {"boundary": boundary}}


def session_records(robot: str, session: str, session_index: int,
                    options: GeneratorOptions, rng: random.Random) -> Iterator[dict]:
    start = START + timedelta(hours=session_index)
    samples_per_task = max(2, round(options.duration_seconds * options.frequency_hz
                                    / options.tasks_per_session))
    sequence = 0
    abruptly_interrupted = (options.scenario == "degraded" and
                            rng.random() < options.defect_probability * 2)
    for task_index in range(options.tasks_per_session):
        task = f"task_{task_index + 1:03}"
        task_start = start + timedelta(seconds=task_index * options.duration_seconds
                                       / options.tasks_per_session)
        task_duration = options.duration_seconds / options.tasks_per_session
        # progress runs from 0 through 1 across the first and last observation.
        # Its time span is therefore (sample_count - 1) / frequency, not the
        # nominal task duration (which also includes the final unsampled interval).
        trajectory_duration = (samples_per_task - 1) / options.frequency_hz
        yield task_event(robot, session, task, sequence, task_start, "running",
                         options.seed, "start")
        sequence += 1
        pending = None
        cut_short = False
        for i in range(samples_per_task):
            if (abruptly_interrupted and task_index == options.tasks_per_session - 1 and
                    i >= samples_per_task // 2):
                cut_short = True
                break
            progress = i / (samples_per_task - 1)
            ts = task_start + timedelta(seconds=i / options.frequency_hz)
            if options.scenario == "degraded" and rng.random() < options.defect_probability:
                ts += timedelta(milliseconds=rng.choice([-33, 58, 112]))
            rec = make_observation(robot, session, task, sequence, ts, progress,
                                   trajectory_duration, options, rng)
            sequence += 1
            if options.scenario == "degraded":
                p = options.defect_probability
                if rng.random() < p:
                    rec["joint_positions"] = None
                if rng.random() < p and rec["joint_positions"]:
                    rec["joint_positions"][0] = float("nan")
                if rng.random() < p and rec["joint_positions"]:
                    rec["joint_positions"][0] = 9.0
                if rng.random() < p:
                    rec["end_effector_orientation"] = [0.0, 0.0, 0.0, 0.2]
                if rng.random() < p:
                    rec["sequence_number"] = max(0, sequence - 2)
                if rng.random() < p:
                    rec["timestamp"] = (ts + timedelta(seconds=3)).isoformat()
                if rng.random() < p and pending is None:
                    pending = rec
                    continue
            yield rec
            if pending is not None:
                yield pending
                pending = None
            if options.scenario == "degraded" and rng.random() < options.defect_probability:
                yield rec.copy()  # exact duplicate event ID, auditable at ingest
        if pending is not None:
            yield pending
        terminal_missing = cut_short or (options.scenario == "degraded" and
                                         rng.random() < options.defect_probability * 5)
        if not terminal_missing:
            terminal_time = task_start + timedelta(seconds=task_duration)
            terminal_status = "success" if rng.random() > 0.08 else "failure"
            yield task_event(robot, session, task, sequence, terminal_time,
                             terminal_status, options.seed, "end")
            sequence += 1
            if options.scenario == "degraded" and rng.random() < options.defect_probability:
                yield task_event(robot, session, task, sequence, terminal_time,
                                 "aborted", options.seed, "end")
                sequence += 1


def generate(output: Path, options: GeneratorOptions) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    target = output / "telemetry.jsonl"
    if target.exists():
        raise FileExistsError(f"Already exists: {target}. Choose another output directory")
    rng = random.Random(options.seed)
    sessions = []
    count = 0
    # Each record is written immediately; no all-data in-memory accumulation.
    with target.open("w", encoding="utf-8") as fh:
        for rid in range(options.robots):
            robot = f"robot_{rid + 1:03d}"
            for sid in range(options.sessions_per_robot):
                session = f"session_{rid + 1:03d}_{sid + 1:03d}"
                sessions.append({"robot_id": robot, "session_id": session,
                                 "operator_id": f"synthetic_operator_{sid + 1:03d}",
                                 "source": "synthetic", "task_type": "pick_and_place"})
                for rec in session_records(robot, session,
                                           rid * options.sessions_per_robot + sid, options, rng):
                    fh.write(json.dumps(rec, separators=(",", ":"), allow_nan=True) + "\n")
                    count += 1
    from hashlib import sha256
    h = sha256()
    with target.open("rb") as input_stream:
        for chunk in iter(lambda: input_stream.read(1024 * 1024), b""):
            h.update(chunk)
    digest = h.hexdigest()
    manifest = {"generator": "robostream-0.1.0", "seed": options.seed,
                "config": asdict(options), "record_count": count, "sha256": digest,
                "simulation_epoch_utc": START.isoformat(),
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "defects": list(DEFECTS) if options.scenario == "degraded" else []}
    atomic_json(output / "manifest.json", manifest)
    atomic_json(output / "sessions.json", sessions)
    return manifest
