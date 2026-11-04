"""Stable typed Parquet per episode, atomically replaced within a new run namespace."""
from __future__ import annotations
import json
import os
import tempfile
from pathlib import Path

from robostream.schemas import TelemetryRecord
from robostream.storage.files import safe_relative_path


def arrow_schema():
    import pyarrow as pa
    return pa.schema([
        pa.field("event_id", pa.string()), pa.field("robot_id", pa.string()),
        pa.field("session_id", pa.string()), pa.field("task_id", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("sequence_number", pa.int64()), pa.field("event_type", pa.string()),
        pa.field("task_status", pa.string()),
        pa.field("joint_positions", pa.list_(pa.float64())),
        pa.field("joint_velocities", pa.list_(pa.float64())),
        pa.field("end_effector_position", pa.list_(pa.float64())),
        pa.field("end_effector_orientation", pa.list_(pa.float64())),
        pa.field("gripper_position", pa.float64()), pa.field("gripper_state", pa.string()),
        pa.field("camera_frame_ref", pa.string()), pa.field("action_json", pa.string()),
        pa.field("metadata_json", pa.string()),
    ])


def write_episode(path: Path, records: list[TelemetryRecord]) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq
    columns = {
        "event_id": [str(r.event_id) for r in records],
        "robot_id": [r.robot_id for r in records],
        "session_id": [r.session_id for r in records],
        "task_id": [r.task_id for r in records],
        "timestamp": [r.timestamp for r in records],
        "sequence_number": [r.sequence_number for r in records],
        "event_type": [r.event_type.value for r in records],
        "task_status": [r.task_status.value if r.task_status else None for r in records],
        "joint_positions": [r.joint_positions for r in records],
        "joint_velocities": [r.joint_velocities for r in records],
        "end_effector_position": [r.end_effector_position for r in records],
        "end_effector_orientation": [r.end_effector_orientation for r in records],
        "gripper_position": [r.gripper_position for r in records],
        "gripper_state": [r.gripper_state.value if r.gripper_state else None for r in records],
        "camera_frame_ref": [r.camera_frame_ref for r in records],
        "action_json": [json.dumps(r.action) if r.action is not None else None for r in records],
        "metadata_json": [json.dumps(r.metadata, sort_keys=True) for r in records],
    }
    schema = arrow_schema()
    table = pa.Table.from_pydict(columns, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".tmp-", suffix=".parquet",
                                         delete=False) as f:
            tmp_path = Path(f.name)
        pq.write_table(table, tmp_path, compression="zstd", row_group_size=50000)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
    return path.stat().st_size


def read_episode(root: Path, storage_uri: str, limit: int = 500, offset: int = 0) -> dict:
    import pyarrow.parquet as pq
    rel = safe_relative_path(storage_uri)
    path = (root / rel).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Path escapes data root")
    if not path.is_file():
        raise FileNotFoundError("Episode Parquet file missing")
    table = pq.ParquetFile(path).read()
    rows = table.slice(offset, limit).to_pylist()
    for row in rows:
        if row.get("timestamp") is not None:
            row["timestamp"] = row["timestamp"].isoformat()
    return {"items": rows, "total": table.num_rows, "limit": limit, "offset": offset}
