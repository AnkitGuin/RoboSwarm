"""Read-only DuckDB analytics over typed Parquet; no user-provided SQL identifiers."""
from __future__ import annotations
from pathlib import Path
from robostream.storage.files import safe_relative_path


def sensor_summary(data_root: Path, files: list[str]) -> list[dict]:
    import duckdb
    paths = []
    for f in files:
        rel = safe_relative_path(f)
        path = (data_root / rel).resolve()
        if not path.is_relative_to(data_root.resolve()):
            raise ValueError("Path outside data directory")
        if path.is_file() and path.suffix == ".parquet":
            paths.append(str(path))
    if not paths:
        return []
    with duckdb.connect(database=":memory:") as con:
        # Python list is passed via bound parameter, never through SQL concatenation.
        result = con.execute("""SELECT robot_id, COUNT(*) AS records,
                                  ROUND(AVG(gripper_position), 4) AS avg_gripper_position,
                                  ROUND(AVG(CASE WHEN joint_positions IS NULL THEN 1 ELSE 0 END), 4)
                                      AS missing_joints_rate
                               FROM read_parquet(?, hive_partitioning=false) GROUP BY robot_id ORDER BY robot_id""",
                             [paths])
        cols = [v[0] for v in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]
