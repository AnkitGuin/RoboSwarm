from datetime import datetime, timezone, timedelta
from uuid import uuid5, NAMESPACE_URL

from robostream.schemas import TelemetryRecord
from robostream.config import PipelineConfig
from robostream.segmentation.episodes import segment
from robostream.quality.scoring import assess
from robostream.validation.temporal import temporal_issues


START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def sample(seq, timestamp, task="task-1", robot="r", session="s", *, status=None):
    values = {"event_id": str(uuid5(NAMESPACE_URL, f"{robot}-{session}-{seq}-{task}")),
              "robot_id": robot, "session_id": session,
              "timestamp": timestamp.isoformat(), "sequence_number": seq,
              "event_type": "task_event" if status else "observation",
              "task_id": task}
    if status:
        values["task_status"] = status
    else:
        values["joint_positions"] = [0.0] * 6
        values["joint_velocities"] = [0.0] * 6
        values["end_effector_position"] = [0.0, 0.0, 0.0]
        values["end_effector_orientation"] = [0.0, 0.0, 0.0, 1.0]
        values["gripper_position"] = 1.0
    return TelemetryRecord.model_validate(values)


def test_deterministic_segmentation_across_reordered_arrival():
    records = [sample(0, START), sample(1, START + timedelta(milliseconds=50)),
               sample(2, START + timedelta(milliseconds=100), status="success")]
    cfg = PipelineConfig()
    x, y = segment(records, cfg), segment(records[::-1], cfg)
    assert x[0].episode_id == y[0].episode_id
    assert x[0].completion_status == "success"
    assert assess(x[0], [], cfg)["quality_status"] == "excellent"


def test_gap_and_multiple_task_ids():
    records = [sample(0, START), sample(1, START + timedelta(seconds=2)),
               sample(2, START + timedelta(seconds=3), task="other"),
               sample(3, START + timedelta(seconds=3.1), task="other", status="failure")]
    eps = segment(records, PipelineConfig(gap_seconds=1))
    assert len(eps) == 3
    assert eps[0].completion_status == "incomplete"
    assert eps[-1].completion_status == "failure"
    assert assess(eps[0], [], PipelineConfig())["quality_score"] == 80


def test_temporal_anomalies_keep_arrival_order():
    recs = [sample(1, START + timedelta(seconds=1)), sample(0, START),
            sample(1, START + timedelta(seconds=0.1))]
    codes = {i.issue_code for i in temporal_issues(recs, PipelineConfig())}
    assert "OUT_OF_ORDER_TIMESTAMP" in codes
    assert "DUPLICATE_SEQUENCE_NUMBER" in codes
    assert "OUT_OF_ORDER_SEQUENCE" in codes


def test_empty_and_one_record():
    assert segment([], PipelineConfig()) == []
    ep = segment([sample(0, START)], PipelineConfig())[0]
    assert ep.completion_status == "incomplete"
    assert assess(ep, [], PipelineConfig())["sampling_rate_hz"] == 0


def test_sampling_metrics_ignore_task_boundary_events():
    records = [
        sample(0, START, status="running"),
        sample(1, START),
        sample(2, START + timedelta(milliseconds=50)),
        sample(3, START + timedelta(milliseconds=100), status="success"),
    ]
    cfg = PipelineConfig()
    episode = segment(records, cfg)[0]

    assert assess(episode, [], cfg)["sampling_rate_hz"] == 20
    assert "DUPLICATE_TIMESTAMP" not in {
        issue.issue_code for issue in temporal_issues(records, cfg)
    }


def test_never_cross_robot_or_session_boundaries():
    records = [sample(0, START, robot="r1"), sample(0, START, robot="r2"),
               sample(0, START, robot="r1", session="s2")]
    assert len(segment(records, PipelineConfig())) == 3
