import json
import random
from datetime import datetime, timezone
import pytest

from robostream.generator.synthetic import GeneratorOptions, generate, make_observation
from robostream.schemas import TelemetryRecord
from robostream.validation.checks import physical_issues
from robostream.config import RobotProfile


def test_generation_reproducible(tmp_path):
    opts = GeneratorOptions(robots=1, sessions_per_robot=1,
                            duration_seconds=3, frequency_hz=10, seed=84, scenario="clean")
    m1 = generate(tmp_path / "one", opts)
    m2 = generate(tmp_path / "two", opts)
    assert {k: v for k, v in m1.items() if k != "generated_at_utc"} == {
        k: v for k, v in m2.items() if k != "generated_at_utc"}
    assert (tmp_path / "one/telemetry.jsonl").read_bytes() == (tmp_path / "two/telemetry.jsonl").read_bytes()
    records = [TelemetryRecord.model_validate(json.loads(line)) for line in
               (tmp_path / "one/telemetry.jsonl").read_text().splitlines()]
    assert all(not physical_issues(r, RobotProfile()) for r in records)
    assert any(r.task_status and r.task_status.value == "success" for r in records)


def test_degraded_has_faults_and_is_reproducible(tmp_path):
    opts = GeneratorOptions(robots=1, sessions_per_robot=1, duration_seconds=3,
                            frequency_hz=10, seed=7, scenario="degraded", defect_probability=0.4)
    a = generate(tmp_path / "a", opts)
    b = generate(tmp_path / "b", opts)
    assert {k: v for k, v in a.items() if k != "generated_at_utc"} == {
        k: v for k, v in b.items() if k != "generated_at_utc"}
    assert (tmp_path / "a/telemetry.jsonl").read_bytes() == (tmp_path / "b/telemetry.jsonl").read_bytes()
    assert a["record_count"] > 0
    parsed = [TelemetryRecord.model_validate(json.loads(line)) for line in
              (tmp_path / "a/telemetry.jsonl").read_text().splitlines()]
    assert any(physical_issues(r, RobotProfile()) for r in parsed)


def test_generator_input_validation():
    with pytest.raises(ValueError):
        GeneratorOptions(frequency_hz=0)


def test_velocity_is_derivative_over_observation_span():
    options = GeneratorOptions(joint_count=1, noise=0, scenario="clean")
    record = make_observation("r", "s", "t", 0,
                              datetime(2026, 1, 1, tzinfo=timezone.utc),
                              0.5, 0.9, options, random.Random(1))

    expected_amplitude = 0.65
    smoothstep_derivative_at_midpoint = 1.5
    assert record["joint_velocities"][0] == pytest.approx(
        expected_amplitude * smoothstep_derivative_at_midpoint / 0.9
    )
