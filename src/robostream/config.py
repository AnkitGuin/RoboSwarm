"""Configurable paths, model-level thresholds, and quality weights."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ROBOSTREAM_", env_file=".env", extra="ignore")
    database_url: str = "sqlite:///./data/robostream.db"  # PostgreSQL in Compose
    data_dir: Path = Path("data")
    max_batch_size: int = Field(5000, ge=1, le=100000)
    max_upload_bytes: int = Field(20_000_000, ge=1024)
    gap_seconds: float = Field(1.0, gt=0)
    expected_frequency_hz: float = Field(20.0, gt=0)
    log_level: str = "INFO"


class RobotProfile(BaseModel):
    joint_count: int = Field(6, gt=0)
    joint_position_min: float = -3.141593
    joint_position_max: float = 3.141593
    max_joint_velocity: float = Field(3.0, gt=0)
    workspace_min: float = -2.0
    workspace_max: float = 2.0
    gripper_min: float = 0.0
    gripper_max: float = 1.0
    quaternion_tolerance: float = Field(0.05, gt=0)


class QualityWeights(BaseModel):
    missing_weight: float = Field(30, ge=0)
    timestamp_weight: float = Field(20, ge=0)
    duplicate_weight: float = Field(15, ge=0)
    invalid_weight: float = Field(30, ge=0)
    incomplete_penalty: float = Field(20, ge=0)
    poor_threshold: float = 50
    acceptable_threshold: float = 75
    excellent_threshold: float = 90


class PipelineConfig(BaseModel):
    pipeline_version: str = "0.1.0"
    gap_seconds: float = Field(1.0, gt=0)
    expected_frequency_hz: float = Field(20.0, gt=0)
    sampling_irregularity_tolerance: float = Field(0.60, ge=0)
    quality: QualityWeights = Field(default_factory=QualityWeights)

    @property
    def fingerprint(self) -> str:
        return sha256(json.dumps(self.model_dump(), sort_keys=True).encode()).hexdigest()[:16]


def load_config(path: str | Path | None = None) -> PipelineConfig:
    if path is None:
        return PipelineConfig()
    return PipelineConfig.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def load_profile(path: str | Path | None = None, name: str = "default") -> RobotProfile:
    if path is None:
        return RobotProfile()
    profiles: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if name not in profiles:
        raise KeyError(f"Unknown robot profile: {name}")
    return RobotProfile.model_validate(profiles[name])
