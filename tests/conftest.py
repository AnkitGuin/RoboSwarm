from __future__ import annotations
from pathlib import Path
import pytest

from robostream.config import Settings
from robostream.database.models import Base
from robostream.database.session import engine_for


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'test.db'}", data_dir=tmp_path / "data")
    Base.metadata.create_all(engine_for(settings))  # test-only database, production uses Alembic
    return settings
