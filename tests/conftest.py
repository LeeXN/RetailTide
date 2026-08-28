from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from retail_tide.config import Settings
from retail_tide.db import init_db, make_engine, session_factory
from retail_tide.registry import sync_registry


@pytest.fixture()
def settings(tmp_path):
    return Settings(database_url=f"sqlite:///{tmp_path / 'test.db'}", config_dir="config")


@pytest.fixture()
def session(settings) -> Session:
    engine = init_db(make_engine(settings))
    factory = session_factory(engine)
    with factory() as db:
        sync_registry(db, settings.config_dir)
        yield db
