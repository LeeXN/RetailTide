from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import Settings, get_settings
from .models.base import Base


def make_engine(settings: Settings | None = None, *, url: str | None = None) -> Engine:
    settings = settings or get_settings()
    database_url = url or settings.database_url
    kwargs: dict = {"future": True, "pool_pre_ping": True}
    if database_url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 60}
    engine = create_engine(database_url, **kwargs)
    if database_url.startswith("sqlite"):

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=60000")
            cursor.execute("PRAGMA cache_size=-32768")
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.execute("PRAGMA mmap_size=268435456")
            cursor.close()

    return engine


def session_factory(engine: Engine):
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)


def init_db(engine: Engine | None = None) -> Engine:
    engine = engine or make_engine()
    Base.metadata.create_all(engine)
    return engine
