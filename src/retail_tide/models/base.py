from __future__ import annotations

from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator

from ..time import as_utc


class UTCDateTime(TypeDecorator):
    """Store UTC and restore an aware UTC datetime on every database backend."""

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        value = as_utc(value)
        if dialect.name == "sqlite":
            return value.replace(tzinfo=None)
        return value

    def process_result_value(self, value, _dialect):
        return as_utc(value)


JSONType = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass
