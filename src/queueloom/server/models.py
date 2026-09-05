"""Persistence model.

* ``projects`` — one row per API key.
* ``task_events`` — append-only raw events exactly as received (audit trail, replayable).
* ``task_runs`` — one row per task id, materialised from events at ingest time. This is what
  the dashboard and query API read; it keeps hot queries cheap without a separate pipeline.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class UTCDateTime(TypeDecorator[datetime]):
    """Store UTC, always return timezone-aware UTC (SQLite drops tzinfo otherwise)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


JSONType = JSON().with_variant(JSONB(), "postgresql")


class RunState(StrEnum):
    QUEUED = "queued"
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    REVOKED = "revoked"


TERMINAL_STATES = frozenset({RunState.SUCCEEDED, RunState.FAILED, RunState.REVOKED})


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    api_key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=lambda: datetime.now(UTC))

    runs: Mapped[list[TaskRun]] = relationship(back_populates="project")


class TaskEventRow(Base):
    __tablename__ = "task_events"
    __table_args__ = (
        Index("ix_task_events_project_task", "project_id", "task_id"),
        Index("ix_task_events_project_ts", "project_id", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    schema_version: Mapped[int] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(32))
    task_id: Mapped[str] = mapped_column(String(255))
    task_name: Mapped[str | None] = mapped_column(String(255))
    environment: Mapped[str] = mapped_column(String(64))
    timestamp: Mapped[datetime] = mapped_column(UTCDateTime)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime, default=lambda: datetime.now(UTC))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)


class TaskRun(Base):
    __tablename__ = "task_runs"
    __table_args__ = (
        UniqueConstraint("project_id", "task_id", name="uq_task_runs_project_task"),
        Index("ix_task_runs_project_last_event", "project_id", "last_event_at"),
        Index("ix_task_runs_project_state", "project_id", "state"),
        Index("ix_task_runs_project_name", "project_id", "task_name"),
        Index("ix_task_runs_project_env", "project_id", "environment"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    task_id: Mapped[str] = mapped_column(String(255))
    task_name: Mapped[str | None] = mapped_column(String(255))
    queue: Mapped[str | None] = mapped_column(String(255))
    environment: Mapped[str] = mapped_column(String(64), default="default")
    worker: Mapped[str | None] = mapped_column(String(255))
    state: Mapped[str] = mapped_column(String(16), default=RunState.QUEUED.value)

    published_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_event_at: Mapped[datetime] = mapped_column(UTCDateTime)
    eta: Mapped[datetime | None] = mapped_column(UTCDateTime)

    queue_latency_ms: Mapped[float | None] = mapped_column(Float)
    duration_ms: Mapped[float | None] = mapped_column(Float)
    retries: Mapped[int] = mapped_column(Integer, default=0)
    attempts: Mapped[int] = mapped_column(Integer, default=0)

    exception_type: Mapped[str | None] = mapped_column(String(200))
    exception_message: Mapped[str | None] = mapped_column(Text)
    traceback: Mapped[str | None] = mapped_column(Text)

    parent_id: Mapped[str | None] = mapped_column(String(255))
    root_id: Mapped[str | None] = mapped_column(String(255))
    args_repr: Mapped[str | None] = mapped_column(Text)
    kwargs_repr: Mapped[str | None] = mapped_column(Text)

    project: Mapped[Project] = relationship(back_populates="runs")

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES
