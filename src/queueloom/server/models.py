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
    Boolean,
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
    alert_rules: Mapped[list[AlertRule]] = relationship(back_populates="project")


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


class AlertState(StrEnum):
    OK = "ok"
    FIRING = "firing"


class AlertRule(Base):
    """Fire a webhook when the failure rate over a sliding window crosses a threshold."""

    __tablename__ = "alert_rules"
    __table_args__ = (Index("ix_alert_rules_project_enabled", "project_id", "enabled"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(100))
    environment: Mapped[str | None] = mapped_column(String(64))
    task_name: Mapped[str | None] = mapped_column(String(255))
    window_minutes: Mapped[int] = mapped_column(Integer, default=15)
    threshold: Mapped[float] = mapped_column(Float, default=0.1)
    min_runs: Mapped[int] = mapped_column(Integer, default=10)
    cooldown_minutes: Mapped[int] = mapped_column(Integer, default=30)
    webhook_url: Mapped[str] = mapped_column(String(2000))
    webhook_format: Mapped[str] = mapped_column(String(16), default="json")
    webhook_secret: Mapped[str | None] = mapped_column(String(200))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    state: Mapped[str] = mapped_column(String(16), default=AlertState.OK.value)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_triggered_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=lambda: datetime.now(UTC))

    project: Mapped[Project] = relationship(back_populates="alert_rules")
    events: Mapped[list[AlertEvent]] = relationship(
        back_populates="rule", cascade="all, delete-orphan", order_by="AlertEvent.id.desc()"
    )


class AlertEvent(Base):
    """History of alert transitions and webhook deliveries."""

    __tablename__ = "alert_events"
    __table_args__ = (Index("ix_alert_events_project_created", "project_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rule_id: Mapped[int] = mapped_column(ForeignKey("alert_rules.id", ondelete="CASCADE"))
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    kind: Mapped[str] = mapped_column(String(16))  # fired | resolved | test
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=lambda: datetime.now(UTC))
    failure_rate: Mapped[float | None] = mapped_column(Float)
    failed: Mapped[int] = mapped_column(Integer, default=0)
    finished: Mapped[int] = mapped_column(Integer, default=0)
    window_since: Mapped[datetime | None] = mapped_column(UTCDateTime)
    window_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
    delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    delivery_status: Mapped[str | None] = mapped_column(String(200))

    rule: Mapped[AlertRule] = relationship(back_populates="events")
