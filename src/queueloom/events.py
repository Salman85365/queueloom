"""Versioned wire schema for task lifecycle events.

This is the contract between the SDK (running inside Celery clients/workers) and the
ingestion server. Every event carries ``schema_version`` so the server can accept, migrate
or reject payloads produced by older/newer SDKs. Bump ``SCHEMA_VERSION`` for any change that
is not purely additive-with-defaults.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION: Final = 1

MAX_MESSAGE_CHARS = 2000
MAX_TRACEBACK_CHARS = 20_000
MAX_REPR_CHARS = 2000


class EventType(StrEnum):
    PUBLISHED = "task.published"
    STARTED = "task.started"
    SUCCEEDED = "task.succeeded"
    FAILED = "task.failed"
    RETRIED = "task.retried"
    REVOKED = "task.revoked"


def utcnow() -> datetime:
    return datetime.now(UTC)


def ensure_aware(value: datetime) -> datetime:
    """Treat naive datetimes as UTC; normalise aware ones to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def truncate(text: str | None, limit: int) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    marker = "…[truncated]"
    return text[: max(0, limit - len(marker))] + marker


class ExceptionInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str = Field(max_length=200)
    message: str = Field(default="", max_length=MAX_MESSAGE_CHARS)
    traceback: str | None = Field(default=None, max_length=MAX_TRACEBACK_CHARS)


class TaskEvent(BaseModel):
    """One lifecycle event for one task execution."""

    model_config = ConfigDict(extra="ignore")

    schema_version: Literal[1] = SCHEMA_VERSION
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex, max_length=64)
    event_type: EventType
    timestamp: datetime = Field(default_factory=utcnow)

    task_id: str = Field(max_length=255)
    task_name: str | None = Field(default=None, max_length=255)
    queue: str | None = Field(default=None, max_length=255)
    environment: str = Field(default="default", max_length=64)
    worker: str | None = Field(default=None, max_length=255)

    # Set by the client when the message is published and forwarded through message headers
    # so the worker can report queue latency even when the client is not instrumented.
    published_at: datetime | None = None
    eta: datetime | None = None
    retries: int | None = Field(default=None, ge=0)
    runtime_ms: float | None = Field(default=None, ge=0)

    parent_id: str | None = Field(default=None, max_length=255)
    root_id: str | None = Field(default=None, max_length=255)

    exception: ExceptionInfo | None = None
    args_repr: str | None = Field(default=None, max_length=MAX_REPR_CHARS)
    kwargs_repr: str | None = Field(default=None, max_length=MAX_REPR_CHARS)

    @field_validator("timestamp", "published_at", "eta", mode="after")
    @classmethod
    def _normalise_tz(cls, value: datetime | None) -> datetime | None:
        return None if value is None else ensure_aware(value)


class EventBatch(BaseModel):
    """Request body for ``POST /v1/events``."""

    model_config = ConfigDict(extra="ignore")

    events: list[TaskEvent] = Field(min_length=1, max_length=1000)
