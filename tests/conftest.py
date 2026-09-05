from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from queueloom.events import EventType, TaskEvent
from queueloom.server.app import create_app
from queueloom.server.config import Settings
from queueloom.server.db import make_engine, make_session_factory, session_scope
from queueloom.server.models import Base
from queueloom.server.projects import create_project

# Set QUEUELOOM_TEST_DATABASE_URL to run the suite against PostgreSQL (CI does).
TEST_DATABASE_URL = os.environ.get("QUEUELOOM_TEST_DATABASE_URL", "sqlite://")


@pytest.fixture
def engine() -> Iterator[Engine]:
    engine = make_engine(TEST_DATABASE_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return make_session_factory(engine)


@pytest.fixture
def settings() -> Settings:
    return Settings(auto_create_schema=False, dashboard_refresh_seconds=0, database_url="sqlite://")


@pytest.fixture
def app(engine: Engine, settings: Settings) -> FastAPI:
    return create_app(settings, engine=engine)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


@dataclass
class ProjectInfo:
    id: int
    name: str
    api_key: str

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}


def make_project(session_factory: sessionmaker[Session], name: str) -> ProjectInfo:
    with session_scope(session_factory) as session:
        project, key = create_project(session, name)
        return ProjectInfo(id=project.id, name=project.name, api_key=key)


@pytest.fixture
def project(session_factory: sessionmaker[Session]) -> ProjectInfo:
    return make_project(session_factory, "demo")


T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def make_event(event_type: EventType = EventType.PUBLISHED, **overrides: Any) -> TaskEvent:
    fields: dict[str, Any] = {
        "event_type": event_type,
        "task_id": "task-1",
        "task_name": "app.tasks.add",
        "queue": "celery",
        "timestamp": T0,
    }
    fields.update(overrides)
    return TaskEvent(**fields)


def lifecycle(
    task_id: str,
    *,
    outcome: EventType = EventType.SUCCEEDED,
    offset: float = 0.0,
    task_name: str = "app.tasks.add",
    **extra: Any,
) -> list[TaskEvent]:
    """A published → started → outcome sequence with realistic timings."""
    common = {"task_id": task_id, "task_name": task_name, **extra}
    events = [
        make_event(EventType.PUBLISHED, timestamp=at(offset), published_at=at(offset), **common),
        make_event(EventType.STARTED, timestamp=at(offset + 0.25), worker="w1", **common),
    ]
    end: dict[str, Any] = {"timestamp": at(offset + 1.25), "runtime_ms": 1000.0, "worker": "w1"}
    if outcome == EventType.FAILED:
        end["exception"] = {"type": "ValueError", "message": "boom", "traceback": "Traceback..."}
    events.append(make_event(outcome, **end, **common))
    return events


def dump(events: list[TaskEvent]) -> dict[str, Any]:
    return {"events": [e.model_dump(mode="json") for e in events]}
