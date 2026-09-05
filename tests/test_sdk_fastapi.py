from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from queueloom.events import EventType
from queueloom.sdk.fastapi import (
    QueueLoomBackgroundTasks,
    add_task,
    instrument,
    uninstrument,
)
from queueloom.sdk.transport import MemoryTransport

RUNS: list[str] = []


def send_email(address: str, *, subject: str = "hi") -> None:
    RUNS.append(f"{address}:{subject}")
    if address.endswith("@invalid.example"):
        raise ValueError(f"undeliverable {address}")


async def async_job(n: int) -> None:
    RUNS.append(f"async:{n}")


@pytest.fixture
def transport() -> Iterator[MemoryTransport]:
    transport = MemoryTransport()
    instrument(transport=transport, environment="api", capture_args=True)
    RUNS.clear()
    yield transport
    uninstrument()


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/signup")
    async def signup(address: str, tasks: BackgroundTasks) -> dict[str, Any]:
        task_id = add_task(tasks, send_email, address, subject="welcome")
        add_task(tasks, async_job, 7, queueloom_name="jobs.async", queueloom_queue="misc")
        return {"task_id": task_id}

    @app.post("/manual")
    async def manual() -> JSONResponse:
        tasks = QueueLoomBackgroundTasks()
        tasks.add_task(send_email, "b@example.com")
        return JSONResponse({"ok": True}, background=tasks)

    return app


def _events(transport: MemoryTransport, task_id: str) -> list[Any]:
    return [e for e in transport.events if e.task_id == task_id]


def test_tracked_background_tasks(transport: MemoryTransport) -> None:
    with TestClient(_app()) as client:
        response = client.post("/signup", params={"address": "a@example.com"})
        task_id = response.json()["task_id"]
    assert RUNS == ["a@example.com:welcome", "async:7"]
    events = _events(transport, task_id)
    assert [e.event_type for e in events] == [
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.SUCCEEDED,
    ]
    published, started, succeeded = events
    assert published.task_name == "tests.test_sdk_fastapi.send_email"
    assert published.queue == "background"
    assert started.published_at == published.published_at
    assert started.args_repr == "('a@example.com',)" and "welcome" in (started.kwargs_repr or "")
    assert succeeded.runtime_ms is not None and started.worker
    named = [e for e in transport.events if e.task_name == "jobs.async"]
    assert [e.event_type for e in named][-1] == EventType.SUCCEEDED and named[0].queue == "misc"


def test_failure_is_recorded_and_reraised(transport: MemoryTransport) -> None:
    with TestClient(_app(), raise_server_exceptions=False) as client:
        response = client.post("/signup", params={"address": "x@invalid.example"})
        task_id = response.json()["task_id"]
    events = _events(transport, task_id)
    assert events[-1].event_type == EventType.FAILED
    assert events[-1].exception is not None
    assert events[-1].exception.type == "ValueError"
    assert "undeliverable" in events[-1].exception.message
    # The second task still ran? Starlette stops after the first failing task; either way the
    # failure must be visible.
    assert any(e.event_type == EventType.FAILED for e in transport.events)


def test_manual_background_tasks_subclass(transport: MemoryTransport) -> None:
    with TestClient(_app()) as client:
        client.post("/manual")
    assert RUNS == ["b@example.com:hi"]
    kinds = [e.event_type for e in transport.events]
    assert kinds == [EventType.PUBLISHED, EventType.STARTED, EventType.SUCCEEDED]


def test_unconfigured_falls_back_to_plain_add_task(monkeypatch: pytest.MonkeyPatch) -> None:
    uninstrument()
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    RUNS.clear()
    with TestClient(_app()) as client:
        assert client.post("/signup", params={"address": "c@example.com"}).json()["task_id"] is None
    assert RUNS == ["c@example.com:welcome", "async:7"]
