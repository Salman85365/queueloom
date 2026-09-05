from __future__ import annotations

import os
import threading

import httpx
import pytest
from fastapi.testclient import TestClient

from queueloom.events import EventType
from queueloom.sdk.transport import HttpTransport, MemoryTransport
from tests.conftest import ProjectInfo, at, lifecycle, make_event


def test_memory_transport_collects() -> None:
    transport = MemoryTransport()
    transport.send(make_event())
    assert len(transport.events) == 1
    assert transport.flush() is True
    transport.clear()
    assert transport.events == []


def test_http_transport_delivers_batches(client: TestClient, project: ProjectInfo) -> None:
    transport = HttpTransport(
        "http://testserver", project.api_key, client=client, batch_size=2, flush_interval=0.05
    )
    try:
        for event in lifecycle("t1"):
            transport.send(event)
        assert transport.flush(timeout=5) is True
        assert transport.sent == 3
        assert transport.dropped == 0
    finally:
        transport.close()

    body = client.get(
        "/v1/tasks", params={"since": at(-10).isoformat()}, headers=project.headers
    ).json()
    assert body["total"] == 1 and body["items"][0]["state"] == "succeeded"


def test_http_transport_drops_on_client_error(client: TestClient) -> None:
    transport = HttpTransport("http://testserver", "ql_wrong", client=client, flush_interval=0.05)
    try:
        transport.send(make_event())
        assert transport.flush(timeout=5) is True
        assert transport.sent == 0
        assert transport.failed_batches == 1
        assert transport.dropped == 1
    finally:
        transport.close()


def test_http_transport_retries_server_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx.Response(503, text="try later")
        return httpx.Response(202, json={"accepted": 1, "duplicates": 0})

    fake = httpx.Client(transport=httpx.MockTransport(handler))
    transport = HttpTransport(
        "http://example", "ql_x", client=fake, flush_interval=0.01, max_retries=3
    )
    try:
        transport.send(make_event())
        assert transport.flush(timeout=10) is True
        assert calls == 3
        assert transport.sent == 1
    finally:
        transport.close()


def test_http_transport_drops_when_queue_is_full() -> None:
    blocker = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        blocker.wait(5)
        return httpx.Response(202)

    fake = httpx.Client(transport=httpx.MockTransport(handler))
    transport = HttpTransport(
        "http://example", "ql_x", client=fake, batch_size=1, flush_interval=0.01, max_queue_size=2
    )
    try:
        for _ in range(10):
            transport.send(make_event(EventType.STARTED))
        assert transport.dropped >= 6  # queue(2) + in-flight(1) + a little slack
    finally:
        blocker.set()
        transport.close()


def test_http_transport_restarts_after_fork(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(202)))
    transport = HttpTransport("http://example", "ql_x", client=fake, flush_interval=0.01)
    try:
        transport.send(make_event())
        assert transport.flush(timeout=5)
        first_thread = transport._thread
        real_pid = os.getpid()
        monkeypatch.setattr(os, "getpid", lambda: real_pid + 1)
        transport.send(make_event())
        assert transport._thread is not first_thread
        assert transport.flush(timeout=5)
        assert transport.sent == 2
    finally:
        monkeypatch.undo()
        transport._pid = os.getpid()
        transport.close()


def test_http_transport_validates_arguments() -> None:
    with pytest.raises(ValueError):
        HttpTransport("", "key")
    with pytest.raises(ValueError):
        HttpTransport("http://x", "")
