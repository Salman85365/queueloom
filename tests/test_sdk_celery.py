from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

from queueloom.events import EventType
from queueloom.sdk.celery import HEADER_PUBLISHED_AT, CeleryInstrumentation, instrument
from queueloom.sdk.transport import MemoryTransport

# -- unit tests: handlers driven with fake Celery objects --------------------------------------


def _fake_task(task_id: str, name: str = "app.t", **request_fields: Any) -> SimpleNamespace:
    request = SimpleNamespace(
        id=task_id,
        hostname="worker@box",
        retries=0,
        eta=None,
        parent_id=None,
        root_id=task_id,
        delivery_info={"routing_key": "emails"},
        **request_fields,
    )
    request.get = lambda key, default=None: getattr(request, key, default)
    return SimpleNamespace(name=name, request=request)


def test_publish_injects_header_and_emits_event() -> None:
    transport = MemoryTransport()
    inst = CeleryInstrumentation(transport, environment="test")
    headers: dict[str, Any] = {"id": "abc", "task": "app.t", "retries": 0, "root_id": "abc"}
    inst.on_before_task_publish(sender="app.t", headers=headers, routing_key="emails")
    assert HEADER_PUBLISHED_AT in headers
    datetime.fromisoformat(headers[HEADER_PUBLISHED_AT])  # parseable
    (event,) = transport.events
    assert event.event_type == EventType.PUBLISHED
    assert event.task_id == "abc" and event.task_name == "app.t" and event.queue == "emails"
    assert event.environment == "test"
    assert event.published_at == event.timestamp


def test_worker_side_lifecycle_with_header() -> None:
    transport = MemoryTransport()
    inst = CeleryInstrumentation(transport, capture_args=True)
    published = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
    task = _fake_task("abc", **{HEADER_PUBLISHED_AT: published.isoformat()})

    inst.on_task_prerun(sender=task, task_id="abc", task=task, args=(1, 2), kwargs={"x": 1})
    inst.on_task_success(sender=task, result=3)
    started, succeeded = transport.events
    assert started.event_type == EventType.STARTED
    assert started.published_at == published
    assert started.queue == "emails" and started.worker == "worker@box"
    assert started.args_repr == "(1, 2)" and started.kwargs_repr == "{'x': 1}"
    assert succeeded.event_type == EventType.SUCCEEDED
    assert succeeded.runtime_ms is not None and succeeded.runtime_ms >= 0


def test_failure_and_retry_carry_exception_info() -> None:
    transport = MemoryTransport()
    inst = CeleryInstrumentation(transport)
    task = _fake_task("abc")
    try:
        raise ValueError("boom")
    except ValueError as exc:
        inst.on_task_failure(sender=task, task_id="abc", exception=exc, traceback=exc.__traceback__)
    inst.on_task_retry(sender=task, request=task.request, reason=TimeoutError("slow"), einfo=None)
    failed, retried = transport.events
    assert failed.exception is not None
    assert failed.exception.type == "ValueError" and failed.exception.message == "boom"
    assert (
        failed.exception.traceback is not None and "ValueError: boom" in failed.exception.traceback
    )
    assert retried.event_type == EventType.RETRIED
    assert retried.retries == 1
    assert retried.exception is not None and retried.exception.type == "TimeoutError"


def test_args_are_not_captured_by_default() -> None:
    transport = MemoryTransport()
    inst = CeleryInstrumentation(transport)
    task = _fake_task("abc")
    inst.on_task_prerun(sender=task, task_id="abc", task=task, args=("secret",), kwargs={})
    assert transport.events[0].args_repr is None


def test_emit_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    class Broken:
        def send(self, event: Any) -> None:
            raise RuntimeError("network is down")

        def flush(self, timeout: float | None = None) -> bool:
            return True

        def close(self) -> None:
            pass

    inst = CeleryInstrumentation(Broken())
    task = _fake_task("abc")
    inst.on_task_prerun(sender=task, task_id="abc", task=task)  # must not raise
    assert "failed to emit" in caplog.text


def test_instrument_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    with pytest.raises(ValueError):
        instrument()


# -- integration: a real in-process Celery worker over the memory broker ------------------------


@pytest.fixture(scope="module")
def celery_app() -> Celery:
    app = Celery("queueloom_tests", broker="memory://", backend="cache+memory://")
    app.conf.update(
        task_always_eager=False,
        worker_hijack_root_logger=False,
        worker_log_color=False,
        broker_connection_retry_on_startup=True,
        task_default_queue="celery",
    )

    @app.task(name="tests.add")
    def add(x: int, y: int) -> int:
        return x + y

    @app.task(name="tests.boom")
    def boom() -> None:
        raise ValueError("kaboom")

    @app.task(name="tests.flaky", bind=True, max_retries=2)
    def flaky(self: Any) -> str:
        if self.request.retries < 1:
            raise self.retry(exc=RuntimeError("try again"), countdown=0)
        return "ok"

    return app


@pytest.fixture(scope="module")
def worker(celery_app: Celery) -> Iterator[Any]:
    with start_worker(
        celery_app, pool="solo", perform_ping_check=False, loglevel="WARNING", shutdown_timeout=30
    ) as w:
        yield w


@pytest.fixture
def instrumentation() -> Iterator[tuple[CeleryInstrumentation, MemoryTransport]]:
    transport = MemoryTransport()
    inst = instrument(transport=transport, environment="integration")
    try:
        yield inst, transport
    finally:
        inst.close()


def _events_for(transport: MemoryTransport, task_id: str) -> list[Any]:
    return [e for e in transport.events if e.task_id == task_id]


def _wait_for(
    transport: MemoryTransport, task_id: str, terminal: EventType, timeout: float = 10.0
) -> list[Any]:
    """Wait for the terminal event of ``task_id``.

    Celery stores the result *before* it fires ``task_success``/``task_failure``, so
    ``AsyncResult.get()`` can return a moment before the SDK emits the final event.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = _events_for(transport, task_id)
        if any(e.event_type == terminal for e in events):
            return events
        time.sleep(0.02)
    raise AssertionError(f"no {terminal} event for {task_id}: {_events_for(transport, task_id)}")


@pytest.mark.integration
def test_real_worker_success(celery_app: Celery, worker: Any, instrumentation: Any) -> None:
    _, transport = instrumentation
    result = celery_app.tasks["tests.add"].delay(2, 3)
    assert result.get(timeout=20) == 5
    events = _wait_for(transport, result.id, EventType.SUCCEEDED)
    assert [e.event_type for e in events] == [
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.SUCCEEDED,
    ]
    published, started, succeeded = events
    assert published.queue == "celery"
    assert started.published_at == published.published_at  # header survived the broker round trip
    assert started.worker
    assert succeeded.runtime_ms is not None
    assert all(e.environment == "integration" for e in events)


@pytest.mark.integration
def test_real_worker_failure(celery_app: Celery, worker: Any, instrumentation: Any) -> None:
    _, transport = instrumentation
    result = celery_app.tasks["tests.boom"].delay()
    with pytest.raises(ValueError):
        result.get(timeout=20)
    events = _wait_for(transport, result.id, EventType.FAILED)
    assert [e.event_type for e in events][-1] == EventType.FAILED
    failed = events[-1]
    assert failed.exception is not None
    assert failed.exception.type == "ValueError" and "kaboom" in failed.exception.message
    assert failed.exception.traceback and "kaboom" in failed.exception.traceback


@pytest.mark.integration
def test_real_worker_retry(celery_app: Celery, worker: Any, instrumentation: Any) -> None:
    _, transport = instrumentation
    result = celery_app.tasks["tests.flaky"].delay()
    assert result.get(timeout=30) == "ok"
    events = _wait_for(transport, result.id, EventType.SUCCEEDED)
    kinds = [e.event_type for e in events]
    # Celery republishes the retried message *before* it fires ``task_retry``, so the second
    # PUBLISHED and the RETRIED event can arrive in either order.
    assert len(kinds) == 6
    assert kinds[:2] == [EventType.PUBLISHED, EventType.STARTED]
    assert sorted(kinds[2:4]) == sorted([EventType.PUBLISHED, EventType.RETRIED])
    assert kinds[4:] == [EventType.STARTED, EventType.SUCCEEDED]
    retried = next(e for e in events if e.event_type == EventType.RETRIED)
    assert retried.retries == 1
    assert retried.exception is not None and retried.exception.type == "RuntimeError"
