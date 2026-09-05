from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import fakeredis
import pytest
from rq import Retry

from queueloom.events import EventType
from queueloom.sdk.rq import (
    META_ATTEMPT,
    META_PUBLISHED_AT,
    QueueLoomQueue,
    QueueLoomSimpleWorker,
    get_instrumentation,
    instrument,
    uninstrument,
)
from queueloom.sdk.transport import MemoryTransport

# Job functions must be importable by the worker.
CALLS: dict[str, int] = {}


def add(x: int, y: int) -> int:
    return x + y


def boom() -> None:
    raise ValueError("kaboom")


def flaky() -> str:
    CALLS["flaky"] = CALLS.get("flaky", 0) + 1
    if CALLS["flaky"] == 1:
        raise RuntimeError("try again")
    return "ok"


@pytest.fixture
def connection() -> Any:
    return fakeredis.FakeStrictRedis()


@pytest.fixture
def transport() -> Iterator[MemoryTransport]:
    transport = MemoryTransport()
    instrument(transport=transport, environment="rq-test", capture_args=True)
    CALLS.clear()
    yield transport
    uninstrument()


def _events(transport: MemoryTransport, job_id: str) -> list[Any]:
    return [e for e in transport.events if e.task_id == job_id]


def _work(queue: QueueLoomQueue, connection: Any) -> None:
    worker = QueueLoomSimpleWorker([queue], connection=connection, queue_class=QueueLoomQueue)
    worker.work(burst=True, with_scheduler=False)


def test_success_lifecycle(connection: Any, transport: MemoryTransport) -> None:
    queue = QueueLoomQueue("maths", connection=connection)
    job = queue.enqueue(add, 2, 3)
    assert job.meta[META_PUBLISHED_AT] and job.meta[META_ATTEMPT] == 0
    _work(queue, connection)
    assert job.result == 5 if hasattr(job, "result") else True
    events = _events(transport, job.id)
    assert [e.event_type for e in events] == [
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.SUCCEEDED,
    ]
    published, started, succeeded = events
    assert published.queue == "maths" and published.task_name == "tests.test_sdk_rq.add"
    assert started.published_at == published.published_at
    assert started.worker and started.args_repr == "(2, 3)"
    assert succeeded.runtime_ms is not None and succeeded.runtime_ms >= 0
    assert all(e.environment == "rq-test" for e in events)


def test_failure_without_retry(connection: Any, transport: MemoryTransport) -> None:
    queue = QueueLoomQueue("default", connection=connection)
    job = queue.enqueue(boom)
    _work(queue, connection)
    events = _events(transport, job.id)
    assert [e.event_type for e in events][-1] == EventType.FAILED
    failed = events[-1]
    assert failed.exception is not None
    assert failed.exception.type == "ValueError" and failed.exception.message == "kaboom"
    assert failed.exception.traceback and "kaboom" in failed.exception.traceback
    assert failed.retries == 0 and failed.args_repr == "()"


def test_retry_then_success(connection: Any, transport: MemoryTransport) -> None:
    queue = QueueLoomQueue("default", connection=connection)
    job = queue.enqueue(flaky, retry=Retry(max=2))
    _work(queue, connection)
    events = _events(transport, job.id)
    kinds = [e.event_type for e in events]
    assert kinds == [
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.RETRIED,
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.SUCCEEDED,
    ]
    retried = events[2]
    assert retried.retries == 1
    assert retried.exception is not None and retried.exception.type == "RuntimeError"
    assert events[3].retries == 1 and events[5].retries == 1
    assert CALLS["flaky"] == 2


def test_scheduled_job_records_eta(connection: Any, transport: MemoryTransport) -> None:
    queue = QueueLoomQueue("default", connection=connection)
    when = datetime.now(UTC) + timedelta(hours=1)
    job = queue.enqueue_at(when, add, 1, 1)
    (published,) = _events(transport, job.id)
    assert published.event_type == EventType.PUBLISHED
    assert published.eta is not None and abs((published.eta - when).total_seconds()) < 1


def test_unconfigured_classes_are_no_ops(
    connection: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    uninstrument()
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    import queueloom.sdk.rq as rq_sdk

    monkeypatch.setattr(rq_sdk, "_warned", False)
    assert get_instrumentation() is None
    assert "not configured" in caplog.text
    queue = QueueLoomQueue("default", connection=connection)
    job = queue.enqueue(add, 1, 2)
    _work(queue, connection)
    assert job.get_status() == "finished"


def test_instrument_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    uninstrument()
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    with pytest.raises(ValueError):
        instrument()
