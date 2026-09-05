from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import dramatiq
import pytest
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware.retries import Retries

from queueloom.events import EventType
from queueloom.sdk.dramatiq import OPTION_PUBLISHED_AT, QueueLoomMiddleware, instrument
from queueloom.sdk.transport import MemoryTransport


@pytest.fixture
def broker() -> Iterator[StubBroker]:
    broker = StubBroker()
    broker.emit_after("process_boot")
    dramatiq.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.close()


@pytest.fixture
def transport(broker: StubBroker) -> MemoryTransport:
    transport = MemoryTransport()
    instrument(broker, transport=transport, environment="dramatiq-test", capture_args=True)
    return transport


@pytest.fixture
def worker(broker: StubBroker) -> Iterator[Worker]:
    worker = Worker(broker, worker_timeout=50, worker_threads=1)
    worker.start()
    yield worker
    worker.stop()


def _events(transport: MemoryTransport, task_id: str) -> list[Any]:
    return [e for e in transport.events if e.task_id == task_id]


def _wait_for(
    transport: MemoryTransport, task_id: str, terminal: EventType, timeout: float = 5
) -> list[Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = _events(transport, task_id)
        if any(e.event_type == terminal for e in events):
            return events
        time.sleep(0.01)
    raise AssertionError(f"no {terminal} for {task_id}: {_events(transport, task_id)}")


def test_middleware_is_placed_before_retries(
    broker: StubBroker, transport: MemoryTransport
) -> None:
    kinds = [type(m) for m in broker.middleware]
    assert kinds.index(QueueLoomMiddleware) < kinds.index(Retries)


def test_success_lifecycle(broker: StubBroker, transport: MemoryTransport, worker: Worker) -> None:
    @dramatiq.actor(queue_name="maths")
    def add(x: int, y: int) -> int:
        return x + y

    message = add.send(2, 3)
    broker.join("maths")
    worker.join()
    events = _wait_for(transport, message.message_id, EventType.SUCCEEDED)
    assert [e.event_type for e in events] == [
        EventType.PUBLISHED,
        EventType.STARTED,
        EventType.SUCCEEDED,
    ]
    published, started, succeeded = events
    assert published.task_name == "add" and published.queue == "maths"
    assert message.options[OPTION_PUBLISHED_AT]  # stamped into the message
    assert started.published_at == published.published_at
    assert started.args_repr == "(2, 3)" and started.worker
    assert succeeded.runtime_ms is not None and succeeded.runtime_ms >= 0
    assert all(e.environment == "dramatiq-test" for e in events)


def test_retry_then_success(broker: StubBroker, transport: MemoryTransport, worker: Worker) -> None:
    attempts = {"n": 0}

    @dramatiq.actor(max_retries=2, min_backoff=1, max_backoff=1)
    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("try again")
        return "ok"

    message = flaky.send()
    broker.join(flaky.queue_name, fail_fast=False)
    worker.join()
    events = _wait_for(transport, message.message_id, EventType.SUCCEEDED)
    kinds = [e.event_type for e in events]
    assert kinds[:2] == [EventType.PUBLISHED, EventType.STARTED]
    assert sorted(kinds[2:4]) == sorted([EventType.PUBLISHED, EventType.RETRIED])
    assert kinds[4:] == [EventType.STARTED, EventType.SUCCEEDED]
    retried = next(e for e in events if e.event_type == EventType.RETRIED)
    assert retried.retries == 1
    assert retried.exception is not None and retried.exception.type == "RuntimeError"
    assert retried.exception.traceback and "try again" in retried.exception.traceback
    assert attempts["n"] == 2


def test_failure_after_retries_exhausted(
    broker: StubBroker, transport: MemoryTransport, worker: Worker
) -> None:
    @dramatiq.actor(max_retries=1, min_backoff=1, max_backoff=1)
    def boom() -> None:
        raise ValueError("kaboom")

    message = boom.send()
    broker.join(boom.queue_name, fail_fast=False)
    worker.join()
    events = _wait_for(transport, message.message_id, EventType.FAILED)
    kinds = [e.event_type for e in events]
    assert kinds.count(EventType.PUBLISHED) == 2  # delay-queue moves are not re-published
    assert kinds.count(EventType.RETRIED) == 1
    assert kinds[-1] == EventType.FAILED
    failed = events[-1]
    assert failed.exception is not None and failed.exception.type == "ValueError"
    assert failed.retries == 1  # one retry happened; Dramatiq's own counter says 2 failures
    assert failed.args_repr == "()"


def test_failure_without_retries_middleware() -> None:
    broker = StubBroker(middleware=[])
    broker.emit_after("process_boot")
    dramatiq.set_broker(broker)
    transport = MemoryTransport()
    instrument(broker, transport=transport)
    assert [type(m) for m in broker.middleware] == [QueueLoomMiddleware]

    @dramatiq.actor
    def boom() -> None:
        raise ValueError("no retries here")

    worker = Worker(broker, worker_timeout=50, worker_threads=1)
    worker.start()
    try:
        message = boom.send()
        broker.join(boom.queue_name, fail_fast=False)
        worker.join()
        events = _wait_for(transport, message.message_id, EventType.FAILED)
        assert [e.event_type for e in events] == [
            EventType.PUBLISHED,
            EventType.STARTED,
            EventType.FAILED,
        ]
    finally:
        worker.stop()
        broker.close()


def test_instrument_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    with pytest.raises(ValueError):
        instrument(StubBroker())
