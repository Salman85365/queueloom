from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput

import queueloom.sdk.temporal as temporal_sdk
from queueloom.events import EventType
from queueloom.sdk.temporal import instrument, task_id_for
from queueloom.sdk.transport import MemoryTransport


class Terminal(ActivityInboundInterceptor):
    """Fake innermost interceptor: runs the activity function."""

    def __init__(self) -> None:
        pass

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        result = input.fn(*input.args)
        if asyncio.iscoroutine(result):
            result = await result
        return result


def _info(attempt: int = 1) -> Any:
    scheduled = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    return SimpleNamespace(
        activity_id="7",
        activity_type="charge_card",
        attempt=attempt,
        task_queue="billing",
        workflow_id="order-42",
        workflow_run_id="run-abc",
        scheduled_time=scheduled,
        current_attempt_scheduled_time=scheduled + timedelta(seconds=attempt - 1),
    )


def _call(fn: Any, *args: Any) -> ExecuteActivityInput:
    return ExecuteActivityInput(fn=fn, args=args, executor=None, headers={})


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> MemoryTransport:
    transport = MemoryTransport()
    monkeypatch.setattr(temporal_sdk, "current_info", lambda: _info())
    return transport


def test_task_id_combines_run_and_activity() -> None:
    assert task_id_for(_info()) == "run-abc:7"
    assert task_id_for(SimpleNamespace(activity_id="1")) == "-:1"


def test_activity_success(transport: MemoryTransport) -> None:
    interceptor = instrument(transport=transport, environment="temporal", capture_args=True)
    chain = interceptor.intercept_activity(Terminal())

    async def charge(amount: int) -> str:
        return f"charged {amount}"

    assert asyncio.run(chain.execute_activity(_call(charge, 12))) == "charged 12"
    started, succeeded = transport.events
    assert started.event_type == EventType.STARTED and succeeded.event_type == EventType.SUCCEEDED
    assert started.task_id == "run-abc:7" and started.task_name == "charge_card"
    assert started.queue == "billing" and started.retries == 0
    assert started.published_at == datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    assert started.root_id == "run-abc" and started.parent_id == "order-42"
    assert started.args_repr == "(12,)" and started.worker
    assert succeeded.runtime_ms is not None and all(
        e.environment == "temporal" for e in transport.events
    )


def test_activity_failure_and_retry_attempt(
    transport: MemoryTransport, monkeypatch: pytest.MonkeyPatch
) -> None:
    interceptor = instrument(transport=transport)
    chain = interceptor.intercept_activity(Terminal())

    def boom() -> None:
        raise TimeoutError("gateway timeout")

    with pytest.raises(TimeoutError):
        asyncio.run(chain.execute_activity(_call(boom)))
    failed = transport.events[-1]
    assert failed.event_type == EventType.FAILED and failed.retries == 0
    assert failed.exception is not None and failed.exception.type == "TimeoutError"
    assert failed.exception.traceback and "gateway timeout" in failed.exception.traceback

    # Temporal retries the same activity as attempt 2: same task id, retries=1.
    monkeypatch.setattr(temporal_sdk, "current_info", lambda: _info(attempt=2))

    def ok() -> str:
        return "fine"

    asyncio.run(chain.execute_activity(_call(ok)))
    started2, succeeded2 = transport.events[-2:]
    assert started2.task_id == failed.task_id and started2.retries == 1
    assert started2.published_at == datetime(2026, 9, 1, 12, 0, 1, tzinfo=UTC)
    assert succeeded2.event_type == EventType.SUCCEEDED


def test_cancellation_is_recorded_as_revoked(transport: MemoryTransport) -> None:
    chain = instrument(transport=transport).intercept_activity(Terminal())

    async def cancelled() -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(chain.execute_activity(_call(cancelled)))
    assert transport.events[-1].event_type == EventType.REVOKED


def test_outside_activity_context_just_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = MemoryTransport()

    def no_context() -> Any:
        raise RuntimeError("Not in activity context")

    monkeypatch.setattr(temporal_sdk, "current_info", no_context)
    chain = instrument(transport=transport).intercept_activity(Terminal())
    assert asyncio.run(chain.execute_activity(_call(lambda: 1))) == 1
    assert transport.events == []


def test_instrument_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    with pytest.raises(ValueError):
        instrument()
