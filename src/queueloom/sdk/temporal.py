"""Temporal instrumentation (activities).

Temporal workflows are durable orchestrations; the unit of background *work* is the activity,
so QueueLoom records one run per activity execution, folding retries (attempts) into it.
Install the interceptor on your worker::

    from temporalio.worker import Worker
    from queueloom.sdk.temporal import QueueLoomInterceptor, instrument

    interceptor = instrument(endpoint="http://localhost:8800", api_key="ql_...", environment="prod")
    worker = Worker(client, task_queue="billing", activities=[...], interceptors=[interceptor])

Mapping: ``task_id`` = ``<workflow run id>:<activity id>``, ``task_name`` = activity type,
``queue`` = task queue, ``retries`` = attempt - 1, ``published_at`` = the attempt's scheduled
time (so queue latency is the schedule-to-start delay Temporal itself reports).
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import traceback as tb_module
from datetime import datetime
from typing import Any

from temporalio import activity
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)

from queueloom.events import (
    MAX_MESSAGE_CHARS,
    MAX_REPR_CHARS,
    MAX_TRACEBACK_CHARS,
    EventType,
    ExceptionInfo,
    TaskEvent,
    ensure_aware,
    truncate,
)
from queueloom.sdk.transport import HttpTransport, Transport

log = logging.getLogger("queueloom.sdk.temporal")

ENV_ENDPOINT = "QUEUELOOM_ENDPOINT"
ENV_API_KEY = "QUEUELOOM_API_KEY"
ENV_ENVIRONMENT = "QUEUELOOM_ENVIRONMENT"


def current_info() -> Any:
    """Indirection over :func:`temporalio.activity.info` (patched in tests)."""
    return activity.info()


def task_id_for(info: Any) -> str:
    run_id = getattr(info, "workflow_run_id", None) or getattr(info, "workflow_id", None) or "-"
    return f"{run_id}:{getattr(info, 'activity_id', '?')}"


def _dt(value: Any) -> datetime | None:
    return ensure_aware(value) if isinstance(value, datetime) else None


class QueueLoomActivityInterceptor(ActivityInboundInterceptor):
    def __init__(
        self, next_interceptor: ActivityInboundInterceptor, owner: QueueLoomInterceptor
    ) -> None:
        super().__init__(next_interceptor)
        self.owner = owner

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        owner = self.owner
        try:
            info = current_info()
        except Exception:
            return await self.next.execute_activity(input)
        task_id = task_id_for(info)
        attempt = int(getattr(info, "attempt", 1) or 1)
        common: dict[str, Any] = {
            "task_id": task_id,
            "task_name": getattr(info, "activity_type", None),
            "queue": getattr(info, "task_queue", None),
            "worker": owner.worker,
            "root_id": getattr(info, "workflow_run_id", None),
            "parent_id": getattr(info, "workflow_id", None),
        }
        published_at = _dt(getattr(info, "current_attempt_scheduled_time", None)) or _dt(
            getattr(info, "scheduled_time", None)
        )
        extra: dict[str, Any] = {}
        if owner.capture_args:
            extra["args_repr"] = truncate(repr(tuple(input.args)), MAX_REPR_CHARS)
        owner.emit(
            event_type=EventType.STARTED,
            published_at=published_at,
            retries=attempt - 1,
            **common,
            **extra,
        )
        started = time.monotonic()
        try:
            result = await self.next.execute_activity(input)
        except asyncio.CancelledError:
            owner.emit(
                event_type=EventType.REVOKED,
                retries=attempt - 1,
                runtime_ms=(time.monotonic() - started) * 1000.0,
                exception=ExceptionInfo(type="Cancelled", message="activity cancelled"),
                **common,
            )
            raise
        except BaseException as exc:
            owner.emit(
                event_type=EventType.FAILED,
                retries=attempt - 1,
                runtime_ms=(time.monotonic() - started) * 1000.0,
                exception=ExceptionInfo(
                    type=type(exc).__name__[:200],
                    message=truncate(str(exc), MAX_MESSAGE_CHARS) or "",
                    traceback=truncate(
                        "".join(tb_module.format_exception(type(exc), exc, exc.__traceback__)),
                        MAX_TRACEBACK_CHARS,
                    ),
                ),
                **common,
            )
            raise
        owner.emit(
            event_type=EventType.SUCCEEDED,
            retries=attempt - 1,
            runtime_ms=(time.monotonic() - started) * 1000.0,
            **common,
        )
        return result


class QueueLoomInterceptor(Interceptor):
    """Worker interceptor; pass an instance in ``Worker(interceptors=[...])``."""

    def __init__(
        self, transport: Transport, *, environment: str = "default", capture_args: bool = False
    ) -> None:
        self.transport = transport
        self.environment = environment
        self.capture_args = capture_args
        self.worker = f"{socket.gethostname()}:{os.getpid()}"

    def emit(self, **fields: Any) -> None:
        try:
            self.transport.send(TaskEvent(environment=self.environment, **fields))
        except Exception:
            log.exception("queueloom: failed to emit %s", fields.get("event_type"))

    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,
    ) -> ActivityInboundInterceptor:
        return QueueLoomActivityInterceptor(next, self)

    def flush(self, timeout: float | None = None) -> bool:
        return self.transport.flush(timeout)

    def close(self) -> None:
        self.transport.close()


def instrument(
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    environment: str | None = None,
    transport: Transport | None = None,
    capture_args: bool = False,
    **transport_options: Any,
) -> QueueLoomInterceptor:
    """Build a :class:`QueueLoomInterceptor` from arguments or ``QUEUELOOM_*`` variables."""
    environment = environment or os.environ.get(ENV_ENVIRONMENT) or "default"
    if transport is None:
        endpoint = endpoint or os.environ.get(ENV_ENDPOINT)
        api_key = api_key or os.environ.get(ENV_API_KEY)
        if not endpoint or not api_key:
            raise ValueError(
                "queueloom.sdk.temporal.instrument needs endpoint and api_key "
                f"(or {ENV_ENDPOINT} / {ENV_API_KEY} environment variables)"
            )
        transport = HttpTransport(endpoint, api_key, **transport_options)
    return QueueLoomInterceptor(transport, environment=environment, capture_args=capture_args)


__all__: list[str] = [
    "QueueLoomActivityInterceptor",
    "QueueLoomInterceptor",
    "current_info",
    "instrument",
    "task_id_for",
]
