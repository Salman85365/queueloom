"""FastAPI / Starlette ``BackgroundTasks`` instrumentation.

Background tasks have no broker, no worker and no retries, but they are still background work
that fails silently. Track them with :func:`add_task`::

    from fastapi import BackgroundTasks
    from queueloom.sdk.fastapi import add_task, instrument

    instrument(endpoint="http://localhost:8800", api_key="ql_...", environment="prod")

    @app.post("/signup")
    async def signup(background_tasks: BackgroundTasks):
        add_task(background_tasks, send_welcome_email, "a@example.com")

Each tracked task appears in QueueLoom as a run on the ``background`` queue, published when it
is scheduled and started when the response has been sent.
"""

from __future__ import annotations

import logging
import os
import socket
import time
import traceback as tb_module
import uuid
from collections.abc import Callable
from typing import Any

from starlette.background import BackgroundTask, BackgroundTasks

from queueloom.events import (
    MAX_MESSAGE_CHARS,
    MAX_REPR_CHARS,
    MAX_TRACEBACK_CHARS,
    EventType,
    ExceptionInfo,
    TaskEvent,
    truncate,
    utcnow,
)
from queueloom.sdk.transport import HttpTransport, Transport

log = logging.getLogger("queueloom.sdk.fastapi")

ENV_ENDPOINT = "QUEUELOOM_ENDPOINT"
ENV_API_KEY = "QUEUELOOM_API_KEY"
ENV_ENVIRONMENT = "QUEUELOOM_ENVIRONMENT"
DEFAULT_QUEUE = "background"


class BackgroundInstrumentation:
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

    def flush(self, timeout: float | None = None) -> bool:
        return self.transport.flush(timeout)

    def close(self) -> None:
        self.transport.close()


_current: BackgroundInstrumentation | None = None


def instrument(
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    environment: str | None = None,
    transport: Transport | None = None,
    capture_args: bool = False,
    **transport_options: Any,
) -> BackgroundInstrumentation:
    global _current
    environment = environment or os.environ.get(ENV_ENVIRONMENT) or "default"
    if transport is None:
        endpoint = endpoint or os.environ.get(ENV_ENDPOINT)
        api_key = api_key or os.environ.get(ENV_API_KEY)
        if not endpoint or not api_key:
            raise ValueError(
                "queueloom.sdk.fastapi.instrument needs endpoint and api_key "
                f"(or {ENV_ENDPOINT} / {ENV_API_KEY} environment variables)"
            )
        transport = HttpTransport(endpoint, api_key, **transport_options)
    _current = BackgroundInstrumentation(
        transport, environment=environment, capture_args=capture_args
    )
    return _current


def uninstrument() -> None:
    global _current
    _current = None


def get_instrumentation() -> BackgroundInstrumentation | None:
    if _current is None and os.environ.get(ENV_ENDPOINT) and os.environ.get(ENV_API_KEY):
        return instrument()
    return _current


def _task_name(func: Callable[..., Any]) -> str:
    module = getattr(func, "__module__", None) or ""
    qualname = getattr(func, "__qualname__", None) or getattr(func, "__name__", repr(func))
    return f"{module}.{qualname}" if module else str(qualname)


class TrackedTask:
    """Wraps a background callable and reports its lifecycle."""

    def __init__(
        self,
        inst: BackgroundInstrumentation,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        name: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        self.inst = inst
        self.task = BackgroundTask(func, *args, **kwargs)
        self.task_id = uuid.uuid4().hex
        self.name = name or _task_name(func)
        self.queue = queue
        self.published_at = utcnow()
        extra: dict[str, Any] = {}
        if inst.capture_args:
            extra = {
                "args_repr": truncate(repr(args), MAX_REPR_CHARS),
                "kwargs_repr": truncate(repr(kwargs), MAX_REPR_CHARS),
            }
        self._extra = extra
        inst.emit(
            event_type=EventType.PUBLISHED,
            timestamp=self.published_at,
            task_id=self.task_id,
            task_name=self.name,
            queue=queue,
            published_at=self.published_at,
            retries=0,
        )

    async def __call__(self) -> None:
        common = {
            "task_id": self.task_id,
            "task_name": self.name,
            "queue": self.queue,
            "worker": self.inst.worker,
        }
        self.inst.emit(
            event_type=EventType.STARTED,
            published_at=self.published_at,
            retries=0,
            **common,
            **self._extra,
        )
        started = time.monotonic()
        try:
            await self.task()
        except BaseException as exc:
            self.inst.emit(
                event_type=EventType.FAILED,
                runtime_ms=(time.monotonic() - started) * 1000.0,
                retries=0,
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
        self.inst.emit(
            event_type=EventType.SUCCEEDED,
            runtime_ms=(time.monotonic() - started) * 1000.0,
            retries=0,
            **common,
        )


def add_task(
    background_tasks: BackgroundTasks,
    func: Callable[..., Any],
    *args: Any,
    queueloom_name: str | None = None,
    queueloom_queue: str = DEFAULT_QUEUE,
    **kwargs: Any,
) -> str | None:
    """Schedule ``func`` on ``background_tasks`` and track it. Returns the QueueLoom task id."""
    inst = get_instrumentation()
    if inst is None:
        background_tasks.add_task(func, *args, **kwargs)
        return None
    tracked = TrackedTask(inst, func, args, kwargs, name=queueloom_name, queue=queueloom_queue)
    background_tasks.add_task(tracked)
    return tracked.task_id


class QueueLoomBackgroundTasks(BackgroundTasks):
    """``BackgroundTasks`` whose ``add_task`` tracks every task automatically.

    Use it when you construct the object yourself (for example in a Starlette response);
    FastAPI's dependency injection always creates a plain ``BackgroundTasks``, so with FastAPI
    prefer :func:`add_task`.
    """

    def add_task(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        inst = get_instrumentation()
        if inst is None or isinstance(func, TrackedTask):
            super().add_task(func, *args, **kwargs)
            return
        super().add_task(TrackedTask(inst, func, args, kwargs))
