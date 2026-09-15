"""Celery instrumentation.

Hooks Celery's signals and emits a :class:`~queueloom.events.TaskEvent` for every lifecycle
transition. The publish hook also injects a ``queueloom_published_at`` message header, so
instrumented workers can calculate queue latency from an instrumented producer's timestamp.
Worker-only instrumentation cannot reconstruct a missing original publish timestamp.

Usage::

    from celery import Celery
    from queueloom.sdk.celery import instrument

    app = Celery(...)
    instrument(app, endpoint="http://localhost:8800", api_key="ql_...", environment="prod")
"""

from __future__ import annotations

import logging
import os
import threading
import time
import traceback as tb_module
from datetime import datetime
from typing import Any

from celery import signals

from queueloom.events import (
    MAX_MESSAGE_CHARS,
    MAX_REPR_CHARS,
    MAX_TRACEBACK_CHARS,
    EventType,
    ExceptionInfo,
    TaskEvent,
    ensure_aware,
    truncate,
    utcnow,
)
from queueloom.sdk.transport import HttpTransport, Transport

log = logging.getLogger("queueloom.sdk.celery")

HEADER_PUBLISHED_AT = "queueloom_published_at"

ENV_ENDPOINT = "QUEUELOOM_ENDPOINT"
ENV_API_KEY = "QUEUELOOM_API_KEY"
ENV_ENVIRONMENT = "QUEUELOOM_ENVIRONMENT"


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_aware(value)
    if isinstance(value, str):
        try:
            return ensure_aware(datetime.fromisoformat(value))
        except ValueError:
            return None
    return None


def _request_get(request: Any, key: str) -> Any:
    """Read a custom header from a task request, tolerating Celery protocol differences."""
    if request is None:
        return None
    getter = getattr(request, "get", None)
    value = getter(key) if callable(getter) else getattr(request, key, None)
    if value is None:
        headers = getattr(request, "headers", None)
        if isinstance(headers, dict):
            value = headers.get(key)
    return value


def _queue_from_request(request: Any) -> str | None:
    info = getattr(request, "delivery_info", None) or {}
    if isinstance(info, dict):
        routing_key = info.get("routing_key")
        if routing_key:
            return str(routing_key)
    return None


def _exception_info(exc: BaseException | Any, tb_text: str | None) -> ExceptionInfo:
    if isinstance(exc, BaseException):
        exc_type = type(exc).__name__
        message = str(exc)
    else:
        exc_type = type(exc).__name__ if exc is not None else "Unknown"
        message = str(exc) if exc is not None else ""
    return ExceptionInfo(
        type=exc_type[:200],
        message=truncate(message, MAX_MESSAGE_CHARS) or "",
        traceback=truncate(tb_text, MAX_TRACEBACK_CHARS),
    )


class CeleryInstrumentation:
    """Connects/disconnects Celery signal handlers and forwards events to a transport."""

    def __init__(
        self,
        transport: Transport,
        *,
        environment: str = "default",
        capture_args: bool = False,
    ) -> None:
        self.transport = transport
        self.environment = environment
        self.capture_args = capture_args
        self._starts: dict[str, float] = {}
        self._lock = threading.Lock()
        self._connected = False

    # -- lifecycle -----------------------------------------------------------------------

    def connect(self) -> None:
        if self._connected:
            return
        signals.before_task_publish.connect(self.on_before_task_publish, weak=False)
        signals.task_prerun.connect(self.on_task_prerun, weak=False)
        signals.task_success.connect(self.on_task_success, weak=False)
        signals.task_failure.connect(self.on_task_failure, weak=False)
        signals.task_retry.connect(self.on_task_retry, weak=False)
        signals.task_revoked.connect(self.on_task_revoked, weak=False)
        self._connected = True

    def disconnect(self) -> None:
        if not self._connected:
            return
        signals.before_task_publish.disconnect(self.on_before_task_publish)
        signals.task_prerun.disconnect(self.on_task_prerun)
        signals.task_success.disconnect(self.on_task_success)
        signals.task_failure.disconnect(self.on_task_failure)
        signals.task_retry.disconnect(self.on_task_retry)
        signals.task_revoked.disconnect(self.on_task_revoked)
        self._connected = False

    def flush(self, timeout: float | None = None) -> bool:
        return self.transport.flush(timeout)

    def close(self) -> None:
        self.disconnect()
        self.transport.close()

    # -- helpers -------------------------------------------------------------------------

    def _emit(self, **fields: Any) -> None:
        """Build and send an event. Never raises into user code."""
        try:
            event = TaskEvent(environment=self.environment, **fields)
            self.transport.send(event)
        except Exception:
            log.exception("queueloom: failed to emit %s", fields.get("event_type"))

    def _start_timer(self, task_id: str) -> None:
        with self._lock:
            self._starts[task_id] = time.monotonic()

    def _stop_timer(self, task_id: str | None) -> float | None:
        if task_id is None:
            return None
        with self._lock:
            started = self._starts.pop(task_id, None)
        if started is None:
            return None
        return (time.monotonic() - started) * 1000.0

    def _args_fields(self, args: Any, kwargs: Any) -> dict[str, str | None]:
        if not self.capture_args:
            return {}
        return {
            "args_repr": truncate(repr(args), MAX_REPR_CHARS) if args is not None else None,
            "kwargs_repr": truncate(repr(kwargs), MAX_REPR_CHARS) if kwargs is not None else None,
        }

    # -- signal handlers -----------------------------------------------------------------

    def on_before_task_publish(
        self,
        sender: str | None = None,
        body: Any = None,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        **_: Any,
    ) -> None:
        now = utcnow()
        if headers is None:
            headers = {}
        headers[HEADER_PUBLISHED_AT] = now.isoformat()

        meta: dict[str, Any] = headers
        if "id" not in headers and isinstance(body, dict):
            meta = body  # message protocol 1 keeps task metadata in the body
        task_id = meta.get("id")
        if task_id is None:
            return
        self._emit(
            event_type=EventType.PUBLISHED,
            timestamp=now,
            task_id=str(task_id),
            task_name=meta.get("task") or sender,
            queue=routing_key,
            published_at=now,
            retries=meta.get("retries") or 0,
            eta=_parse_dt(meta.get("eta")),
            parent_id=meta.get("parent_id"),
            root_id=meta.get("root_id"),
        )

    def on_task_prerun(
        self,
        sender: Any = None,
        task_id: str | None = None,
        task: Any = None,
        args: Any = None,
        kwargs: Any = None,
        **_: Any,
    ) -> None:
        task = task or sender
        request = getattr(task, "request", None)
        task_id = task_id or getattr(request, "id", None)
        if task_id is None:
            return
        self._start_timer(task_id)
        self._emit(
            event_type=EventType.STARTED,
            task_id=task_id,
            task_name=getattr(task, "name", None),
            queue=_queue_from_request(request),
            worker=getattr(request, "hostname", None),
            published_at=_parse_dt(_request_get(request, HEADER_PUBLISHED_AT)),
            retries=getattr(request, "retries", None) or 0,
            eta=_parse_dt(getattr(request, "eta", None)),
            parent_id=getattr(request, "parent_id", None),
            root_id=getattr(request, "root_id", None),
            **self._args_fields(args, kwargs),
        )

    def on_task_success(self, sender: Any = None, result: Any = None, **_: Any) -> None:
        request = getattr(sender, "request", None)
        task_id = getattr(request, "id", None)
        if task_id is None:
            return
        self._emit(
            event_type=EventType.SUCCEEDED,
            task_id=task_id,
            task_name=getattr(sender, "name", None),
            worker=getattr(request, "hostname", None),
            retries=getattr(request, "retries", None) or 0,
            runtime_ms=self._stop_timer(task_id),
        )

    def on_task_failure(
        self,
        sender: Any = None,
        task_id: str | None = None,
        exception: BaseException | None = None,
        args: Any = None,
        kwargs: Any = None,
        traceback: Any = None,
        einfo: Any = None,
        **_: Any,
    ) -> None:
        request = getattr(sender, "request", None)
        task_id = task_id or getattr(request, "id", None)
        if task_id is None:
            return
        tb_text: str | None = getattr(einfo, "traceback", None)
        if tb_text is None and exception is not None and traceback is not None:
            tb_text = "".join(tb_module.format_exception(type(exception), exception, traceback))
        self._emit(
            event_type=EventType.FAILED,
            task_id=task_id,
            task_name=getattr(sender, "name", None),
            worker=getattr(request, "hostname", None),
            retries=getattr(request, "retries", None) or 0,
            runtime_ms=self._stop_timer(task_id),
            exception=_exception_info(exception, tb_text),
            **self._args_fields(args, kwargs),
        )

    def on_task_retry(
        self,
        sender: Any = None,
        request: Any = None,
        reason: Any = None,
        einfo: Any = None,
        **_: Any,
    ) -> None:
        task_id = getattr(request, "id", None)
        if task_id is None:
            return
        tb_text = getattr(einfo, "traceback", None)
        # ``reason`` is usually a celery.exceptions.Retry wrapping the real cause in ``.exc``.
        cause = getattr(reason, "exc", None)
        if isinstance(cause, BaseException):
            reason = cause
        self._emit(
            event_type=EventType.RETRIED,
            task_id=task_id,
            task_name=getattr(sender, "name", None) or getattr(request, "task", None),
            worker=getattr(request, "hostname", None),
            retries=(getattr(request, "retries", None) or 0) + 1,
            runtime_ms=self._stop_timer(task_id),
            exception=_exception_info(reason, tb_text),
        )

    def on_task_revoked(
        self,
        sender: Any = None,
        request: Any = None,
        terminated: bool | None = None,
        signum: Any = None,
        expired: bool | None = None,
        **_: Any,
    ) -> None:
        task_id = getattr(request, "id", None)
        if task_id is None:
            return
        reason = "terminated" if terminated else ("expired" if expired else "revoked")
        self._stop_timer(task_id)
        self._emit(
            event_type=EventType.REVOKED,
            task_id=task_id,
            task_name=getattr(request, "name", None) or getattr(sender, "name", None),
            worker=getattr(request, "hostname", None),
            exception=ExceptionInfo(type="Revoked", message=reason),
        )


def instrument(
    app: Any = None,
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    environment: str | None = None,
    transport: Transport | None = None,
    capture_args: bool = False,
    **transport_options: Any,
) -> CeleryInstrumentation:
    """Instrument Celery and start shipping events.

    ``endpoint``/``api_key``/``environment`` fall back to the ``QUEUELOOM_ENDPOINT``,
    ``QUEUELOOM_API_KEY`` and ``QUEUELOOM_ENVIRONMENT`` environment variables. Pass an explicit
    ``transport`` (for example :class:`~queueloom.sdk.MemoryTransport`) to bypass HTTP.

    Celery signals are process-global, so ``app`` is only accepted for readability and future
    per-app configuration; instrumentation applies to every Celery app in the process.
    """
    environment = environment or os.environ.get(ENV_ENVIRONMENT) or "default"
    if transport is None:
        endpoint = endpoint or os.environ.get(ENV_ENDPOINT)
        api_key = api_key or os.environ.get(ENV_API_KEY)
        if not endpoint or not api_key:
            raise ValueError(
                "queueloom.sdk.celery.instrument needs endpoint and api_key "
                f"(or {ENV_ENDPOINT} / {ENV_API_KEY} environment variables)"
            )
        transport = HttpTransport(endpoint, api_key, **transport_options)
    instrumentation = CeleryInstrumentation(
        transport, environment=environment, capture_args=capture_args
    )
    instrumentation.connect()
    return instrumentation
