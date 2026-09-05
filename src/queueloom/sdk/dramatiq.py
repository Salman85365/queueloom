"""Dramatiq instrumentation.

Dramatiq exposes a middleware API rather than signals, so QueueLoom ships as a middleware::

    import dramatiq
    from dramatiq.brokers.redis import RedisBroker
    from queueloom.sdk.dramatiq import instrument

    broker = RedisBroker()
    instrument(broker, endpoint="http://localhost:8800", api_key="ql_...", environment="prod")
    dramatiq.set_broker(broker)

The middleware is inserted *before* Dramatiq's ``Retries`` middleware. ``after_*`` hooks run in
reverse registration order, so ours runs after ``Retries`` has decided whether a failed message
will be retried (``message.failed`` is set when it will not). Retries re-enqueue a copy of the
message with the same ``message_id``, which QueueLoom folds into one task run.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import traceback as tb_module
from datetime import datetime
from typing import Any

from dramatiq.middleware import Middleware
from dramatiq.middleware.retries import Retries

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

log = logging.getLogger("queueloom.sdk.dramatiq")

OPTION_PUBLISHED_AT = "queueloom_published_at"
OPTION_ATTEMPT = "queueloom_attempt"

ENV_ENDPOINT = "QUEUELOOM_ENDPOINT"
ENV_API_KEY = "QUEUELOOM_API_KEY"
ENV_ENVIRONMENT = "QUEUELOOM_ENVIRONMENT"


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            return ensure_aware(datetime.fromisoformat(value))
        except ValueError:
            return None
    return None


class QueueLoomMiddleware(Middleware):
    """Emits QueueLoom task events for every Dramatiq message."""

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
        self.worker = f"{socket.gethostname()}:{os.getpid()}"
        self._starts: dict[str, float] = {}
        self._lock = threading.Lock()

    # -- helpers -------------------------------------------------------------------------

    def _emit(self, **fields: Any) -> None:
        try:
            self.transport.send(TaskEvent(environment=self.environment, **fields))
        except Exception:
            log.exception("queueloom: failed to emit %s", fields.get("event_type"))

    def _stop_timer(self, message_id: str) -> float | None:
        with self._lock:
            started = self._starts.pop(message_id, None)
        return None if started is None else (time.monotonic() - started) * 1000.0

    def _args_fields(self, message: Any) -> dict[str, str | None]:
        if not self.capture_args:
            return {}
        return {
            "args_repr": truncate(repr(tuple(message.args)), MAX_REPR_CHARS),
            "kwargs_repr": truncate(repr(dict(message.kwargs)), MAX_REPR_CHARS),
        }

    @staticmethod
    def _published_at(message: Any) -> datetime | None:
        options = getattr(message, "options", None) or {}
        published = _parse_dt(options.get(OPTION_PUBLISHED_AT))
        if published is not None:
            return published
        stamp = getattr(message, "message_timestamp", None)
        if isinstance(stamp, int | float):
            return ensure_aware(datetime.fromtimestamp(stamp / 1000.0))
        return None

    @staticmethod
    def _retries(message: Any) -> int:
        options = getattr(message, "options", None) or {}
        return int(options.get("retries", 0) or 0)

    # -- middleware hooks ----------------------------------------------------------------

    def before_enqueue(self, broker: Any, message: Any, delay: int) -> None:
        now = utcnow()
        attempt = self._retries(message)
        already_published = (
            OPTION_PUBLISHED_AT in message.options
            and message.options.get(OPTION_ATTEMPT) == attempt
        )
        message.options[OPTION_PUBLISHED_AT] = now.isoformat()
        message.options[OPTION_ATTEMPT] = attempt
        if already_published:
            # A delayed message being moved from the delay queue into its real queue: the same
            # attempt, now runnable. Queue latency is measured from here; no new publish event.
            return
        eta = None
        if delay:
            from datetime import timedelta

            eta = now + timedelta(milliseconds=delay)
        self._emit(
            event_type=EventType.PUBLISHED,
            timestamp=now,
            task_id=message.message_id,
            task_name=message.actor_name,
            queue=message.queue_name,
            published_at=now,
            eta=eta,
            retries=self._retries(message),
        )

    def before_process_message(self, broker: Any, message: Any) -> None:
        with self._lock:
            self._starts[message.message_id] = time.monotonic()
        self._emit(
            event_type=EventType.STARTED,
            task_id=message.message_id,
            task_name=message.actor_name,
            queue=message.queue_name,
            worker=self.worker,
            published_at=self._published_at(message),
            retries=self._retries(message),
            **self._args_fields(message),
        )

    def after_process_message(
        self,
        broker: Any,
        message: Any,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        runtime_ms = self._stop_timer(message.message_id)
        common = {
            "task_id": message.message_id,
            "task_name": message.actor_name,
            "queue": message.queue_name,
            "worker": self.worker,
            "runtime_ms": runtime_ms,
        }
        if exception is None:
            self._emit(event_type=EventType.SUCCEEDED, retries=self._retries(message), **common)
            return
        tb_text = message.options.get("traceback") or "".join(
            tb_module.format_exception(type(exception), exception, exception.__traceback__)
        )
        info = ExceptionInfo(
            type=type(exception).__name__[:200],
            message=truncate(str(exception), MAX_MESSAGE_CHARS) or "",
            traceback=truncate(tb_text, MAX_TRACEBACK_CHARS),
        )
        has_retries = any(isinstance(m, Retries) for m in broker.middleware)
        will_retry = has_retries and not getattr(message, "failed", False)
        if will_retry:
            # Retries already bumped options["retries"] for the re-enqueued copy.
            self._emit(
                event_type=EventType.RETRIED,
                retries=self._retries(message),
                exception=info,
                **common,
            )
        else:
            # Retries counts failures (it bumps before deciding to give up); report retries done.
            retries = max(0, self._retries(message) - 1) if has_retries else 0
            self._emit(
                event_type=EventType.FAILED,
                retries=retries,
                exception=info,
                **common,
                **self._args_fields(message),
            )

    def after_skip_message(self, broker: Any, message: Any) -> None:
        self._stop_timer(message.message_id)
        self._emit(
            event_type=EventType.REVOKED,
            task_id=message.message_id,
            task_name=message.actor_name,
            queue=message.queue_name,
            worker=self.worker,
            exception=ExceptionInfo(type="Skipped", message="skipped by middleware"),
        )


def instrument(
    broker: Any,
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    environment: str | None = None,
    transport: Transport | None = None,
    capture_args: bool = False,
    **transport_options: Any,
) -> QueueLoomMiddleware:
    """Attach QueueLoom to a Dramatiq broker and return the middleware."""
    environment = environment or os.environ.get(ENV_ENVIRONMENT) or "default"
    if transport is None:
        endpoint = endpoint or os.environ.get(ENV_ENDPOINT)
        api_key = api_key or os.environ.get(ENV_API_KEY)
        if not endpoint or not api_key:
            raise ValueError(
                "queueloom.sdk.dramatiq.instrument needs endpoint and api_key "
                f"(or {ENV_ENDPOINT} / {ENV_API_KEY} environment variables)"
            )
        transport = HttpTransport(endpoint, api_key, **transport_options)
    middleware = QueueLoomMiddleware(transport, environment=environment, capture_args=capture_args)
    has_retries = any(isinstance(m, Retries) for m in broker.middleware)
    if has_retries:
        broker.add_middleware(middleware, before=Retries)
    else:
        broker.add_middleware(middleware)
    return middleware
