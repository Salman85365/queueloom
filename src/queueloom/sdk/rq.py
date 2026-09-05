"""RQ (Redis Queue) instrumentation.

RQ has neither signals nor middleware, so QueueLoom provides drop-in ``Queue`` and ``Worker``
subclasses. Configure once (or via ``QUEUELOOM_*`` environment variables) and use them::

    from redis import Redis
    from queueloom.sdk.rq import QueueLoomQueue, QueueLoomWorker, instrument

    instrument(endpoint="http://localhost:8800", api_key="ql_...", environment="prod")
    queue = QueueLoomQueue("emails", connection=Redis())
    queue.enqueue(send_email, "a@example.com")

and run workers with ``QUEUELOOM_ENDPOINT`` / ``QUEUELOOM_API_KEY`` set::

    rq worker -w queueloom.sdk.rq.QueueLoomWorker \\
        --queue-class queueloom.sdk.rq.QueueLoomQueue emails

The publish side stamps ``job.meta`` with the enqueue time and attempt number, so workers report
queue latency and retries even when producers use a plain ``Queue``.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from typing import Any

from redis.client import Pipeline
from rq import Queue, SimpleWorker, Worker
from rq.job import Job

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

log = logging.getLogger("queueloom.sdk.rq")

META_PUBLISHED_AT = "queueloom_published_at"
META_ATTEMPT = "queueloom_attempt"

ENV_ENDPOINT = "QUEUELOOM_ENDPOINT"
ENV_API_KEY = "QUEUELOOM_API_KEY"
ENV_ENVIRONMENT = "QUEUELOOM_ENVIRONMENT"


class RQInstrumentation:
    """Process-wide configuration shared by the queue and worker classes."""

    def __init__(
        self, transport: Transport, *, environment: str = "default", capture_args: bool = False
    ) -> None:
        self.transport = transport
        self.environment = environment
        self.capture_args = capture_args
        self._starts: dict[str, float] = {}
        self._lock = threading.Lock()

    def emit(self, **fields: Any) -> None:
        try:
            self.transport.send(TaskEvent(environment=self.environment, **fields))
        except Exception:
            log.exception("queueloom: failed to emit %s", fields.get("event_type"))

    def start_timer(self, job_id: str) -> None:
        with self._lock:
            self._starts[job_id] = time.monotonic()

    def stop_timer(self, job_id: str) -> float | None:
        with self._lock:
            started = self._starts.pop(job_id, None)
        return None if started is None else (time.monotonic() - started) * 1000.0

    def flush(self, timeout: float | None = None) -> bool:
        return self.transport.flush(timeout)

    def close(self) -> None:
        self.transport.close()


_current: RQInstrumentation | None = None
_warned = False


def instrument(
    *,
    endpoint: str | None = None,
    api_key: str | None = None,
    environment: str | None = None,
    transport: Transport | None = None,
    capture_args: bool = False,
    **transport_options: Any,
) -> RQInstrumentation:
    """Configure QueueLoom for RQ in this process."""
    global _current
    environment = environment or os.environ.get(ENV_ENVIRONMENT) or "default"
    if transport is None:
        endpoint = endpoint or os.environ.get(ENV_ENDPOINT)
        api_key = api_key or os.environ.get(ENV_API_KEY)
        if not endpoint or not api_key:
            raise ValueError(
                "queueloom.sdk.rq.instrument needs endpoint and api_key "
                f"(or {ENV_ENDPOINT} / {ENV_API_KEY} environment variables)"
            )
        transport = HttpTransport(endpoint, api_key, **transport_options)
    _current = RQInstrumentation(transport, environment=environment, capture_args=capture_args)
    return _current


def uninstrument() -> None:
    global _current
    _current = None


def get_instrumentation() -> RQInstrumentation | None:
    """Return the active instrumentation, configuring from the environment on first use."""
    global _current, _warned
    if _current is not None:
        return _current
    if os.environ.get(ENV_ENDPOINT) and os.environ.get(ENV_API_KEY):
        return instrument()
    if not _warned:
        _warned = True
        log.warning(
            "queueloom: RQ classes in use but not configured; call queueloom.sdk.rq.instrument() "
            "or set %s and %s",
            ENV_ENDPOINT,
            ENV_API_KEY,
        )
    return None


def _job_meta(job: Any) -> dict[str, Any]:
    meta = getattr(job, "meta", None)
    if not isinstance(meta, dict):
        meta = {}
        job.meta = meta
    return meta


def _published_at(job: Any) -> datetime | None:
    value = _job_meta(job).get(META_PUBLISHED_AT)
    if isinstance(value, str):
        try:
            return ensure_aware(datetime.fromisoformat(value))
        except ValueError:
            return None
    enqueued = getattr(job, "enqueued_at", None)
    return ensure_aware(enqueued) if isinstance(enqueued, datetime) else None


def _attempt(job: Any) -> int:
    return int(_job_meta(job).get(META_ATTEMPT, 0) or 0)


def _args_fields(inst: RQInstrumentation, job: Any) -> dict[str, str | None]:
    if not inst.capture_args:
        return {}
    return {
        "args_repr": truncate(repr(tuple(getattr(job, "args", ()) or ())), MAX_REPR_CHARS),
        "kwargs_repr": truncate(repr(dict(getattr(job, "kwargs", {}) or {})), MAX_REPR_CHARS),
    }


def _publish(job: Any, queue_name: str, eta: datetime | None = None) -> None:
    inst = get_instrumentation()
    if inst is None:
        return
    meta = _job_meta(job)
    now = utcnow()
    attempt = meta[META_ATTEMPT] + 1 if META_ATTEMPT in meta else 0
    meta[META_PUBLISHED_AT] = now.isoformat()
    meta[META_ATTEMPT] = attempt
    inst.emit(
        event_type=EventType.PUBLISHED,
        timestamp=now,
        task_id=job.id,
        task_name=getattr(job, "func_name", None),
        queue=queue_name,
        published_at=now,
        eta=eta,
        retries=attempt,
    )


class QueueLoomQueue(Queue):
    """``rq.Queue`` that records publishes (including retries and scheduled jobs)."""

    def _enqueue_job(
        self,
        job: Job,
        pipeline: Pipeline | None = None,
        at_front: bool = False,
        unique: bool = False,
    ) -> Job:
        _publish(job, self.name)
        return super()._enqueue_job(job, pipeline=pipeline, at_front=at_front, unique=unique)

    def schedule_job(
        self,
        job: Job,
        datetime: datetime,
        pipeline: Pipeline | None = None,
        unique: bool = False,
    ) -> Job:
        _publish(job, self.name, eta=ensure_aware(datetime))
        return super().schedule_job(job, datetime, pipeline=pipeline, unique=unique)


class QueueLoomWorkerMixin:
    """Worker hooks shared by the forking and non-forking worker classes."""

    queue_class = QueueLoomQueue
    _hostname: str | None = None

    def _worker_name(self) -> str | None:
        return getattr(self, "name", None) or getattr(self, "hostname", None)

    def prepare_job_execution(self, job: Any, *args: Any, **kwargs: Any) -> None:
        super().prepare_job_execution(job, *args, **kwargs)  # type: ignore[misc]
        inst = get_instrumentation()
        if inst is None:
            return
        inst.start_timer(job.id)
        inst.emit(
            event_type=EventType.STARTED,
            task_id=job.id,
            task_name=getattr(job, "func_name", None),
            queue=getattr(job, "origin", None),
            worker=self._worker_name(),
            published_at=_published_at(job),
            retries=_attempt(job),
            **_args_fields(inst, job),
        )

    def handle_job_success(self, job: Any, queue: Any, started_job_registry: Any) -> None:
        inst = get_instrumentation()
        if inst is not None:
            inst.emit(
                event_type=EventType.SUCCEEDED,
                task_id=job.id,
                task_name=getattr(job, "func_name", None),
                queue=getattr(job, "origin", None),
                worker=self._worker_name(),
                retries=_attempt(job),
                runtime_ms=inst.stop_timer(job.id),
            )
        super().handle_job_success(job, queue, started_job_registry)  # type: ignore[misc]

    def handle_job_retry(self, job: Any, queue: Any, retry: Any, *args: Any, **kwargs: Any) -> None:
        inst = get_instrumentation()
        if inst is not None:
            inst.emit(
                event_type=EventType.RETRIED,
                task_id=job.id,
                task_name=getattr(job, "func_name", None),
                queue=getattr(job, "origin", None),
                worker=self._worker_name(),
                retries=_attempt(job) + 1,
                runtime_ms=inst.stop_timer(job.id),
                exception=ExceptionInfo(type="Retry", message="job returned Retry"),
            )
        super().handle_job_retry(job, queue, retry, *args, **kwargs)  # type: ignore[misc]

    def handle_job_failure(
        self, job: Any, queue: Any, started_job_registry: Any = None, exc_string: str = ""
    ) -> None:
        inst = get_instrumentation()
        if inst is not None:
            will_retry = bool(getattr(job, "should_retry", False))
            exc_type, message = _split_exc_string(exc_string)
            info = ExceptionInfo(
                type=exc_type,
                message=truncate(message, MAX_MESSAGE_CHARS) or "",
                traceback=truncate(exc_string or None, MAX_TRACEBACK_CHARS),
            )
            inst.emit(
                event_type=EventType.RETRIED if will_retry else EventType.FAILED,
                task_id=job.id,
                task_name=getattr(job, "func_name", None),
                queue=getattr(job, "origin", None),
                worker=self._worker_name(),
                retries=_attempt(job) + 1 if will_retry else _attempt(job),
                runtime_ms=inst.stop_timer(job.id),
                exception=info,
                **({} if will_retry else _args_fields(inst, job)),
            )
        super().handle_job_failure(  # type: ignore[misc]
            job, queue, started_job_registry=started_job_registry, exc_string=exc_string
        )


def _split_exc_string(exc_string: str) -> tuple[str, str]:
    """Best-effort ``('ValueError', 'boom')`` from a formatted traceback."""
    for line in reversed((exc_string or "").strip().splitlines()):
        line = line.strip()
        if not line or line.startswith(("File ", "Traceback")):
            continue
        exc_type, sep, message = line.partition(":")
        if sep and " " not in exc_type.strip():
            return exc_type.strip().rsplit(".", 1)[-1][:200], message.strip()
        return line.rsplit(".", 1)[-1][:200], ""
    return "Exception", ""


class QueueLoomWorker(QueueLoomWorkerMixin, Worker):
    """Forking ``rq.Worker`` with QueueLoom instrumentation."""


class QueueLoomSimpleWorker(QueueLoomWorkerMixin, SimpleWorker):
    """Non-forking worker (tests, Windows, debugging) with QueueLoom instrumentation."""


__all__ = [
    "META_ATTEMPT",
    "META_PUBLISHED_AT",
    "QueueLoomQueue",
    "QueueLoomSimpleWorker",
    "QueueLoomWorker",
    "RQInstrumentation",
    "get_instrumentation",
    "instrument",
    "uninstrument",
]
