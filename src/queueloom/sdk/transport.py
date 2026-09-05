"""Transports move events from the instrumented process to the QueueLoom server.

Design goals:

* Never block or raise inside the instrumented code path. ``send`` is a non-blocking queue
  put; failures are counted and logged, not propagated.
* Bounded memory. When the queue is full, new events are dropped (and counted) rather than
  growing without limit.
* Fork safe. Celery's prefork pool forks worker processes; a background thread does not
  survive ``fork``. The transport notices a PID change and lazily restarts itself.
"""

from __future__ import annotations

import atexit
import logging
import os
import queue
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

import httpx

from queueloom import __version__
from queueloom.events import TaskEvent

log = logging.getLogger("queueloom.sdk")


class Transport(Protocol):
    def send(self, event: TaskEvent) -> None: ...

    def flush(self, timeout: float | None = None) -> bool: ...

    def close(self) -> None: ...


class MemoryTransport:
    """Collects events in memory. Useful for tests and local debugging."""

    def __init__(self) -> None:
        self.events: list[TaskEvent] = []
        self._lock = threading.Lock()

    def send(self, event: TaskEvent) -> None:
        with self._lock:
            self.events.append(event)

    def flush(self, timeout: float | None = None) -> bool:
        return True

    def close(self) -> None:
        return None

    def clear(self) -> None:
        with self._lock:
            self.events.clear()


class HttpTransport:
    """Batches events and POSTs them to ``<endpoint>/v1/events`` from a daemon thread."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        *,
        batch_size: int = 100,
        flush_interval: float = 1.0,
        max_queue_size: int = 10_000,
        timeout: float = 5.0,
        max_retries: int = 3,
        client: httpx.Client | None = None,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        if not endpoint:
            raise ValueError("endpoint is required")
        if not api_key:
            raise ValueError("api_key is required")
        self.url = endpoint.rstrip("/") + "/v1/events"
        self.batch_size = max(1, batch_size)
        self.flush_interval = max(0.01, flush_interval)
        self.max_queue_size = max_queue_size
        self.timeout = timeout
        self.max_retries = max(0, max_retries)

        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": f"queueloom-sdk/{__version__}",
        }
        self._client = client
        self._client_factory = client_factory
        self._owns_client = client is None

        self._queue: queue.Queue[TaskEvent] = queue.Queue(maxsize=max_queue_size)
        self._cond = threading.Condition()
        self._pending = 0
        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._pid: int | None = None
        self._stop = threading.Event()
        self._atexit_registered = False

        # Counters are informational; read them from tests/diagnostics.
        self.sent = 0
        self.dropped = 0
        self.failed_batches = 0

    # -- public API ----------------------------------------------------------------------

    def send(self, event: TaskEvent) -> None:
        self._ensure_worker()
        with self._cond:
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                self.dropped += 1
                if self.dropped in (1, 100, 1000) or self.dropped % 10_000 == 0:
                    log.warning("queueloom: event queue full, dropped %d events", self.dropped)
                return
            self._pending += 1

    def flush(self, timeout: float | None = None) -> bool:
        """Block until all queued events have been sent (or given up on)."""
        with self._cond:
            return self._cond.wait_for(lambda: self._pending == 0, timeout)

    def close(self) -> None:
        thread = self._thread
        if thread is None or self._pid != os.getpid():
            return
        self._stop.set()
        thread.join(timeout=self.timeout * (self.max_retries + 1) + self.flush_interval)
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None
        self._thread = None

    # -- internals -----------------------------------------------------------------------

    def _ensure_worker(self) -> None:
        pid = os.getpid()
        thread = self._thread
        if thread is not None and thread.is_alive() and self._pid == pid:
            return
        with self._start_lock:
            thread = self._thread
            if thread is not None and thread.is_alive() and self._pid == pid:
                return
            if self._pid is not None and self._pid != pid:
                # We are in a forked child: inherited queue/thread state is meaningless.
                self._queue = queue.Queue(maxsize=self.max_queue_size)
                self._pending = 0
                if self._owns_client:
                    self._client = None
            self._stop = threading.Event()
            self._thread = threading.Thread(
                target=self._run, name="queueloom-transport", daemon=True
            )
            self._pid = pid
            self._thread.start()
            if not self._atexit_registered:
                atexit.register(self.close)
                self._atexit_registered = True

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            if self._client_factory is not None:
                self._client = self._client_factory()
            else:
                self._client = httpx.Client(timeout=self.timeout)
        return self._client

    def _run(self) -> None:
        stop = self._stop
        while not stop.is_set() or not self._queue.empty():
            batch = self._collect_batch()
            if not batch:
                continue
            try:
                self._post(batch)
            finally:
                with self._cond:
                    self._pending -= len(batch)
                    self._cond.notify_all()

    def _collect_batch(self) -> list[TaskEvent]:
        batch: list[TaskEvent] = []
        deadline = time.monotonic() + self.flush_interval
        while len(batch) < self.batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(self._queue.get(timeout=remaining))
            except queue.Empty:
                break
        return batch

    def _post(self, batch: list[TaskEvent]) -> None:
        payload: dict[str, Any] = {"events": [e.model_dump(mode="json") for e in batch]}
        attempt = 0
        while True:
            try:
                response = self._get_client().post(self.url, json=payload, headers=self._headers)
            except httpx.HTTPError as exc:
                error: str | None = f"{type(exc).__name__}: {exc}"
                retryable = True
            else:
                if response.status_code < 300:
                    self.sent += len(batch)
                    return
                error = f"HTTP {response.status_code}: {response.text[:200]}"
                retryable = response.status_code == 429 or response.status_code >= 500

            if not retryable or attempt >= self.max_retries:
                self.failed_batches += 1
                self.dropped += len(batch)
                log.warning("queueloom: giving up on batch of %d events (%s)", len(batch), error)
                return
            attempt += 1
            delay = min(0.5 * (2 ** (attempt - 1)), 5.0)
            if self._stop.wait(delay) and attempt > 1:
                # Shutting down: allow one last try, then drop.
                self.failed_batches += 1
                self.dropped += len(batch)
                return
