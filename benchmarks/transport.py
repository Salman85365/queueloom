#!/usr/bin/env python3
"""Measure prebuilt-event submission, with delivery accounted for separately.

Run from a checkout after installing QueueLoom. This local transport microbenchmark
does not exercise the QueueLoom ingestion API, a database, or a task framework.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import inspect
import json
import logging
import os
import platform
import socket
import statistics
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

from queueloom import __version__
from queueloom.events import EventType, TaskEvent
from queueloom.sdk.transport import HttpTransport

BATCH_SIZE = 100
FLUSH_INTERVAL = 0.01
HTTP_TIMEOUT = 0.25
FLUSH_TIMEOUT = 30.0
MAX_RETRIES = 0


class Collector(ThreadingHTTPServer):
    """A counting HTTP acknowledgement stub, not the QueueLoom server."""

    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), CollectorHandler)
        self.event_ids: list[str] = []
        self.requests = 0
        self.lock = threading.Lock()

    def reset(self) -> None:
        with self.lock:
            self.event_ids.clear()
            self.requests = 0


class CollectorHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def do_POST(self) -> None:
        collector = self.server
        if self.path != "/v1/events":
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        event_ids = [event["event_id"] for event in json.loads(body)["events"]]
        with collector.lock:
            collector.event_ids.extend(event_ids)
            collector.requests += 1
        reply = b'{"accepted":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)
        self.wfile.flush()

    def log_message(self, _format: str, *args: object) -> None:
        pass


@contextmanager
def healthy_collector():
    collector = Collector()
    thread = threading.Thread(target=collector.serve_forever, daemon=True)
    thread.start()
    try:
        yield collector
    finally:
        collector.shutdown()
        collector.server_close()
        thread.join()


@contextmanager
def unreachable_endpoint():
    # Reserve an unused loopback port without listening. Depending on the host,
    # connecting fails immediately or times out; record the observed failure.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        endpoint = f"http://127.0.0.1:{reserved.getsockname()[1]}"
        with httpx.Client(timeout=HTTP_TIMEOUT, trust_env=False) as client:
            start = time.perf_counter()
            try:
                client.post(endpoint + "/v1/events", json={"events": []})
            except httpx.HTTPError as exc:
                probe = {
                    "exception_type": type(exc).__name__,
                    "elapsed_seconds": time.perf_counter() - start,
                }
            else:
                raise RuntimeError("Unreachable endpoint unexpectedly answered")
        yield endpoint, probe


class NoOp:
    def send(self, _event: TaskEvent) -> None:
        pass


def submit(transport, events: list[TaskEvent]) -> float:
    start = time.perf_counter_ns()
    for event in events:
        transport.send(event)
    return (time.perf_counter_ns() - start) / 1_000_000_000


def run_baseline(events: list[TaskEvent], warmup: list[TaskEvent]) -> dict:
    transport = NoOp()
    submit(transport, warmup)
    elapsed = submit(transport, events)
    return {
        "scenario": "no_op_baseline",
        "submitted_calls": len(events),
        "submission_seconds": elapsed,
        "submission_us_per_event": elapsed * 1_000_000 / len(events),
    }


def run_http(
    scenario: str,
    endpoint: str,
    events: list[TaskEvent],
    warmup: list[TaskEvent],
    collector: Collector | None = None,
) -> dict:
    queue_capacity = max(len(events), len(warmup)) + BATCH_SIZE
    # Explicitly bypass environment proxies for an exclusively local experiment.
    with httpx.Client(timeout=HTTP_TIMEOUT, trust_env=False) as client:
        transport = HttpTransport(
            endpoint,
            "benchmark-only-not-a-secret",
            batch_size=BATCH_SIZE,
            flush_interval=FLUSH_INTERVAL,
            max_queue_size=queue_capacity,
            timeout=HTTP_TIMEOUT,
            max_retries=MAX_RETRIES,
            client=client,
        )
        try:
            submit(transport, warmup)
            if not transport.flush(timeout=FLUSH_TIMEOUT):
                raise RuntimeError(f"{scenario}: warmup failed to drain")
            if collector is not None:
                collector.reset()
            before = (transport.sent, transport.dropped, transport.failed_batches)
            start = time.perf_counter_ns()
            submission_seconds = submit(transport, events)
            drained = transport.flush(timeout=FLUSH_TIMEOUT)
            total_seconds = (time.perf_counter_ns() - start) / 1_000_000_000
            if not drained:
                raise RuntimeError(f"{scenario}: timed out; no valid result")
            sent = transport.sent - before[0]
            dropped = transport.dropped - before[1]
            failed_batches = transport.failed_batches - before[2]
            received = 0 if collector is None else len(collector.event_ids)
            if sent + dropped != len(events):
                raise RuntimeError(f"{scenario}: events are unaccounted for")
            if collector is not None:
                if collector.event_ids != [event.event_id for event in events]:
                    raise RuntimeError("Healthy collector did not receive the exact workload")
                if sent != len(events) or dropped:
                    raise RuntimeError("Healthy workload was not completely delivered")
            elif sent or dropped != len(events):
                raise RuntimeError("Unreachable endpoint unexpectedly delivered events")
            return {
                "scenario": scenario,
                "submitted_calls": len(events),
                # The warmup drained fully, and the queue can hold the entire run.
                # Therefore no send can encounter a full queue in this experiment.
                "queue_accepted_events": len(events),
                "queue_rejected_events": 0,
                "sent_events": sent,
                "dropped_events": dropped,
                "failed_batches": failed_batches,
                "collector_received_events": received,
                "collector_requests": 0 if collector is None else collector.requests,
                "pending_events_at_end": len(events) - sent - dropped,
                "flush_completed": drained,
                "submission_seconds": submission_seconds,
                "submission_us_per_event": submission_seconds * 1_000_000 / len(events),
                "submission_through_drain_seconds": total_seconds,
                "delivered_events_per_second": sent / total_seconds,
            }
        finally:
            transport.close()


def source_hash(obj) -> str:
    return hashlib.sha256(Path(inspect.getfile(obj)).read_bytes()).hexdigest()


def dependency_versions() -> dict[str, str]:
    return dict(
        sorted(
            (distribution.metadata["Name"], distribution.version)
            for distribution in importlib.metadata.distributions()
            if distribution.metadata["Name"].lower() not in {"queueloom", "pip"}
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.events, args.warmup, args.repeats) < 1:
        parser.error("events, warmup, and repeats must be positive")
    logging.getLogger("queueloom.sdk").setLevel(logging.CRITICAL)
    # Fixed fields and IDs: no UUID generation, clock reads, or event construction
    # occur inside the timed loop. No user data, hostname, or filesystem paths.
    events = [
        TaskEvent(
            event_id=f"benchmark-{index:08d}",
            event_type=EventType.SUCCEEDED,
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            task_id=f"task-{index:08d}",
            task_name="benchmark.example_task",
            queue="benchmark",
            environment="benchmark",
            worker="benchmark-worker",
            retries=0,
            runtime_ms=12.5,
            args_repr="(42,)",
            kwargs_repr="{}",
        )
        for index in range(max(args.events, args.warmup))
    ]
    warmup = events[: args.warmup]
    events = events[: args.events]
    results = []
    with healthy_collector() as collector, unreachable_endpoint() as (unavailable, probe):
        endpoint = f"http://127.0.0.1:{collector.server_address[1]}"
        for repeat in range(args.repeats):
            # Rotate order deterministically to reduce a fixed ordering bias.
            scenarios = ["no_op_baseline", "healthy_http_stub", "unreachable_http_endpoint"]
            rotation = repeat % len(scenarios)
            for scenario in scenarios[rotation:] + scenarios[:rotation]:
                if scenario == "no_op_baseline":
                    result = run_baseline(events, warmup)
                elif scenario == "healthy_http_stub":
                    result = run_http(scenario, endpoint, events, warmup, collector)
                else:
                    result = run_http(scenario, unavailable, events, warmup)
                result["repeat"] = repeat + 1
                results.append(result)
    summary = {}
    for scenario in ("no_op_baseline", "healthy_http_stub", "unreachable_http_endpoint"):
        runs = [result for result in results if result["scenario"] == scenario]
        values = [result["submission_us_per_event"] for result in runs]
        summary[scenario] = {
            "median_submission_us_per_event": statistics.median(values),
            "min_submission_us_per_event": min(values),
            "max_submission_us_per_event": max(values),
        }
        if scenario != "no_op_baseline":
            summary[scenario].update(
                median_submission_through_drain_seconds=statistics.median(
                    result["submission_through_drain_seconds"] for result in runs
                ),
                total_submitted_events=sum(result["submitted_calls"] for result in runs),
                total_sent_events=sum(result["sent_events"] for result in runs),
                total_dropped_events=sum(result["dropped_events"] for result in runs),
                total_collector_received_events=sum(
                    result["collector_received_events"] for result in runs
                ),
            )
    baseline = summary["no_op_baseline"]["median_submission_us_per_event"]
    for scenario in ("healthy_http_stub", "unreachable_http_endpoint"):
        summary[scenario]["median_submission_us_above_baseline"] = (
            summary[scenario]["median_submission_us_per_event"] - baseline
        )
    payload = {
        "schema_version": 1,
        "benchmark": "prebuilt_event_http_transport_submission",
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "os": platform.system(),
            "os_release": platform.release(),
            "machine": platform.machine(),
            "logical_cpu_count": os.cpu_count(),
            "queueloom_version": __version__,
            "dependencies": dependency_versions(),
            "source_sha256": {
                "transport.py": source_hash(HttpTransport),
                "events.py": source_hash(TaskEvent),
                "benchmark.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            },
        },
        "configuration": {
            "events_per_run": args.events,
            "warmup_events_per_run": args.warmup,
            "repeats": args.repeats,
            "batch_size": BATCH_SIZE,
            "flush_interval_seconds": FLUSH_INTERVAL,
            "http_timeout_seconds": HTTP_TIMEOUT,
            "flush_timeout_seconds": FLUSH_TIMEOUT,
            "max_retries": MAX_RETRIES,
            "unreachable_endpoint": "bound_loopback_port_without_listener",
            "unreachable_endpoint_probe": probe,
            "queue_capacity": max(args.events, args.warmup) + BATCH_SIZE,
            "http_environment_proxies": False,
            "logging": "critical_only",
            "sample_event_json_bytes": len(events[0].model_dump_json().encode()),
            "includes_event_construction": False,
            "includes_transport_startup": False,
            "includes_server_database_ingestion": False,
        },
        "summary": summary,
        "runs": results,
    }
    rendered = json.dumps(payload, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
