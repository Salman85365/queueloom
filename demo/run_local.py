"""Zero-infrastructure QueueLoom demo.

Runs everything in one process: a SQLite-backed QueueLoom server, an in-memory Celery broker,
an in-process Celery worker, and a producer that keeps enqueuing tasks. No Redis, no Postgres,
no Docker. Open the printed URL and watch tasks flow.

    python demo/run_local.py            # runs until Ctrl-C
    python demo/run_local.py --duration 60 --port 8800
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path

# Make `demo` importable when run as a script, and force the memory broker before import.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "cache+memory://"
os.environ.setdefault("QUEUELOOM_ENVIRONMENT", "local-demo")

import uvicorn
from celery.contrib.testing.worker import start_worker

from demo.tasks import app as celery_app
from demo.tasks import configure, enqueue_random
from queueloom.server.app import create_app
from queueloom.server.config import Settings
from queueloom.server.db import make_engine, make_session_factory, session_scope
from queueloom.server.migrate import upgrade
from queueloom.server.projects import (
    create_project,
    generate_api_key,
    get_project_by_name,
    hash_api_key,
)

SERVER_START_TIMEOUT = 15.0


def _wait_for_server(server: uvicorn.Server, thread: threading.Thread) -> None:
    deadline = time.monotonic() + SERVER_START_TIMEOUT
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError("The demo server stopped during startup. Check the error above.")
        if time.monotonic() >= deadline:
            raise RuntimeError("The demo server did not become ready within 15 seconds.")
        time.sleep(0.05)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8800)
    parser.add_argument("--rate", type=float, default=3.0, help="tasks per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    parser.add_argument("--db", default=None, help="SQLite path (default: temp file)")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if not math.isfinite(args.rate) or args.rate <= 0 or not math.isfinite(1.0 / args.rate):
        parser.error("--rate must be a finite positive number with a finite task interval")
    if not math.isfinite(args.duration) or args.duration < 0:
        parser.error("--duration must be a finite non-negative number (0 = forever)")

    logging.basicConfig(level=logging.WARNING)
    # The in-memory result backend has no hostname; kombu warns about it on every connection.
    logging.getLogger("kombu.connection").setLevel(logging.ERROR)
    db_path = args.db or os.path.join(tempfile.mkdtemp(prefix="queueloom-demo-"), "demo.db")
    settings = Settings(
        database_url=f"sqlite:///{db_path}",
        port=args.port,
        dashboard_refresh_seconds=5,
        auto_migrate=False,
        retention_interval_seconds=0,
        alert_eval_interval_seconds=0,
    )
    engine = make_engine(settings.database_url)
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings, engine=engine),
            host="127.0.0.1",
            port=args.port,
            log_level="warning",
            timeout_graceful_shutdown=5,
        )
    )

    def run_server() -> None:
        # Uvicorn logs bind failures and exits. Let the startup wait report them
        # to the main thread so the command exits instead of waiting forever.
        with suppress(SystemExit):
            server.run()

    server_thread = threading.Thread(target=run_server, name="queueloom-server", daemon=True)
    endpoint = f"http://127.0.0.1:{args.port}"
    instrumentation = None
    server_started = False
    sent = 0
    try:
        upgrade(engine)
        server_thread.start()
        try:
            _wait_for_server(server, server_thread)
        except RuntimeError as exc:
            parser.exit(1, f"{exc} Try a different --port (current: {args.port}).\n")
        server_started = True
        with session_scope(make_session_factory(engine)) as session:
            existing = get_project_by_name(session, "demo")
            if existing is None:
                _, api_key = create_project(session, "demo")
            else:
                api_key = generate_api_key()
                existing.api_key_hash = hash_api_key(api_key)
                print("Existing demo history retained; the demo API key has been rotated.")
        instrumentation = configure(endpoint=endpoint, api_key=api_key, flush_interval=0.5)

        print(f"QueueLoom dashboard: {endpoint}/projects/demo")
        print(f"SQLite database:     {db_path}")
        print("Press Ctrl-C to stop.\n")

        interval = 1.0 / args.rate
        with start_worker(
            celery_app,
            pool="solo",
            queues=["default", "reports"],
            perform_ping_check=False,
            loglevel="WARNING",
        ):
            deadline = time.monotonic() + args.duration if args.duration else None
            while deadline is None or time.monotonic() < deadline:
                enqueue_random()
                sent += 1
                remaining = (
                    max(0.0, deadline - time.monotonic()) if deadline is not None else interval
                )
                time.sleep(min(interval, remaining))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if instrumentation is not None:
                try:
                    instrumentation.flush(timeout=5)
                finally:
                    instrumentation.close()
        finally:
            server.should_exit = True
            if server_thread.ident is not None:
                server_thread.join(timeout=10)
            engine.dispose()
    if server_started:
        print(f"\nenqueued {sent} tasks; dashboard data kept at {db_path}")


if __name__ == "__main__":
    main()
