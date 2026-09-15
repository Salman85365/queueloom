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
import os
import sys
import tempfile
import threading
import time
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
from queueloom.server.projects import create_project, get_project_by_name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8800)
    parser.add_argument("--rate", type=float, default=3.0, help="tasks per second")
    parser.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    parser.add_argument("--db", default=None, help="SQLite path (default: temp file)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    # The in-memory result backend has no hostname; kombu warns about it on every connection.
    logging.getLogger("kombu.connection").setLevel(logging.ERROR)
    db_path = args.db or os.path.join(tempfile.mkdtemp(prefix="queueloom-demo-"), "demo.db")
    settings = Settings(
        database_url=f"sqlite:///{db_path}", port=args.port, dashboard_refresh_seconds=5
    )
    engine = make_engine(settings.database_url)
    upgrade(engine)
    with session_scope(make_session_factory(engine)) as session:
        existing = get_project_by_name(session, "demo")
        if existing is None:
            _, api_key = create_project(session, "demo")
        else:
            session.delete(existing)
            session.flush()
            _, api_key = create_project(session, "demo")

    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings, engine=engine),
            host="127.0.0.1",
            port=args.port,
            log_level="warning",
        )
    )
    threading.Thread(target=server.run, name="queueloom-server", daemon=True).start()
    while not server.started:
        time.sleep(0.05)

    endpoint = f"http://127.0.0.1:{args.port}"
    instrumentation = configure(endpoint=endpoint, api_key=api_key, flush_interval=0.5)

    print(f"QueueLoom dashboard: {endpoint}/projects/demo")
    print(f"SQLite database:     {db_path}")
    print("Press Ctrl-C to stop.\n")

    deadline = time.monotonic() + args.duration if args.duration else None
    interval = 1.0 / max(args.rate, 0.01)
    sent = 0
    try:
        with start_worker(
            celery_app,
            pool="solo",
            queues=["default", "reports"],
            perform_ping_check=False,
            loglevel="WARNING",
        ):
            while deadline is None or time.monotonic() < deadline:
                enqueue_random()
                sent += 1
                time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        instrumentation.flush(timeout=5)
        instrumentation.close()
        server.should_exit = True
    print(f"\nenqueued {sent} tasks; dashboard data kept at {db_path}")


if __name__ == "__main__":
    main()
