from __future__ import annotations

import importlib
import socket
import sys
import threading
import time
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import select

from queueloom.events import EventType, TaskEvent
from queueloom.server.db import make_engine, make_session_factory, session_scope
from queueloom.server.ingest import ingest_events
from queueloom.server.migrate import upgrade
from queueloom.server.models import TaskEventRow, TaskRun
from queueloom.server.projects import create_project, get_project_by_name


@pytest.fixture
def demo(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("CELERY_BROKER_URL", "memory://")
    monkeypatch.setenv("CELERY_RESULT_BACKEND", "cache+memory://")
    monkeypatch.setenv("QUEUELOOM_ENVIRONMENT", "local-demo")
    monkeypatch.delenv("QUEUELOOM_ENDPOINT", raising=False)
    monkeypatch.delenv("QUEUELOOM_API_KEY", raising=False)
    monkeypatch.setattr(sys, "path", sys.path.copy())
    return importlib.import_module("demo.run_local")


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--port", "0"),
        ("--port", "65536"),
        ("--rate", "0"),
        ("--rate", "-1"),
        ("--rate", "nan"),
        ("--rate", "inf"),
        ("--rate", "1e-320"),
        ("--duration", "-1"),
        ("--duration", "nan"),
        ("--duration", "inf"),
    ],
)
def test_invalid_options_exit_before_database_access(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    option: str,
    value: str,
) -> None:
    database = Mock(side_effect=AssertionError("invalid options must not touch the database"))
    monkeypatch.setattr(demo, "make_engine", database)
    monkeypatch.setattr(sys, "argv", ["run_local.py", option, value])
    with pytest.raises(SystemExit) as exited:
        demo.main()
    assert exited.value.code == 2
    assert option in capsys.readouterr().err
    database.assert_not_called()


def test_occupied_port_exits_instead_of_waiting_forever(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "demo.db"
    engine = make_engine(f"sqlite:///{database}")
    upgrade(engine)
    factory = make_session_factory(engine)
    with session_scope(factory) as session:
        project, _ = create_project(session, "demo")
        project_id, original_key_hash = project.id, project.api_key_hash
    engine.dispose()
    real_sleep = time.sleep
    waits_after_server_exit = 0

    def bounded_sleep(seconds: float) -> None:
        nonlocal waits_after_server_exit
        if not any(thread.name == "queueloom-server" for thread in threading.enumerate()):
            waits_after_server_exit += 1
        assert waits_after_server_exit < 5, "demo kept waiting after server startup failed"
        real_sleep(seconds)

    monkeypatch.setattr(demo.time, "sleep", bounded_sleep)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_local.py",
                "--port",
                str(port),
                "--db",
                str(database),
                "--duration",
                "0.01",
            ],
        )
        with pytest.raises(SystemExit) as exited:
            demo.main()
    assert exited.value.code == 1
    assert "--port" in capsys.readouterr().err
    assert not any(thread.name == "queueloom-server" for thread in threading.enumerate())
    try:
        with session_scope(factory) as session:
            project = get_project_by_name(session, "demo")
            assert project is not None and project.id == project_id
            assert project.api_key_hash == original_key_hash
    finally:
        engine.dispose()


def test_startup_wait_is_bounded_when_thread_is_still_alive(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = iter([0.0, 16.0])
    monkeypatch.setattr(demo.time, "monotonic", lambda: next(clock))
    with pytest.raises(RuntimeError, match="did not become ready"):
        demo._wait_for_server(
            SimpleNamespace(started=False), SimpleNamespace(is_alive=lambda: True)
        )


def run_short_demo(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    database: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_local.py",
            "--port",
            str(free_port()),
            "--db",
            str(database),
            "--duration",
            "0.02",
            "--rate",
            "0.001",
        ],
    )
    monkeypatch.setattr(demo, "start_worker", lambda *args, **kwargs: nullcontext())
    monkeypatch.setattr(demo, "enqueue_random", lambda: None)
    real_sleep = time.sleep

    def no_oversleep(seconds: float) -> None:
        assert seconds <= 1, "task interval exceeded the requested short demo duration"
        real_sleep(seconds)

    monkeypatch.setattr(demo.time, "sleep", no_oversleep)
    demo.main()


def test_reusing_database_preserves_project_and_history(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "reuse.db"
    engine = make_engine(f"sqlite:///{database}")
    upgrade(engine)
    factory = make_session_factory(engine)
    with session_scope(factory) as session:
        project, _ = create_project(session, "demo")
        project_id, original_key_hash = project.id, project.api_key_hash
        ingest_events(
            session,
            project,
            [
                TaskEvent(
                    event_type=EventType.SUCCEEDED,
                    task_id="existing-task",
                    task_name="existing.task",
                    timestamp=datetime(2020, 1, 1, tzinfo=UTC),
                )
            ],
        )
        run_id = session.scalar(select(TaskRun.id))
        event_id = session.scalar(select(TaskEventRow.id))
    try:
        run_short_demo(demo, monkeypatch, database)
        with session_scope(factory) as session:
            project = get_project_by_name(session, "demo")
            assert project is not None and project.id == project_id
            assert project.api_key_hash != original_key_hash
            assert session.scalar(select(TaskRun.id)) == run_id
            assert session.scalar(select(TaskEventRow.id)) == event_id
    finally:
        engine.dispose()


def test_short_duration_does_not_sleep_for_a_long_task_interval(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_short_demo(demo, monkeypatch, tmp_path / "short.db")
    assert not any(thread.name == "queueloom-server" for thread in threading.enumerate())


def test_setup_failure_stops_the_started_server(
    demo: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_local.py",
            "--port",
            str(free_port()),
            "--db",
            str(tmp_path / "failure.db"),
        ],
    )
    monkeypatch.setattr(demo, "configure", Mock(side_effect=RuntimeError("setup failed")))
    servers = []
    server_class = demo.uvicorn.Server

    def capture_server(*args, **kwargs):
        server = server_class(*args, **kwargs)
        servers.append(server)
        return server

    monkeypatch.setattr(demo.uvicorn, "Server", capture_server)
    try:
        with pytest.raises(RuntimeError, match="setup failed"):
            demo.main()
        assert not any(thread.name == "queueloom-server" for thread in threading.enumerate())
    finally:
        # Also clean up when run against the old implementation for regression proof.
        for server in servers:
            server.should_exit = True
        for thread in threading.enumerate():
            if thread.name == "queueloom-server":
                thread.join(timeout=5)
