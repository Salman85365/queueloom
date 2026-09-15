"""Smoke-check a distribution installed in a fresh virtual environment.

Run from outside the source checkout after installing the wheel. First run with
``--sdk-only`` for a base install, then without it after installing ``[server]``.
No broker, external service, or API credential is required.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def check_sdk(expected_version: str | None) -> str:
    import queueloom
    from queueloom.events import EventType, TaskEvent
    from queueloom.sdk import MemoryTransport

    package_path = Path(queueloom.__file__).resolve()
    environment_path = Path(sys.prefix).resolve()
    if not package_path.is_relative_to(environment_path):
        raise RuntimeError(f"Expected an installed distribution, imported {package_path}")
    version = importlib.metadata.version("queueloom")
    assert queueloom.__version__ == version
    if expected_version:
        assert version == expected_version, (version, expected_version)
    event = TaskEvent(event_type=EventType.SUCCEEDED, task_id="package-smoke")
    transport = MemoryTransport()
    transport.send(event)
    assert transport.events == [event]
    executable = Path(sys.executable).parent / ("queueloom.exe" if os.name == "nt" else "queueloom")
    result = subprocess.run(
        [str(executable), "--version"], check=True, capture_output=True, text=True
    )
    assert result.stdout.strip() == f"queueloom {version}"
    subprocess.run([str(executable), "--help"], check=True, capture_output=True, text=True)
    print(f"PASS: installed SDK, metadata, version and CLI help ({version})")
    return version


def check_server(version: str) -> None:
    from fastapi.testclient import TestClient
    from sqlalchemy import inspect

    from queueloom.events import EventType, TaskEvent
    from queueloom.server.app import create_app
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine
    from queueloom.server.migrate import current_revision, head_revision

    executable = Path(sys.executable).parent / ("queueloom.exe" if os.name == "nt" else "queueloom")
    with tempfile.TemporaryDirectory(prefix="queueloom-package-") as directory:
        database_url = f"sqlite:///{Path(directory) / 'smoke.db'}"
        cli_env = {**os.environ, "QUEUELOOM_DATABASE_URL": database_url}
        for args in (
            ["migrate"],
            ["migrate"],
            ["project", "create", "package-smoke", "--api-key", "ql_package_smoke"],
            ["project", "list"],
        ):
            subprocess.run(
                [str(executable), *args],
                check=True,
                capture_output=True,
                text=True,
                env=cli_env,
            )
        engine = make_engine(database_url)
        try:
            revision = current_revision(engine)
            assert revision is not None and revision == head_revision(engine)
            assert {"projects", "task_runs", "task_events"} <= set(
                inspect(engine).get_table_names()
            )
        finally:
            engine.dispose()
        settings = Settings(
            database_url=database_url,
            dashboard_password=None,
            ai_provider="template",
            alert_eval_interval_seconds=0,
            retention_interval_seconds=0,
        )
        with TestClient(create_app(settings)) as client:
            health = client.get("/v1/health")
            assert health.status_code == 200, health.text
            assert health.json()["version"] == version
            event = TaskEvent(
                event_type=EventType.SUCCEEDED,
                task_id="package-smoke",
                task_name="package.smoke",
            )
            response = client.post(
                "/v1/events",
                headers={"Authorization": "Bearer ql_package_smoke"},
                json={"events": [event.model_dump(mode="json")]},
            )
            assert response.status_code == 202, response.text
            assert response.json()["accepted"] == 1
            for path in (
                "/",
                "/login",
                "/projects/package-smoke",
                "/projects/package-smoke/tasks",
                "/projects/package-smoke/tasks/package-smoke",
                "/projects/package-smoke/alerts",
                "/projects/package-smoke/diagnose",
            ):
                page = client.get(path)
                assert page.status_code == 200, (path, page.text)
                assert "text/html" in page.headers["content-type"], path
        print(
            "PASS: migration CLI (including repeat), project CLI, health, ingestion and dashboard"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sdk-only", action="store_true", help="Check only base SDK/CLI dependencies."
    )
    parser.add_argument("--expected-version", help="Require this exact release version.")
    options = parser.parse_args()
    installed_version = check_sdk(options.expected_version)
    if not options.sdk_only:
        check_server(installed_version)
