from __future__ import annotations

import re
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session, sessionmaker

from queueloom.server.app import create_app
from queueloom.server.config import Settings
from queueloom.server.db import session_scope
from queueloom.server.models import AlertRule
from tests.conftest import ProjectInfo


@pytest.fixture
def locked_client(engine: Engine) -> Iterator[TestClient]:
    settings = Settings(
        auto_migrate=False,
        dashboard_refresh_seconds=0,
        dashboard_password="hunter2",
        secret_key="test-secret",
        alert_eval_interval_seconds=0,
        retention_interval_seconds=0,
    )
    with TestClient(create_app(settings, engine=engine)) as c:
        yield c


def _csrf(html: str) -> str:
    match = re.search(r'name="csrf" value="([^"]+)"', html)
    assert match, "no csrf token in page"
    return match.group(1)


def _login(client: TestClient, password: str = "hunter2") -> None:
    page = client.get("/login")
    response = client.post(
        "/login",
        data={"password": password, "csrf": _csrf(page.text), "next": "/"},
        follow_redirects=False,
    )
    assert response.status_code == 303, response.text


def test_open_mode_shows_warning_and_no_login(client: TestClient, project: ProjectInfo) -> None:
    page = client.get(f"/projects/{project.name}")
    assert page.status_code == 200
    assert "QUEUELOOM_DASHBOARD_PASSWORD" in page.text
    assert "Sign out" not in page.text
    assert client.get("/login", follow_redirects=False).status_code == 303


def test_locked_dashboard_redirects_to_login(
    locked_client: TestClient, project: ProjectInfo
) -> None:
    response = locked_client.get(f"/projects/{project.name}/tasks?range=1h", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/login?next=")
    assert "%2Ftasks" in response.headers["location"]
    login = locked_client.get("/login")
    assert login.status_code == 200 and "Sign in" in login.text


def test_wrong_password_and_missing_csrf_are_rejected(locked_client: TestClient) -> None:
    page = locked_client.get("/login")
    token = _csrf(page.text)
    bad = locked_client.post("/login", data={"password": "nope", "csrf": token})
    assert bad.status_code == 401 and "Wrong password" in bad.text
    no_csrf = locked_client.post("/login", data={"password": "hunter2", "csrf": "forged"})
    assert no_csrf.status_code == 401
    assert locked_client.get("/", follow_redirects=False).status_code == 303


def test_login_logout_cycle(locked_client: TestClient, project: ProjectInfo) -> None:
    _login(locked_client)
    page = locked_client.get(f"/projects/{project.name}")
    assert page.status_code == 200 and "Sign out" in page.text
    assert "QUEUELOOM_DASHBOARD_PASSWORD" not in page.text
    # Already signed in: /login bounces home.
    assert locked_client.get("/login", follow_redirects=False).status_code == 303

    locked_client.post("/logout", data={"csrf": _csrf(page.text)}, follow_redirects=False)
    assert locked_client.get("/", follow_redirects=False).status_code == 303


def test_next_redirect_is_same_site_only(locked_client: TestClient) -> None:
    page = locked_client.get("/login?next=https://evil.example/phish")
    response = locked_client.post(
        "/login",
        data={
            "password": "hunter2",
            "csrf": _csrf(page.text),
            "next": "https://evil.example/phish",
        },
        follow_redirects=False,
    )
    assert response.headers["location"] == "/"


def test_api_ignores_dashboard_password(locked_client: TestClient, project: ProjectInfo) -> None:
    assert locked_client.get("/v1/health").status_code == 200
    assert locked_client.get("/v1/projects/me", headers=project.headers).status_code == 200


def test_alert_forms_create_toggle_delete(
    locked_client: TestClient, project: ProjectInfo, session_factory: sessionmaker[Session]
) -> None:
    _login(locked_client)
    page = locked_client.get(f"/projects/{project.name}/alerts")
    token = _csrf(page.text)

    forged = locked_client.post(
        f"/projects/{project.name}/alerts",
        data={"csrf": "nope", "webhook_url": "https://h.example"},
    )
    assert forged.status_code == 403

    invalid = locked_client.post(
        f"/projects/{project.name}/alerts",
        data={"csrf": token, "webhook_url": "ftp://h.example", "threshold_pct": "20"},
        follow_redirects=True,
    )
    assert invalid.status_code == 200 and "webhook_url" in invalid.text

    created = locked_client.post(
        f"/projects/{project.name}/alerts",
        data={
            "csrf": token,
            "rule_name": "emails",
            "webhook_url": "https://hooks.example/x",
            "webhook_format": "slack",
            "threshold_pct": "20",
            "window_minutes": "10",
            "min_runs": "3",
            "cooldown_minutes": "5",
            "environment": "",
            "task_name": "app.send_email",
        },
        follow_redirects=True,
    )
    assert created.status_code == 200 and "emails" in created.text
    with session_scope(session_factory) as session:
        rule = session.scalar(select(AlertRule).where(AlertRule.project_id == project.id))
        assert rule is not None
        assert rule.threshold == 0.2 and rule.task_name == "app.send_email"
        assert rule.webhook_format == "slack" and rule.enabled is True
        rule_id = rule.id

    locked_client.post(f"/projects/{project.name}/alerts/{rule_id}/toggle", data={"csrf": token})
    with session_scope(session_factory) as session:
        assert session.get(AlertRule, rule_id).enabled is False  # type: ignore[union-attr]

    locked_client.post(f"/projects/{project.name}/alerts/{rule_id}/delete", data={"csrf": token})
    with session_scope(session_factory) as session:
        assert session.get(AlertRule, rule_id) is None
