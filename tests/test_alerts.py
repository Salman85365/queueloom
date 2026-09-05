from __future__ import annotations

import hashlib
import hmac
import json
from datetime import timedelta
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from queueloom.events import EventType
from queueloom.server.alerts import (
    SIGNATURE_HEADER,
    build_payload,
    evaluate_rule,
    measure,
    run_evaluation,
)
from queueloom.server.db import session_scope
from queueloom.server.ingest import ingest_events
from queueloom.server.models import AlertEvent, AlertRule, AlertState, Project
from queueloom.server.projects import get_project_by_name
from tests.conftest import ProjectInfo, at, dump, lifecycle


def _seed(
    session_factory: sessionmaker[Session],
    project: ProjectInfo,
    *,
    ok: int,
    failed: int,
    offset: float = 0.0,
    task_name: str = "app.tasks.add",
    environment: str = "default",
) -> None:
    events: list[Any] = []
    for i in range(ok):
        events += lifecycle(
            f"ok-{offset}-{i}", offset=offset + i, task_name=task_name, environment=environment
        )
    for i in range(failed):
        events += lifecycle(
            f"bad-{offset}-{i}",
            offset=offset + i,
            outcome=EventType.FAILED,
            task_name=task_name,
            environment=environment,
        )
    with session_scope(session_factory) as session:
        p = get_project_by_name(session, project.name)
        assert p is not None
        ingest_events(session, p, events)


def _rule(session_factory: sessionmaker[Session], project: ProjectInfo, **overrides: Any) -> int:
    fields: dict[str, Any] = {
        "name": "high failure rate",
        "window_minutes": 15,
        "threshold": 0.2,
        "min_runs": 5,
        "cooldown_minutes": 30,
        "webhook_url": "http://hooks.example/alerts",
        "webhook_format": "json",
    }
    fields.update(overrides)
    with session_scope(session_factory) as session:
        rule = AlertRule(project_id=project.id, **fields)
        session.add(rule)
        session.flush()
        return rule.id


def _get_rule(session: Session, rule_id: int) -> AlertRule:
    rule = session.get(AlertRule, rule_id)
    assert rule is not None
    return rule


def test_measure_scopes_by_window_environment_and_task(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    # runs finish at offset+1.25s; window is [now-15m, now]
    _seed(session_factory, project, ok=4, failed=1, offset=0, environment="prod")
    _seed(session_factory, project, ok=2, failed=2, offset=100, environment="staging")
    _seed(session_factory, project, ok=1, failed=0, offset=-3600, environment="prod")  # old
    rule_id = _rule(session_factory, project, environment="prod")
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        m = measure(session, rule, at(200))
        assert (m.failed, m.finished) == (1, 5)
        rule.environment = None
        m = measure(session, rule, at(200))
        assert (m.failed, m.finished) == (3, 9)
        rule.task_name = "nope"
        m = measure(session, rule, at(200))
        assert (m.failed, m.finished) == (0, 0)
        assert m.failure_rate is None


def test_rule_fires_resolves_and_respects_cooldown(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    rule_id = _rule(session_factory, project, threshold=0.25, min_runs=4, cooldown_minutes=10)

    # Not enough data: nothing happens.
    _seed(session_factory, project, ok=1, failed=1, offset=0)
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        assert evaluate_rule(session, rule, at(10)) is None
        assert rule.state == AlertState.OK.value
        assert rule.last_evaluated_at == at(10)

    # 2 failed / 6 finished = 33% >= 25%: fires once, then stays firing silently.
    _seed(session_factory, project, ok=3, failed=1, offset=20)
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        event = evaluate_rule(session, rule, at(30))
        assert event is not None and event.kind == "fired"
        assert event.failed == 2 and event.finished == 6
        assert rule.state == AlertState.FIRING.value
        assert evaluate_rule(session, rule, at(31)) is None

    # Window slides past the failures (15m later): resolves.
    _seed(session_factory, project, ok=6, failed=0, offset=1000)
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        event = evaluate_rule(session, rule, at(1010))
        assert event is not None and event.kind == "resolved"
        assert rule.state == AlertState.OK.value
        assert rule.last_resolved_at == at(1010)

    # Spikes again inside the cooldown: suppressed. After cooldown: fires.
    _seed(session_factory, project, ok=0, failed=6, offset=1020)
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        assert evaluate_rule(session, rule, at(1030)) is None
        assert rule.state == AlertState.OK.value
        event = evaluate_rule(session, rule, at(1010 + 601))
        assert event is not None and event.kind == "fired"


def test_disabled_rules_are_skipped(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    _seed(session_factory, project, ok=0, failed=10, offset=0)
    _rule(session_factory, project, enabled=False)
    assert run_evaluation(session_factory, now=at(20)) == []


def test_run_evaluation_delivers_signed_webhook(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    _seed(session_factory, project, ok=2, failed=8, offset=0)
    rule_id = _rule(session_factory, project, webhook_secret="s3cret")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ids = run_evaluation(session_factory, now=at(20), client=client, base_url="https://ql.example")
    assert len(ids) == 1
    (request,) = received
    body = json.loads(request.content)
    assert body["type"] == "queueloom.alert.fired"
    assert body["failure_rate"] == 0.8
    assert body["rule"]["id"] == rule_id
    assert body["link"] == f"https://ql.example/projects/{project.name}/alerts"
    assert "80.0%" in body["text"]
    expected = "sha256=" + hmac.new(b"s3cret", request.content, hashlib.sha256).hexdigest()
    assert request.headers[SIGNATURE_HEADER] == expected

    with session_scope(session_factory) as session:
        event = session.get(AlertEvent, ids[0])
        assert event is not None and event.delivered is True
        assert event.delivery_status == "HTTP 200"

    # Idempotent: a second evaluation while still firing emits nothing.
    assert run_evaluation(session_factory, now=at(21), client=client) == []


def test_delivery_failure_is_recorded_not_raised(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    _seed(session_factory, project, ok=0, failed=6, offset=0)
    _rule(session_factory, project)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ids = run_evaluation(session_factory, now=at(20), client=client)
    with session_scope(session_factory) as session:
        event = session.get(AlertEvent, ids[0])
        assert event is not None and event.delivered is False
        assert "ConnectError" in (event.delivery_status or "")


def test_slack_format_payload(session_factory: sessionmaker[Session], project: ProjectInfo) -> None:
    rule_id = _rule(session_factory, project, webhook_format="slack", task_name="app.send_email")
    with session_scope(session_factory) as session:
        rule = _get_rule(session, rule_id)
        proj = session.get(Project, project.id)
        assert proj is not None
        event = AlertEvent(
            rule_id=rule.id,
            project_id=project.id,
            kind="fired",
            created_at=at(0),
            failure_rate=0.5,
            failed=5,
            finished=10,
            window_since=at(-900),
            window_until=at(0),
        )
        payload = build_payload(rule, event, proj, None)
        assert set(payload) == {"text"}
        assert "task=app.send_email" in payload["text"]


def test_alert_api_crud_and_test_delivery(
    app: FastAPI, client: TestClient, project: ProjectInfo, session_factory: sessionmaker[Session]
) -> None:
    received: list[dict[str, Any]] = []
    app.state.webhook_client = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: (received.append(json.loads(r.content)), httpx.Response(204))[1]
        )
    )
    bad = client.post(
        "/v1/alerts", json={"name": "x", "webhook_url": "ftp://nope"}, headers=project.headers
    )
    assert bad.status_code == 422
    created = client.post(
        "/v1/alerts",
        json={
            "name": "emails",
            "webhook_url": "http://hooks.example/x",
            "threshold": 0.5,
            "min_runs": 2,
            "task_name": "app.send_email",
            "webhook_format": "slack",
        },
        headers=project.headers,
    )
    assert created.status_code == 201, created.text
    rule_id = created.json()["id"]
    assert created.json()["state"] == "ok"

    listing = client.get("/v1/alerts", headers=project.headers).json()
    assert [r["id"] for r in listing["items"]] == [rule_id]

    detail = client.get(f"/v1/alerts/{rule_id}", headers=project.headers).json()
    assert detail["current"]["finished"] == 0

    tested = client.post(f"/v1/alerts/{rule_id}/test", headers=project.headers).json()
    assert tested["kind"] == "test" and tested["delivered"] is True
    assert received and "test" in received[-1]["text"]

    # Evaluate on demand: seed failures dated "now" so the window catches them.
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    events: list[Any] = []
    for i in range(3):
        events += lifecycle(f"e{i}", outcome=EventType.FAILED, task_name="app.send_email")
    for j, e in enumerate(events):
        e.timestamp = now - timedelta(seconds=30) + timedelta(milliseconds=10 * j)
    client.post("/v1/events", json=dump(events), headers=project.headers)
    evaluated = client.post("/v1/alerts/evaluate", headers=project.headers).json()
    assert [e["kind"] for e in evaluated["events"]] == ["fired"]
    assert received[-1]["text"].startswith("\U0001f534")

    history = client.get(f"/v1/alerts/{rule_id}/events", headers=project.headers).json()
    assert [e["kind"] for e in history["items"]] == ["fired", "test"]

    page = client.get(f"/projects/{project.name}/alerts")
    assert page.status_code == 200 and "emails" in page.text and "fired" in page.text

    assert client.delete(f"/v1/alerts/{rule_id}", headers=project.headers).status_code == 204
    assert client.get(f"/v1/alerts/{rule_id}", headers=project.headers).status_code == 404
    with session_scope(session_factory) as session:
        assert session.scalars(select(AlertEvent)).all() == []  # cascaded


def test_alert_rules_are_project_scoped(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    from tests.conftest import make_project

    p1 = make_project(session_factory, "one")
    p2 = make_project(session_factory, "two")
    rule_id = client.post(
        "/v1/alerts", json={"name": "r", "webhook_url": "https://h.example"}, headers=p1.headers
    ).json()["id"]
    assert client.get(f"/v1/alerts/{rule_id}", headers=p2.headers).status_code == 404
    assert client.delete(f"/v1/alerts/{rule_id}", headers=p2.headers).status_code == 404
    assert client.get("/v1/alerts", headers=p2.headers).json()["items"] == []
