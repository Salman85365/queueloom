"""Failure-rate alerting.

A rule watches the failure rate of finished runs (succeeded + failed) over a sliding window,
optionally scoped to an environment and/or task name. When the rate crosses the threshold with
at least ``min_runs`` finished runs, the rule transitions to ``firing`` and a webhook is
delivered. When the rate drops back below the threshold it transitions to ``ok`` and a
``resolved`` webhook is delivered. ``cooldown_minutes`` prevents re-firing immediately after a
resolve.

Evaluation is deterministic given a clock (``now``), so it is easy to test and to run from a
periodic loop, a cron job, or an API call.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from queueloom.server.models import AlertEvent, AlertRule, AlertState, Project, RunState, TaskRun

log = logging.getLogger("queueloom.server.alerts")

WEBHOOK_FORMATS = ("json", "slack")
SIGNATURE_HEADER = "X-QueueLoom-Signature"


@dataclass(frozen=True)
class Measurement:
    since: datetime
    until: datetime
    failed: int
    finished: int

    @property
    def failure_rate(self) -> float | None:
        return None if self.finished == 0 else self.failed / self.finished


def measure(session: Session, rule: AlertRule, now: datetime) -> Measurement:
    since = now - timedelta(minutes=rule.window_minutes)
    stmt = (
        select(TaskRun.state, func.count())
        .where(
            TaskRun.project_id == rule.project_id,
            TaskRun.state.in_([RunState.SUCCEEDED.value, RunState.FAILED.value]),
            TaskRun.finished_at >= since,
            TaskRun.finished_at <= now,
        )
        .group_by(TaskRun.state)
    )
    if rule.environment:
        stmt = stmt.where(TaskRun.environment == rule.environment)
    if rule.task_name:
        stmt = stmt.where(TaskRun.task_name == rule.task_name)
    counts = {str(state): int(count) for state, count in session.execute(stmt)}
    failed = counts.get(RunState.FAILED.value, 0)
    finished = failed + counts.get(RunState.SUCCEEDED.value, 0)
    return Measurement(since=since, until=now, failed=failed, finished=finished)


def evaluate_rule(session: Session, rule: AlertRule, now: datetime) -> AlertEvent | None:
    """Update the rule's state for ``now``; return the transition event, if any."""
    m = measure(session, rule, now)
    rule.last_evaluated_at = now
    if m.finished < rule.min_runs or m.failure_rate is None:
        return None

    event: AlertEvent | None = None
    if m.failure_rate >= rule.threshold:
        in_cooldown = rule.last_resolved_at is not None and now - rule.last_resolved_at < timedelta(
            minutes=rule.cooldown_minutes
        )
        if rule.state != AlertState.FIRING.value and not in_cooldown:
            rule.state = AlertState.FIRING.value
            rule.last_triggered_at = now
            event = _event(rule, "fired", m, now)
    elif rule.state == AlertState.FIRING.value:
        rule.state = AlertState.OK.value
        rule.last_resolved_at = now
        event = _event(rule, "resolved", m, now)

    if event is not None:
        session.add(event)
        session.flush()
    return event


def _event(rule: AlertRule, kind: str, m: Measurement, now: datetime) -> AlertEvent:
    return AlertEvent(
        rule_id=rule.id,
        project_id=rule.project_id,
        kind=kind,
        created_at=now,
        failure_rate=m.failure_rate,
        failed=m.failed,
        finished=m.finished,
        window_since=m.since,
        window_until=m.until,
    )


def evaluate_all(
    session: Session, now: datetime, *, project_id: int | None = None
) -> list[AlertEvent]:
    stmt = select(AlertRule).where(AlertRule.enabled.is_(True)).order_by(AlertRule.id)
    if project_id is not None:
        stmt = stmt.where(AlertRule.project_id == project_id)
    events: list[AlertEvent] = []
    for rule in session.scalars(stmt):
        event = evaluate_rule(session, rule, now)
        if event is not None:
            events.append(event)
    return events


# -- webhook delivery --------------------------------------------------------------------------


def _scope(rule: AlertRule) -> str:
    parts = []
    if rule.environment:
        parts.append(f"env={rule.environment}")
    if rule.task_name:
        parts.append(f"task={rule.task_name}")
    return ", ".join(parts) if parts else "all tasks"


def build_payload(
    rule: AlertRule, event: AlertEvent, project: Project, base_url: str | None
) -> dict[str, Any]:
    rate = event.failure_rate
    rate_text = "n/a" if rate is None else f"{rate * 100:.1f}%"
    icon = {"fired": "\U0001f534", "resolved": "\U0001f7e2"}.get(event.kind, "\U0001f9ea")
    verb = {"fired": "firing", "resolved": "resolved", "test": "test"}[event.kind]
    text = (
        f"{icon} QueueLoom alert {verb}: {rule.name} — failure rate {rate_text} "
        f"({event.failed}/{event.finished} runs) over the last {rule.window_minutes}m, "
        f"project {project.name}, {_scope(rule)}."
    )
    link = None
    if base_url:
        link = f"{base_url.rstrip('/')}/projects/{project.name}/alerts"
        text += f" {link}"
    payload: dict[str, Any] = {
        "type": f"queueloom.alert.{event.kind}",
        "text": text,
        "project": project.name,
        "rule": {
            "id": rule.id,
            "name": rule.name,
            "environment": rule.environment,
            "task_name": rule.task_name,
            "window_minutes": rule.window_minutes,
            "threshold": rule.threshold,
            "min_runs": rule.min_runs,
        },
        "failure_rate": rate,
        "failed": event.failed,
        "finished": event.finished,
        "window": {
            "since": event.window_since.isoformat() if event.window_since else None,
            "until": event.window_until.isoformat() if event.window_until else None,
        },
        "occurred_at": event.created_at.isoformat(),
        "link": link,
    }
    if rule.webhook_format == "slack":
        return {"text": text}
    return payload


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def deliver(
    rule: AlertRule, event: AlertEvent, payload: dict[str, Any], client: httpx.Client
) -> None:
    """POST the payload; record the outcome on the event. Never raises."""
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": "queueloom-alerts"}
    if rule.webhook_secret:
        headers[SIGNATURE_HEADER] = sign(rule.webhook_secret, body)
    try:
        response = client.post(rule.webhook_url, content=body, headers=headers)
    except httpx.HTTPError as exc:
        event.delivered = False
        event.delivery_status = f"{type(exc).__name__}: {exc}"[:200]
        log.warning("alert %s webhook failed: %s", rule.name, event.delivery_status)
        return
    event.delivered = response.status_code < 300
    event.delivery_status = f"HTTP {response.status_code}"[:200]
    if not event.delivered:
        log.warning("alert %s webhook returned %s", rule.name, response.status_code)


def run_evaluation(
    session_factory: sessionmaker[Session],
    *,
    now: datetime | None = None,
    project_id: int | None = None,
    client: httpx.Client | None = None,
    base_url: str | None = None,
    timeout: float = 10.0,
) -> list[int]:
    """Evaluate rules, commit transitions, then deliver webhooks. Returns event ids."""
    now = now or datetime.now(UTC)
    session = session_factory()
    try:
        events = evaluate_all(session, now, project_id=project_id)
        pending = [(e.id, e.rule_id) for e in events]
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    if not pending:
        return []

    owns_client = client is None
    client = client or httpx.Client(timeout=timeout)
    try:
        for event_id, _rule_id in pending:
            deliver_event(session_factory, event_id, client=client, base_url=base_url)
    finally:
        if owns_client:
            client.close()
    return [event_id for event_id, _ in pending]


def deliver_event(
    session_factory: sessionmaker[Session],
    event_id: int,
    *,
    client: httpx.Client,
    base_url: str | None,
) -> None:
    session = session_factory()
    try:
        event = session.get(AlertEvent, event_id)
        if event is None:
            return
        rule = session.get(AlertRule, event.rule_id)
        project = session.get(Project, event.project_id)
        if rule is None or project is None:
            return
        deliver(rule, event, build_payload(rule, event, project, base_url), client)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
