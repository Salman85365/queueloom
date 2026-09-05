"""JSON API: ingestion plus read endpoints for the project that owns the API key."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select

from queueloom import __version__
from queueloom.ai import SummaryError, get_provider
from queueloom.events import SCHEMA_VERSION, EventBatch
from queueloom.server.alerts import (
    WEBHOOK_FORMATS,
    build_payload,
    deliver,
    evaluate_all,
    measure,
)
from queueloom.server.deps import ProjectDep, SessionDep
from queueloom.server.diagnosis import diagnose, render_report
from queueloom.server.ingest import ingest_events
from queueloom.server.models import AlertEvent, AlertRule, TaskEventRow, TaskRun
from queueloom.server.queries import (
    RunFilters,
    compute_stats,
    count_runs,
    get_run,
    list_events_for_task,
    list_runs,
)
from queueloom.server.timeutil import resolve_window

router = APIRouter(prefix="/v1")


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int


def run_to_dict(run: TaskRun) -> dict[str, Any]:
    return {
        "task_id": run.task_id,
        "task_name": run.task_name,
        "queue": run.queue,
        "environment": run.environment,
        "worker": run.worker,
        "state": run.state,
        "published_at": run.published_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "last_event_at": run.last_event_at,
        "eta": run.eta,
        "queue_latency_ms": run.queue_latency_ms,
        "duration_ms": run.duration_ms,
        "retries": run.retries,
        "attempts": run.attempts,
        "exception_type": run.exception_type,
        "exception_message": run.exception_message,
        "parent_id": run.parent_id,
        "root_id": run.root_id,
        "args_repr": run.args_repr,
        "kwargs_repr": run.kwargs_repr,
    }


def event_to_dict(row: TaskEventRow) -> dict[str, Any]:
    return {
        "event_id": row.event_id,
        "event_type": row.event_type,
        "timestamp": row.timestamp,
        "received_at": row.received_at,
        "payload": row.payload,
    }


@router.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "version": __version__, "schema_version": SCHEMA_VERSION}


@router.post("/events", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
def ingest(batch: EventBatch, session: SessionDep, project: ProjectDep) -> IngestResponse:
    result = ingest_events(session, project, batch.events)
    return IngestResponse(accepted=result.accepted, duplicates=result.duplicates)


@router.get("/projects/me")
def me(project: ProjectDep) -> dict[str, Any]:
    return {"id": project.id, "name": project.name, "created_at": project.created_at}


def _filters(
    environment: str | None,
    task_name: str | None,
    state: str | None,
    queue: str | None,
    since: datetime | None,
    until: datetime | None,
    range_: str | None,
) -> RunFilters:
    try:
        since_dt, until_dt = resolve_window(since, until, range_)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return RunFilters(
        environment=environment or None,
        task_name=task_name or None,
        state=state or None,
        queue=queue or None,
        since=since_dt,
        until=until_dt,
    )


@router.get("/tasks")
def tasks(
    session: SessionDep,
    project: ProjectDep,
    environment: str | None = None,
    task_name: str | None = None,
    state: str | None = None,
    queue: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    range: Annotated[str | None, Query(alias="range")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    filters = _filters(environment, task_name, state, queue, since, until, range)
    runs = list_runs(session, project.id, filters, limit=limit, offset=offset)
    return {
        "total": count_runs(session, project.id, filters),
        "limit": limit,
        "offset": offset,
        "items": [run_to_dict(r) for r in runs],
    }


@router.get("/tasks/{task_id}")
def task_detail(task_id: str, session: SessionDep, project: ProjectDep) -> dict[str, Any]:
    run = get_run(session, project.id, task_id)
    if run is None:
        raise HTTPException(status_code=404, detail="task not found")
    events = list_events_for_task(session, project.id, task_id)
    data = run_to_dict(run)
    data["traceback"] = run.traceback
    data["events"] = [event_to_dict(e) for e in events]
    return data


@router.get("/stats")
def stats(
    request: Request,
    session: SessionDep,
    project: ProjectDep,
    environment: str | None = None,
    task_name: str | None = None,
    queue: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    range: Annotated[str | None, Query(alias="range")] = None,
) -> dict[str, Any]:
    filters = _filters(environment, task_name, None, queue, since, until, range)
    settings = request.app.state.settings
    return compute_stats(
        session, project.id, filters, sample_limit=settings.stats_sample_limit
    ).to_dict()


# -- alerts ------------------------------------------------------------------------------------


class AlertRuleIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    environment: str | None = Field(default=None, max_length=64)
    task_name: str | None = Field(default=None, max_length=255)
    window_minutes: int = Field(default=15, ge=1, le=7 * 24 * 60)
    threshold: float = Field(default=0.1, ge=0.0, le=1.0)
    min_runs: int = Field(default=10, ge=1)
    cooldown_minutes: int = Field(default=30, ge=0)
    webhook_url: str = Field(max_length=2000)
    webhook_format: str = "json"
    webhook_secret: str | None = Field(default=None, max_length=200)
    enabled: bool = True

    @field_validator("webhook_url")
    @classmethod
    def _http_only(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("webhook_url must start with http:// or https://")
        return value

    @field_validator("webhook_format")
    @classmethod
    def _known_format(cls, value: str) -> str:
        if value not in WEBHOOK_FORMATS:
            raise ValueError(f"webhook_format must be one of {WEBHOOK_FORMATS}")
        return value


def rule_to_dict(rule: AlertRule) -> dict[str, Any]:
    return {
        "id": rule.id,
        "name": rule.name,
        "environment": rule.environment,
        "task_name": rule.task_name,
        "window_minutes": rule.window_minutes,
        "threshold": rule.threshold,
        "min_runs": rule.min_runs,
        "cooldown_minutes": rule.cooldown_minutes,
        "webhook_url": rule.webhook_url,
        "webhook_format": rule.webhook_format,
        "has_secret": bool(rule.webhook_secret),
        "enabled": rule.enabled,
        "state": rule.state,
        "last_evaluated_at": rule.last_evaluated_at,
        "last_triggered_at": rule.last_triggered_at,
        "last_resolved_at": rule.last_resolved_at,
        "created_at": rule.created_at,
    }


def alert_event_to_dict(event: AlertEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "rule_id": event.rule_id,
        "kind": event.kind,
        "created_at": event.created_at,
        "failure_rate": event.failure_rate,
        "failed": event.failed,
        "finished": event.finished,
        "window_since": event.window_since,
        "window_until": event.window_until,
        "delivered": event.delivered,
        "delivery_status": event.delivery_status,
    }


def _rule_or_404(session: SessionDep, project: ProjectDep, rule_id: int) -> AlertRule:
    rule = session.scalar(
        select(AlertRule).where(AlertRule.id == rule_id, AlertRule.project_id == project.id)
    )
    if rule is None:
        raise HTTPException(status_code=404, detail="alert rule not found")
    return rule


@router.post("/alerts", status_code=status.HTTP_201_CREATED)
def create_alert(body: AlertRuleIn, session: SessionDep, project: ProjectDep) -> dict[str, Any]:
    rule = AlertRule(project_id=project.id, **body.model_dump())
    session.add(rule)
    session.flush()
    return rule_to_dict(rule)


@router.get("/alerts")
def list_alerts(session: SessionDep, project: ProjectDep) -> dict[str, Any]:
    rules = session.scalars(
        select(AlertRule).where(AlertRule.project_id == project.id).order_by(AlertRule.id)
    )
    return {"items": [rule_to_dict(r) for r in rules]}


@router.get("/alerts/{rule_id}")
def get_alert(rule_id: int, session: SessionDep, project: ProjectDep) -> dict[str, Any]:
    rule = _rule_or_404(session, project, rule_id)
    data = rule_to_dict(rule)
    data["current"] = measure(session, rule, datetime.now(UTC)).__dict__
    return data


@router.delete("/alerts/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_alert(rule_id: int, session: SessionDep, project: ProjectDep) -> None:
    session.delete(_rule_or_404(session, project, rule_id))


@router.get("/alerts/{rule_id}/events")
def alert_events(
    rule_id: int,
    session: SessionDep,
    project: ProjectDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> dict[str, Any]:
    _rule_or_404(session, project, rule_id)
    events = session.scalars(
        select(AlertEvent)
        .where(AlertEvent.rule_id == rule_id)
        .order_by(AlertEvent.id.desc())
        .limit(limit)
    )
    return {"items": [alert_event_to_dict(e) for e in events]}


@router.post("/alerts/{rule_id}/test")
def test_alert(
    rule_id: int, request: Request, session: SessionDep, project: ProjectDep
) -> dict[str, Any]:
    """Send a test payload to the rule's webhook and report the delivery result."""
    rule = _rule_or_404(session, project, rule_id)
    now = datetime.now(UTC)
    m = measure(session, rule, now)
    event = AlertEvent(
        rule_id=rule.id,
        project_id=project.id,
        kind="test",
        created_at=now,
        failure_rate=m.failure_rate,
        failed=m.failed,
        finished=m.finished,
        window_since=m.since,
        window_until=m.until,
    )
    session.add(event)
    session.flush()
    settings = request.app.state.settings
    client: httpx.Client = getattr(request.app.state, "webhook_client", None) or httpx.Client(
        timeout=settings.webhook_timeout_seconds
    )
    deliver(rule, event, build_payload(rule, event, project, settings.public_base_url), client)
    return alert_event_to_dict(event)


@router.post("/alerts/evaluate")
def evaluate_alerts(request: Request, session: SessionDep, project: ProjectDep) -> dict[str, Any]:
    """Evaluate this project's rules now and deliver any resulting webhooks."""
    now = datetime.now(UTC)
    events = evaluate_all(session, now, project_id=project.id)
    settings = request.app.state.settings
    client: httpx.Client = getattr(request.app.state, "webhook_client", None) or httpx.Client(
        timeout=settings.webhook_timeout_seconds
    )
    for event in events:
        rule = session.get(AlertRule, event.rule_id)
        if rule is not None:
            deliver(
                rule, event, build_payload(rule, event, project, settings.public_base_url), client
            )
    return {"evaluated_at": now, "events": [alert_event_to_dict(e) for e in events]}


# -- diagnosis ---------------------------------------------------------------------------------


class SummaryRequest(BaseModel):
    question: str | None = Field(default=None, max_length=1000)


def _diagnosis(
    request: Request,
    session: SessionDep,
    project: ProjectDep,
    environment: str | None,
    since: datetime | None,
    until: datetime | None,
    range_: str | None,
) -> tuple[dict[str, Any], str]:
    filters = _filters(environment, None, None, None, since, until, range_)
    settings = request.app.state.settings
    result = diagnose(session, project.id, filters, sample_limit=settings.stats_sample_limit)
    return result.to_dict(), render_report(result, project.name)


@router.get("/diagnosis")
def diagnosis(
    request: Request,
    session: SessionDep,
    project: ProjectDep,
    environment: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    range: Annotated[str | None, Query(alias="range")] = None,
) -> dict[str, Any]:
    """Deterministic incident diagnosis for the window: clusters, regressions, stuck tasks."""
    data, report = _diagnosis(request, session, project, environment, since, until, range)
    data["report"] = report
    return data


@router.post("/diagnosis/summary")
def diagnosis_summary(
    request: Request,
    session: SessionDep,
    project: ProjectDep,
    body: SummaryRequest | None = None,
    environment: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    range: Annotated[str | None, Query(alias="range")] = None,
) -> dict[str, Any]:
    """Explain the diagnosis with the configured AI provider (or the report itself if none)."""
    data, report = _diagnosis(request, session, project, environment, since, until, range)
    provider = getattr(request.app.state, "summary_provider", None) or get_provider(
        request.app.state.settings
    )
    try:
        summary = provider.summarize(report, question=body.question if body else None)
    except SummaryError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return {"summary": summary.to_dict(), "diagnosis": data, "report": report}
