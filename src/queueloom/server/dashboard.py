"""Server-rendered dashboard. Read-only; intended for self-hosted/trusted networks in the MVP."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from queueloom.server.alerts import measure
from queueloom.server.deps import SessionDep
from queueloom.server.models import AlertEvent, AlertRule, RunState
from queueloom.server.projects import get_project_by_name, list_projects
from queueloom.server.queries import (
    RunFilters,
    compute_stats,
    count_runs,
    distinct_values,
    get_run,
    list_events_for_task,
    list_runs,
)
from queueloom.server.timeutil import DEFAULT_RANGE, resolve_window

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

RANGE_CHOICES = ["15m", "1h", "6h", "24h", "7d", "30d"]
PAGE_SIZE = 50
EMPTY = "\u2013"  # en dash shown for missing values


def fmt_ms(value: float | None) -> str:
    if value is None:
        return EMPTY
    if value < 1000:
        return f"{value:.0f} ms"
    if value < 60_000:
        return f"{value / 1000:.2f} s"
    return f"{value / 60_000:.1f} min"


def fmt_pct(value: float | None) -> str:
    return EMPTY if value is None else f"{value * 100:.1f}%"


def fmt_ts(value: datetime | None) -> str:
    return EMPTY if value is None else value.strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_ago(value: datetime | None, now: datetime) -> str:
    if value is None:
        return EMPTY
    seconds = max(0, int((now - value).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


templates.env.filters["ms"] = fmt_ms
templates.env.filters["pct"] = fmt_pct
templates.env.filters["ts"] = fmt_ts


def _project_or_404(session: SessionDep, name: str) -> Any:
    project = get_project_by_name(session, name)
    if project is None:
        raise HTTPException(status_code=404, detail="project not found")
    return project


def _filters_from_query(request: Request) -> tuple[RunFilters, dict[str, str]]:
    q = request.query_params
    raw = {
        "environment": q.get("environment", ""),
        "task_name": q.get("task_name", ""),
        "state": q.get("state", ""),
        "queue": q.get("queue", ""),
        "range": q.get("range", DEFAULT_RANGE),
    }
    try:
        since, until = resolve_window(None, None, raw["range"])
    except ValueError:
        raw["range"] = DEFAULT_RANGE
        since, until = resolve_window(None, None, DEFAULT_RANGE)
    filters = RunFilters(
        environment=raw["environment"] or None,
        task_name=raw["task_name"] or None,
        state=raw["state"] or None,
        queue=raw["queue"] or None,
        since=since,
        until=until,
    )
    return filters, raw


def _common_context(request: Request, session: SessionDep, project: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "request": request,
        "project": project,
        "projects": list_projects(session),
        "environments": distinct_values(session, project.id, "environment"),
        "task_names": distinct_values(session, project.id, "task_name"),
        "queues": distinct_values(session, project.id, "queue"),
        "states": [s.value for s in RunState],
        "range_choices": RANGE_CHOICES,
        "refresh_seconds": request.app.state.settings.dashboard_refresh_seconds,
        "now": now,
        "ago": lambda dt: fmt_ago(dt, now),
    }


@router.get("/", response_class=HTMLResponse)
def index(request: Request, session: SessionDep) -> Any:
    projects = list_projects(session)
    return templates.TemplateResponse(
        request, "index.html", {"projects": projects, "project": None}
    )


@router.get("/projects/{name}", response_class=HTMLResponse)
def overview(name: str, request: Request, session: SessionDep) -> Any:
    project = _project_or_404(session, name)
    filters, raw = _filters_from_query(request)
    stats = compute_stats(
        session, project.id, filters, sample_limit=request.app.state.settings.stats_sample_limit
    )
    recent_failures = list_runs(
        session,
        project.id,
        RunFilters(
            environment=filters.environment,
            task_name=filters.task_name,
            queue=filters.queue,
            state=RunState.FAILED.value,
            since=filters.since,
            until=filters.until,
        ),
        limit=10,
    )
    ctx = _common_context(request, session, project)
    ctx.update({"stats": stats, "filters": raw, "recent_failures": recent_failures})
    return templates.TemplateResponse(request, "overview.html", ctx)


@router.get("/projects/{name}/tasks", response_class=HTMLResponse)
def task_list(name: str, request: Request, session: SessionDep) -> Any:
    project = _project_or_404(session, name)
    filters, raw = _filters_from_query(request)
    try:
        page = max(1, int(request.query_params.get("page", "1")))
    except ValueError:
        page = 1
    total = count_runs(session, project.id, filters)
    runs = list_runs(session, project.id, filters, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE)
    ctx = _common_context(request, session, project)
    ctx.update(
        {
            "runs": runs,
            "filters": raw,
            "total": total,
            "page": page,
            "pages": max(1, -(-total // PAGE_SIZE)),
        }
    )
    return templates.TemplateResponse(request, "tasks.html", ctx)


@router.get("/projects/{name}/tasks/{task_id}", response_class=HTMLResponse)
def task_detail(name: str, task_id: str, request: Request, session: SessionDep) -> Any:
    project = _project_or_404(session, name)
    run = get_run(session, project.id, task_id)
    if run is None:
        raise HTTPException(status_code=404, detail="task not found")
    events = list_events_for_task(session, project.id, task_id)
    ctx = _common_context(request, session, project)
    ctx.update({"run": run, "events": events})
    return templates.TemplateResponse(request, "task_detail.html", ctx)


@router.get("/projects/{name}/alerts", response_class=HTMLResponse)
def alerts_page(name: str, request: Request, session: SessionDep) -> Any:
    project = _project_or_404(session, name)
    now = datetime.now(UTC)
    rules = list(
        session.scalars(
            select(AlertRule).where(AlertRule.project_id == project.id).order_by(AlertRule.id)
        )
    )
    current = {rule.id: measure(session, rule, now) for rule in rules}
    events = list(
        session.scalars(
            select(AlertEvent)
            .where(AlertEvent.project_id == project.id)
            .order_by(AlertEvent.id.desc())
            .limit(50)
        )
    )
    rule_names = {rule.id: rule.name for rule in rules}
    ctx = _common_context(request, session, project)
    ctx.update({"rules": rules, "current": current, "events": events, "rule_names": rule_names})
    return templates.TemplateResponse(request, "alerts.html", ctx)
