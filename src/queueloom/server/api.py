"""JSON API: ingestion plus read endpoints for the project that owns the API key."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel

from queueloom import __version__
from queueloom.events import SCHEMA_VERSION, EventBatch
from queueloom.server.deps import ProjectDep, SessionDep
from queueloom.server.ingest import ingest_events
from queueloom.server.models import TaskEventRow, TaskRun
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
