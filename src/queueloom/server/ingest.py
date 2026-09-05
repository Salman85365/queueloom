"""Turn raw events into stored rows and materialised task runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from queueloom.events import EventType, TaskEvent
from queueloom.server.models import Project, RunState, TaskEventRow, TaskRun


@dataclass(frozen=True)
class IngestResult:
    accepted: int
    duplicates: int


def _ms(later: datetime, earlier: datetime) -> float:
    return max(0.0, (later - earlier).total_seconds() * 1000.0)


def apply_event(run: TaskRun, event: TaskEvent) -> None:
    """Fold one event into a run. Pure with respect to the database.

    Events may arrive out of order (client and worker clocks, batching, retries). Descriptive
    fields are filled when missing; state only moves forward for events that are not older
    than the newest event already applied.
    """
    ts = event.timestamp
    stale = ts < run.last_event_at if run.last_event_at is not None else False

    if event.task_name and not run.task_name:
        run.task_name = event.task_name
    if event.queue and not run.queue:
        run.queue = event.queue
    if event.worker:
        run.worker = event.worker
    if event.parent_id and not run.parent_id:
        run.parent_id = event.parent_id
    if event.root_id and not run.root_id:
        run.root_id = event.root_id
    if event.args_repr and not run.args_repr:
        run.args_repr = event.args_repr
    if event.kwargs_repr and not run.kwargs_repr:
        run.kwargs_repr = event.kwargs_repr
    if event.eta and not run.eta:
        run.eta = event.eta
    if event.retries is not None:
        run.retries = max(run.retries or 0, event.retries)
    if event.published_at and (run.published_at is None or event.published_at > run.published_at):
        run.published_at = event.published_at

    kind = event.event_type
    if kind == EventType.PUBLISHED:
        run.attempts = (run.attempts or 0) + 1
        if not stale:
            run.state = RunState.QUEUED.value
    elif kind == EventType.STARTED:
        run.started_at = ts
        if run.published_at is not None:
            run.queue_latency_ms = _ms(ts, run.published_at)
        if not stale:
            run.state = RunState.STARTED.value
    elif kind in (EventType.SUCCEEDED, EventType.FAILED):
        run.finished_at = ts
        if event.runtime_ms is not None:
            run.duration_ms = event.runtime_ms
        elif run.started_at is not None:
            run.duration_ms = _ms(ts, run.started_at)
        if kind == EventType.FAILED and event.exception is not None:
            run.exception_type = event.exception.type
            run.exception_message = event.exception.message
            run.traceback = event.exception.traceback
        if not stale:
            run.state = (
                RunState.SUCCEEDED.value if kind == EventType.SUCCEEDED else RunState.FAILED.value
            )
    elif kind == EventType.RETRIED:
        if event.exception is not None:
            run.exception_type = event.exception.type
            run.exception_message = event.exception.message
            run.traceback = event.exception.traceback
        if not stale:
            run.state = RunState.RETRYING.value
    elif kind == EventType.REVOKED:
        run.finished_at = ts
        if event.exception is not None:
            run.exception_type = event.exception.type
            run.exception_message = event.exception.message
        if not stale:
            run.state = RunState.REVOKED.value

    if run.last_event_at is None or ts > run.last_event_at:
        run.last_event_at = ts


def _new_run(project: Project, event: TaskEvent) -> TaskRun:
    return TaskRun(
        project_id=project.id,
        task_id=event.task_id,
        environment=event.environment,
        state=RunState.QUEUED.value,
        last_event_at=event.timestamp,
        retries=0,
        attempts=0,
    )


def _to_row(project: Project, event: TaskEvent) -> TaskEventRow:
    return TaskEventRow(
        event_id=event.event_id,
        project_id=project.id,
        schema_version=event.schema_version,
        event_type=event.event_type.value,
        task_id=event.task_id,
        task_name=event.task_name,
        environment=event.environment,
        timestamp=event.timestamp,
        payload=event.model_dump(mode="json"),
    )


def ingest_events(session: Session, project: Project, events: list[TaskEvent]) -> IngestResult:
    """Store a batch of events for ``project`` and update the affected runs.

    Idempotent per ``event_id``: re-sent events (SDK retries) are counted as duplicates and
    ignored. The caller owns the transaction.
    """
    if not events:
        return IngestResult(accepted=0, duplicates=0)

    # Sort so that folding is deterministic regardless of batch order.
    ordered = sorted(events, key=lambda e: (e.timestamp, e.event_id))

    incoming_ids = {e.event_id for e in ordered}
    existing_ids = set(
        session.scalars(
            select(TaskEventRow.event_id).where(TaskEventRow.event_id.in_(incoming_ids))
        )
    )
    fresh = [e for e in ordered if e.event_id not in existing_ids]
    duplicates = len(ordered) - len(fresh)
    # A batch may legitimately contain the same event twice.
    seen: set[str] = set()
    unique: list[TaskEvent] = []
    for e in fresh:
        if e.event_id in seen:
            duplicates += 1
            continue
        seen.add(e.event_id)
        unique.append(e)
    if not unique:
        return IngestResult(accepted=0, duplicates=duplicates)

    task_ids = {e.task_id for e in unique}
    runs: dict[str, TaskRun] = {
        run.task_id: run
        for run in session.scalars(
            select(TaskRun).where(TaskRun.project_id == project.id, TaskRun.task_id.in_(task_ids))
        )
    }

    savepoint = session.begin_nested()
    try:
        for event in unique:
            session.add(_to_row(project, event))
            run = runs.get(event.task_id)
            if run is None:
                run = _new_run(project, event)
                session.add(run)
                runs[event.task_id] = run
            apply_event(run, event)
        session.flush()
        savepoint.commit()
    except IntegrityError:
        # Concurrent writer inserted one of our event_ids between the check and the flush.
        savepoint.rollback()
        session.expire_all()
        return _ingest_one_by_one(session, project, unique, duplicates)
    return IngestResult(accepted=len(unique), duplicates=duplicates)


def _ingest_one_by_one(
    session: Session, project: Project, events: list[TaskEvent], duplicates: int
) -> IngestResult:
    accepted = 0
    for event in events:
        savepoint = session.begin_nested()
        try:
            run = session.scalar(
                select(TaskRun).where(
                    TaskRun.project_id == project.id, TaskRun.task_id == event.task_id
                )
            )
            session.add(_to_row(project, event))
            if run is None:
                run = _new_run(project, event)
                session.add(run)
            apply_event(run, event)
            session.flush()
            savepoint.commit()
            accepted += 1
        except IntegrityError:
            savepoint.rollback()
            duplicates += 1
    return IngestResult(accepted=accepted, duplicates=duplicates)
