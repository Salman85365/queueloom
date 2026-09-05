"""Retention: delete telemetry older than the configured window.

Runs from the server's background loop (every few hours by default) or from
``queueloom cleanup``. Deletes in bounded batches so a large backlog never holds one long
transaction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from queueloom.server.models import AlertEvent, Base, TaskEventRow, TaskRun

log = logging.getLogger("queueloom.server.retention")


@dataclass(frozen=True)
class CleanupResult:
    cutoff: datetime
    task_events: int
    task_runs: int
    alert_events: int

    @property
    def total(self) -> int:
        return self.task_events + self.task_runs + self.alert_events


def _delete_batched(
    session: Session, model: type[Base], id_column: Any, predicate: Any, batch_size: int
) -> int:
    deleted = 0
    while True:
        ids = list(session.scalars(select(id_column).where(predicate).limit(batch_size)))
        if not ids:
            return deleted
        session.execute(delete(model).where(id_column.in_(ids)))
        session.commit()
        deleted += len(ids)
        if len(ids) < batch_size:
            return deleted


def cleanup(
    session_factory: sessionmaker[Session],
    *,
    retention_days: int,
    now: datetime | None = None,
    batch_size: int = 5_000,
    project_id: int | None = None,
) -> CleanupResult:
    """Delete runs, events and alert history whose last activity predates the cutoff."""
    if retention_days <= 0:
        raise ValueError("retention_days must be positive")
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=retention_days)
    session = session_factory()
    try:
        ev_pred = TaskEventRow.timestamp < cutoff
        run_pred = TaskRun.last_event_at < cutoff
        alert_pred = AlertEvent.created_at < cutoff
        if project_id is not None:
            ev_pred = ev_pred & (TaskEventRow.project_id == project_id)
            run_pred = run_pred & (TaskRun.project_id == project_id)
            alert_pred = alert_pred & (AlertEvent.project_id == project_id)
        events = _delete_batched(session, TaskEventRow, TaskEventRow.id, ev_pred, batch_size)
        runs = _delete_batched(session, TaskRun, TaskRun.id, run_pred, batch_size)
        alerts = _delete_batched(session, AlertEvent, AlertEvent.id, alert_pred, batch_size)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    result = CleanupResult(cutoff=cutoff, task_events=events, task_runs=runs, alert_events=alerts)
    if result.total:
        log.info(
            "retention: removed %d events, %d runs, %d alert events older than %s",
            events,
            runs,
            alerts,
            cutoff.isoformat(),
        )
    return result
