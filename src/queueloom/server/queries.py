"""Read-side queries used by the API and the dashboard."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from math import ceil
from typing import Any, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from queueloom.server.models import RunState, TaskEventRow, TaskRun

SelectT = TypeVar("SelectT", bound=Select[Any])


@dataclass
class RunFilters:
    environment: str | None = None
    task_name: str | None = None
    state: str | None = None
    queue: str | None = None
    since: datetime | None = None
    until: datetime | None = None


def _apply_filters(stmt: SelectT, project_id: int, f: RunFilters) -> SelectT:
    stmt = stmt.where(TaskRun.project_id == project_id)
    if f.environment:
        stmt = stmt.where(TaskRun.environment == f.environment)
    if f.task_name:
        stmt = stmt.where(TaskRun.task_name == f.task_name)
    if f.state:
        stmt = stmt.where(TaskRun.state == f.state)
    if f.queue:
        stmt = stmt.where(TaskRun.queue == f.queue)
    if f.since:
        stmt = stmt.where(TaskRun.last_event_at >= f.since)
    if f.until:
        stmt = stmt.where(TaskRun.last_event_at <= f.until)
    return stmt


def list_runs(
    session: Session,
    project_id: int,
    filters: RunFilters,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[TaskRun]:
    stmt = _apply_filters(select(TaskRun), project_id, filters)
    stmt = (
        stmt.order_by(TaskRun.last_event_at.desc(), TaskRun.id.desc()).limit(limit).offset(offset)
    )
    return list(session.scalars(stmt))


def count_runs(session: Session, project_id: int, filters: RunFilters) -> int:
    stmt = _apply_filters(select(TaskRun), project_id, filters)
    return session.scalar(select(func.count()).select_from(stmt.subquery())) or 0


def get_run(session: Session, project_id: int, task_id: str) -> TaskRun | None:
    return session.scalar(
        select(TaskRun).where(TaskRun.project_id == project_id, TaskRun.task_id == task_id)
    )


def list_events_for_task(session: Session, project_id: int, task_id: str) -> list[TaskEventRow]:
    stmt = (
        select(TaskEventRow)
        .where(TaskEventRow.project_id == project_id, TaskEventRow.task_id == task_id)
        .order_by(TaskEventRow.timestamp, TaskEventRow.id)
    )
    return list(session.scalars(stmt))


def distinct_values(session: Session, project_id: int, column: str) -> list[str]:
    col = getattr(TaskRun, column)
    stmt = (
        select(col)
        .where(TaskRun.project_id == project_id, col.is_not(None))
        .distinct()
        .order_by(col)
    )
    return [str(v) for v in session.scalars(stmt)]


# -- statistics ------------------------------------------------------------------------------


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile; good enough for dashboards, no numpy dependency."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, ceil(pct / 100.0 * len(ordered)) - 1))
    return ordered[rank]


@dataclass
class TaskNameStats:
    task_name: str
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    retrying: int = 0
    durations_ms: list[float] = field(default_factory=list, repr=False)
    latencies_ms: list[float] = field(default_factory=list, repr=False)

    @property
    def failure_rate(self) -> float | None:
        finished = self.succeeded + self.failed
        return None if finished == 0 else self.failed / finished

    @property
    def p50_duration_ms(self) -> float | None:
        return percentile(self.durations_ms, 50)

    @property
    def p95_duration_ms(self) -> float | None:
        return percentile(self.durations_ms, 95)

    @property
    def avg_duration_ms(self) -> float | None:
        return sum(self.durations_ms) / len(self.durations_ms) if self.durations_ms else None

    @property
    def p95_latency_ms(self) -> float | None:
        return percentile(self.latencies_ms, 95)

    def to_dict(self) -> dict[str, object]:
        return {
            "task_name": self.task_name,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "retrying": self.retrying,
            "failure_rate": self.failure_rate,
            "avg_duration_ms": self.avg_duration_ms,
            "p50_duration_ms": self.p50_duration_ms,
            "p95_duration_ms": self.p95_duration_ms,
            "p95_queue_latency_ms": self.p95_latency_ms,
        }


@dataclass
class Stats:
    since: datetime
    until: datetime
    total: int
    by_state: dict[str, int]
    failure_rate: float | None
    avg_duration_ms: float | None
    p50_duration_ms: float | None
    p95_duration_ms: float | None
    avg_queue_latency_ms: float | None
    p95_queue_latency_ms: float | None
    total_retries: int
    by_task: list[TaskNameStats]
    sampled: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "since": self.since.isoformat(),
            "until": self.until.isoformat(),
            "total": self.total,
            "by_state": self.by_state,
            "failure_rate": self.failure_rate,
            "avg_duration_ms": self.avg_duration_ms,
            "p50_duration_ms": self.p50_duration_ms,
            "p95_duration_ms": self.p95_duration_ms,
            "avg_queue_latency_ms": self.avg_queue_latency_ms,
            "p95_queue_latency_ms": self.p95_queue_latency_ms,
            "total_retries": self.total_retries,
            "by_task": [t.to_dict() for t in self.by_task],
            "sampled": self.sampled,
        }


def compute_stats(
    session: Session,
    project_id: int,
    filters: RunFilters,
    *,
    sample_limit: int = 50_000,
) -> Stats:
    assert filters.since is not None and filters.until is not None
    by_state = {s.value: 0 for s in RunState}
    count_stmt = _apply_filters(
        select(TaskRun.state, func.count()).group_by(TaskRun.state),
        project_id,
        filters,
    )
    total = 0
    for state, count in session.execute(count_stmt):
        by_state[str(state)] = int(count)
        total += int(count)

    sample_stmt = (
        _apply_filters(
            select(
                TaskRun.task_name,
                TaskRun.state,
                TaskRun.duration_ms,
                TaskRun.queue_latency_ms,
                TaskRun.retries,
            ),
            project_id,
            filters,
        )
        .order_by(TaskRun.last_event_at.desc())
        .limit(sample_limit)
    )

    durations: list[float] = []
    latencies: list[float] = []
    total_retries = 0
    per_task: dict[str, TaskNameStats] = defaultdict(lambda: TaskNameStats(task_name=""))
    sampled_rows = 0
    for task_name, state, duration_ms, latency_ms, retries in session.execute(sample_stmt):
        sampled_rows += 1
        name = task_name or "(unknown)"
        ts = per_task[name]
        ts.task_name = name
        ts.total += 1
        if state == RunState.SUCCEEDED.value:
            ts.succeeded += 1
        elif state == RunState.FAILED.value:
            ts.failed += 1
        elif state == RunState.RETRYING.value:
            ts.retrying += 1
        if duration_ms is not None:
            durations.append(duration_ms)
            ts.durations_ms.append(duration_ms)
        if latency_ms is not None:
            latencies.append(latency_ms)
            ts.latencies_ms.append(latency_ms)
        total_retries += int(retries or 0)

    finished = by_state[RunState.SUCCEEDED.value] + by_state[RunState.FAILED.value]
    return Stats(
        since=filters.since,
        until=filters.until,
        total=total,
        by_state=by_state,
        failure_rate=(by_state[RunState.FAILED.value] / finished) if finished else None,
        avg_duration_ms=sum(durations) / len(durations) if durations else None,
        p50_duration_ms=percentile(durations, 50),
        p95_duration_ms=percentile(durations, 95),
        avg_queue_latency_ms=sum(latencies) / len(latencies) if latencies else None,
        p95_queue_latency_ms=percentile(latencies, 95),
        total_retries=total_retries,
        by_task=sorted(per_task.values(), key=lambda t: (-t.failed, -t.total, t.task_name)),
        sampled=sampled_rows >= sample_limit,
    )
