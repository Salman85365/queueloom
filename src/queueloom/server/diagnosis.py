"""Deterministic incident diagnosis.

Before any language model gets involved, QueueLoom computes the facts: which tasks are failing
more than they were in the previous window, how the failures cluster by exception, which tasks
got slower, what is stuck in the queue. That report is useful on its own and is the only input
the AI summary sees, so the model cannot invent telemetry.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from queueloom.server.models import RunState, TaskRun
from queueloom.server.queries import RunFilters, TaskNameStats, compute_stats

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_HEX = re.compile(r"\b[0-9a-fA-F]{12,}\b")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
_WS = re.compile(r"\s+")

STUCK_QUEUED_AFTER = timedelta(minutes=5)
STUCK_RUNNING_AFTER = timedelta(minutes=30)
MAX_CLUSTERS = 10
MAX_SAMPLE_IDS = 5
MAX_TRACEBACK_CHARS = 3000


def normalise_message(message: str | None) -> str:
    """Collapse variable parts of an error message so similar errors cluster together."""
    if not message:
        return ""
    text = _UUID.sub("<uuid>", message)
    text = _EMAIL.sub("<email>", text)
    text = _HEX.sub("<hex>", text)
    text = _NUMBER.sub("#", text)
    text = _WS.sub(" ", text).strip()
    return text[:120]


@dataclass
class ErrorCluster:
    task_name: str
    exception_type: str
    message_pattern: str
    count: int = 0
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    sample_task_ids: list[str] = field(default_factory=list)
    sample_message: str | None = None
    sample_traceback: str | None = None
    workers: list[str] = field(default_factory=list)


@dataclass
class TaskFinding:
    task_name: str
    total: int
    failed: int
    failure_rate: float | None
    baseline_failure_rate: float | None
    p95_duration_ms: float | None
    baseline_p95_duration_ms: float | None
    p95_queue_latency_ms: float | None
    baseline_p95_queue_latency_ms: float | None
    retrying: int
    flags: list[str]


@dataclass
class Diagnosis:
    since: datetime
    until: datetime
    baseline_since: datetime
    environment: str | None
    total_runs: int
    failed_runs: int
    failure_rate: float | None
    baseline_total_runs: int
    baseline_failure_rate: float | None
    error_clusters: list[ErrorCluster]
    task_findings: list[TaskFinding]
    stuck_queued: int
    stuck_running: int
    workers_with_failures: dict[str, int]
    headline: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        return data


def _rate(stats: TaskNameStats | None) -> float | None:
    return None if stats is None else stats.failure_rate


def _flags(cur: TaskNameStats, base: TaskNameStats | None) -> list[str]:
    flags: list[str] = []
    cur_rate = cur.failure_rate
    base_rate = _rate(base)
    if cur.failed and (base is None or base.failed == 0):
        flags.append("new_failures")
    if (
        cur_rate is not None
        and cur_rate >= 0.05
        and (base_rate is None or cur_rate >= 2 * base_rate + 0.02)
    ):
        flags.append("failure_rate_up")
    cur_p95, base_p95 = cur.p95_duration_ms, base.p95_duration_ms if base else None
    if cur_p95 is not None and base_p95 is not None and cur_p95 >= 2 * base_p95 and cur_p95 > 500:
        flags.append("slower")
    cur_lat, base_lat = cur.p95_latency_ms, base.p95_latency_ms if base else None
    if cur_lat is not None and base_lat is not None and cur_lat >= 2 * base_lat and cur_lat > 1000:
        flags.append("queue_latency_up")
    if cur.total and cur.retrying / cur.total >= 0.2:
        flags.append("retry_storm")
    return flags


def _clusters(
    session: Session, project_id: int, filters: RunFilters, limit: int
) -> list[ErrorCluster]:
    stmt = (
        select(TaskRun)
        .where(
            TaskRun.project_id == project_id,
            TaskRun.state == RunState.FAILED.value,
            TaskRun.last_event_at >= filters.since,
            TaskRun.last_event_at <= filters.until,
        )
        .order_by(TaskRun.last_event_at.desc())
        .limit(limit)
    )
    if filters.environment:
        stmt = stmt.where(TaskRun.environment == filters.environment)
    clusters: dict[tuple[str, str, str], ErrorCluster] = {}
    for run in session.scalars(stmt):
        key = (
            run.task_name or "(unknown)",
            run.exception_type or "Unknown",
            normalise_message(run.exception_message),
        )
        cluster = clusters.get(key)
        if cluster is None:
            cluster = clusters[key] = ErrorCluster(*key)
        cluster.count += 1
        seen = run.finished_at or run.last_event_at
        if cluster.first_seen is None or seen < cluster.first_seen:
            cluster.first_seen = seen
        if cluster.last_seen is None or seen > cluster.last_seen:
            cluster.last_seen = seen
        if len(cluster.sample_task_ids) < MAX_SAMPLE_IDS:
            cluster.sample_task_ids.append(run.task_id)
        if cluster.sample_message is None:
            cluster.sample_message = (run.exception_message or "")[:500]
        if cluster.sample_traceback is None and run.traceback:
            cluster.sample_traceback = run.traceback[-MAX_TRACEBACK_CHARS:]
        if run.worker and run.worker not in cluster.workers:
            cluster.workers.append(run.worker)
    ordered = sorted(clusters.values(), key=lambda c: (-c.count, c.task_name))
    return ordered[:MAX_CLUSTERS]


def _count(session: Session, project_id: int, environment: str | None, *conditions: Any) -> int:
    stmt = (
        select(func.count())
        .select_from(TaskRun)
        .where(TaskRun.project_id == project_id, *conditions)
    )
    if environment:
        stmt = stmt.where(TaskRun.environment == environment)
    return int(session.scalar(stmt) or 0)


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def _headline(d: Diagnosis) -> str:
    window = d.until - d.since
    minutes = int(window.total_seconds() // 60)
    span = f"{minutes}m" if minutes < 120 else f"{minutes // 60}h"
    if d.total_runs == 0:
        return f"No task runs in the last {span}."
    parts = [
        f"{d.failed_runs} of {d.total_runs} runs failed ({_pct(d.failure_rate)}) in the last {span}"
    ]
    if d.baseline_total_runs:
        parts[0] += f", versus {_pct(d.baseline_failure_rate)} in the previous {span}"
    parts[0] += "."
    if d.error_clusters:
        top = d.error_clusters[0]
        parts.append(
            f"Top error: {top.task_name} raising {top.exception_type} "
            f"({top.count} run{'s' if top.count != 1 else ''})."
        )
    if d.stuck_queued:
        minutes_q = int(STUCK_QUEUED_AFTER.total_seconds() // 60)
        parts.append(f"{d.stuck_queued} task(s) queued for over {minutes_q} minutes.")
    if d.stuck_running:
        minutes_r = int(STUCK_RUNNING_AFTER.total_seconds() // 60)
        parts.append(f"{d.stuck_running} task(s) running for over {minutes_r} minutes.")
    return " ".join(parts)


def diagnose(
    session: Session,
    project_id: int,
    filters: RunFilters,
    *,
    sample_limit: int = 50_000,
    cluster_limit: int = 5_000,
) -> Diagnosis:
    assert filters.since is not None and filters.until is not None
    window = filters.until - filters.since
    baseline_filters = RunFilters(
        environment=filters.environment, since=filters.since - window, until=filters.since
    )
    current = compute_stats(session, project_id, filters, sample_limit=sample_limit)
    baseline = compute_stats(session, project_id, baseline_filters, sample_limit=sample_limit)
    base_by_task = {t.task_name: t for t in baseline.by_task}

    findings: list[TaskFinding] = []
    for cur in current.by_task:
        base = base_by_task.get(cur.task_name)
        flags = _flags(cur, base)
        if not flags:
            continue
        findings.append(
            TaskFinding(
                task_name=cur.task_name,
                total=cur.total,
                failed=cur.failed,
                failure_rate=cur.failure_rate,
                baseline_failure_rate=_rate(base),
                p95_duration_ms=cur.p95_duration_ms,
                baseline_p95_duration_ms=base.p95_duration_ms if base else None,
                p95_queue_latency_ms=cur.p95_latency_ms,
                baseline_p95_queue_latency_ms=base.p95_latency_ms if base else None,
                retrying=cur.retrying,
                flags=flags,
            )
        )
    findings.sort(key=lambda f: (-f.failed, -(f.failure_rate or 0), f.task_name))

    clusters = _clusters(session, project_id, filters, cluster_limit)
    workers: dict[str, int] = defaultdict(int)
    for cluster in clusters:
        for worker in cluster.workers:
            workers[worker] += cluster.count if len(cluster.workers) == 1 else 1

    env = filters.environment
    stuck_queued = _count(
        session,
        project_id,
        env,
        TaskRun.state == RunState.QUEUED.value,
        TaskRun.published_at.is_not(None),
        TaskRun.published_at <= filters.until - STUCK_QUEUED_AFTER,
        TaskRun.last_event_at >= filters.since,
    )
    stuck_running = _count(
        session,
        project_id,
        env,
        TaskRun.state == RunState.STARTED.value,
        TaskRun.started_at.is_not(None),
        TaskRun.started_at <= filters.until - STUCK_RUNNING_AFTER,
        TaskRun.last_event_at >= filters.since,
    )

    diagnosis = Diagnosis(
        since=filters.since,
        until=filters.until,
        baseline_since=baseline_filters.since,  # type: ignore[arg-type]
        environment=env,
        total_runs=current.total,
        failed_runs=current.by_state.get(RunState.FAILED.value, 0),
        failure_rate=current.failure_rate,
        baseline_total_runs=baseline.total,
        baseline_failure_rate=baseline.failure_rate,
        error_clusters=clusters,
        task_findings=findings,
        stuck_queued=stuck_queued,
        stuck_running=stuck_running,
        workers_with_failures=dict(sorted(workers.items(), key=lambda kv: -kv[1])),
        headline="",
    )
    diagnosis.headline = _headline(diagnosis)
    return diagnosis


def _ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0f} ms"


def render_report(d: Diagnosis, project_name: str) -> str:
    """Plain-text report: the fallback summary and the model's only input."""
    lines = [
        f"QueueLoom diagnosis for project {project_name}"
        + (f", environment {d.environment}" if d.environment else ""),
        f"Window: {d.since.isoformat()} to {d.until.isoformat()} "
        f"(baseline: previous window starting {d.baseline_since.isoformat()})",
        "",
        d.headline,
        "",
    ]
    if d.task_findings:
        lines.append("Tasks that changed versus the baseline window:")
        for f in d.task_findings:
            lines.append(
                f"- {f.task_name}: {f.failed}/{f.total} failed ({_pct(f.failure_rate)}, "
                f"baseline {_pct(f.baseline_failure_rate)}); p95 duration {_ms(f.p95_duration_ms)} "
                f"(baseline {_ms(f.baseline_p95_duration_ms)}); p95 queue latency "
                f"{_ms(f.p95_queue_latency_ms)} (baseline {_ms(f.baseline_p95_queue_latency_ms)}); "
                f"retrying {f.retrying}; flags: {', '.join(f.flags)}"
            )
        lines.append("")
    if d.error_clusters:
        lines.append("Error clusters (most frequent first):")
        for c in d.error_clusters:
            lines.append(
                f"- {c.count}x {c.task_name} -> {c.exception_type}: "
                f"{c.message_pattern or '(no message)'}"
                + (f" [workers: {', '.join(c.workers)}]" if c.workers else "")
            )
            if c.sample_message and c.sample_message != c.message_pattern:
                lines.append(f"  example: {c.sample_message[:200]}")
        lines.append("")
        top = d.error_clusters[0]
        if top.sample_traceback:
            lines.append(f"Sample traceback for the top cluster ({top.task_name}):")
            lines.append(top.sample_traceback.strip())
            lines.append("")
    if d.stuck_queued or d.stuck_running:
        lines.append(f"Stuck: {d.stuck_queued} queued > 5 min, {d.stuck_running} running > 30 min.")
        lines.append("")
    if not (d.task_findings or d.error_clusters or d.stuck_queued or d.stuck_running):
        lines.append("Nothing unusual: no failures, regressions or stuck tasks in this window.")
    return "\n".join(lines).rstrip() + "\n"
