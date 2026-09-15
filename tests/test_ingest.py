from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from queueloom.events import EventType
from queueloom.server.db import session_scope
from queueloom.server.ingest import apply_event, ingest_events
from queueloom.server.models import RunState, TaskEventRow, TaskRun
from queueloom.server.projects import get_project_by_name
from tests.conftest import ProjectInfo, at, lifecycle, make_event


def _ingest(
    session_factory: sessionmaker[Session], project: ProjectInfo, events: list
) -> tuple[int, int]:
    with session_scope(session_factory) as session:
        p = get_project_by_name(session, project.name)
        assert p is not None
        result = ingest_events(session, p, events)
        return result.accepted, result.duplicates


def _run(session_factory: sessionmaker[Session], project: ProjectInfo, task_id: str) -> TaskRun:
    with session_scope(session_factory) as session:
        run = session.scalar(
            select(TaskRun).where(TaskRun.project_id == project.id, TaskRun.task_id == task_id)
        )
        assert run is not None
        return run


def test_successful_lifecycle_materialises_run(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    accepted, duplicates = _ingest(session_factory, project, lifecycle("t1"))
    assert (accepted, duplicates) == (3, 0)
    run = _run(session_factory, project, "t1")
    assert run.state == RunState.SUCCEEDED.value
    assert run.task_name == "app.tasks.add"
    assert run.queue == "celery"
    assert run.worker == "w1"
    assert run.attempts == 1
    assert run.queue_latency_ms == 250.0
    assert run.duration_ms == 1000.0
    assert run.published_at == at(0) and run.started_at == at(0.25) and run.finished_at == at(1.25)


def test_failed_lifecycle_records_exception(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    _ingest(session_factory, project, lifecycle("t2", outcome=EventType.FAILED))
    run = _run(session_factory, project, "t2")
    assert run.state == RunState.FAILED.value
    assert run.exception_type == "ValueError"
    assert run.exception_message == "boom"
    assert run.traceback == "Traceback..."


def test_retry_cycle(session_factory: sessionmaker[Session], project: ProjectInfo) -> None:
    common = {"task_id": "t3", "task_name": "app.tasks.flaky"}
    events = [
        make_event(EventType.PUBLISHED, timestamp=at(0), published_at=at(0), **common),
        make_event(EventType.STARTED, timestamp=at(1), **common),
        make_event(
            EventType.RETRIED,
            timestamp=at(2),
            retries=1,
            exception={"type": "TimeoutError", "message": "slow"},
            **common,
        ),
        make_event(EventType.PUBLISHED, timestamp=at(3), published_at=at(3), retries=1, **common),
        make_event(EventType.STARTED, timestamp=at(4), retries=1, **common),
        make_event(EventType.SUCCEEDED, timestamp=at(5), runtime_ms=900.0, retries=1, **common),
    ]
    # Ingest in two batches to exercise run reuse across requests.
    _ingest(session_factory, project, events[:3])
    run = _run(session_factory, project, "t3")
    assert run.state == RunState.RETRYING.value
    assert run.exception_type == "TimeoutError"
    _ingest(session_factory, project, events[3:])
    run = _run(session_factory, project, "t3")
    assert run.state == RunState.SUCCEEDED.value
    assert run.retries == 1
    assert run.attempts == 2
    assert run.queue_latency_ms == 1000.0  # latest attempt: published at 3s, started at 4s
    assert run.duration_ms == 900.0


@pytest.mark.parametrize("publish_first", [True, False])
def test_retry_waits_for_its_own_start_before_reporting_latency(
    session_factory: sessionmaker[Session], project: ProjectInfo, publish_first: bool
) -> None:
    common = {"task_id": "retry-latency"}
    _ingest(
        session_factory,
        project,
        [
            make_event(
                EventType.PUBLISHED, timestamp=at(0), published_at=at(0), retries=0, **common
            ),
            make_event(EventType.STARTED, timestamp=at(1), retries=0, **common),
        ],
    )
    assert _run(session_factory, project, "retry-latency").queue_latency_ms == 1000

    published = make_event(
        EventType.PUBLISHED, timestamp=at(2), published_at=at(2), retries=1, **common
    )
    retried = make_event(EventType.RETRIED, timestamp=at(2.1), retries=1, **common)
    for event in [published, retried] if publish_first else [retried, published]:
        _ingest(session_factory, project, [event])
        run = _run(session_factory, project, "retry-latency")
        assert run.state in {RunState.QUEUED.value, RunState.RETRYING.value}
        assert run.started_at is None
        assert run.queue_latency_ms is None  # neither the old latency nor a false zero

    _ingest(
        session_factory,
        project,
        [make_event(EventType.STARTED, timestamp=at(1), published_at=at(0), retries=0, **common)],
    )
    _ingest(
        session_factory,
        project,
        [make_event(EventType.SUCCEEDED, timestamp=at(4), runtime_ms=750, retries=1, **common)],
    )
    run = _run(session_factory, project, "retry-latency")
    assert run.state == RunState.SUCCEEDED.value
    assert run.published_at == at(2) and run.started_at is None
    assert run.queue_latency_ms is None  # the current STARTED event has not arrived
    assert run.duration_ms == 750

    _ingest(
        session_factory,
        project,
        [make_event(EventType.STARTED, timestamp=at(3), retries=1, **common)],
    )
    run = _run(session_factory, project, "retry-latency")
    assert run.state == RunState.SUCCEEDED.value and run.started_at == at(3)
    assert run.queue_latency_ms == 1000 and run.duration_ms == 750


def test_out_of_order_events_do_not_regress_state(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    events = lifecycle("t4")
    _ingest(session_factory, project, [events[2]])  # succeeded arrives first
    _ingest(session_factory, project, [events[0], events[1]])
    run = _run(session_factory, project, "t4")
    assert run.state == RunState.SUCCEEDED.value
    assert run.published_at == at(0)
    assert run.started_at == at(0.25)
    assert run.last_event_at == at(1.25)


def test_delayed_previous_attempt_preserves_latest_run_metadata(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    common = {"task_id": "late-retry", "retries": 1}
    latest = [
        make_event(EventType.PUBLISHED, timestamp=at(3), published_at=at(3), **common),
        make_event(EventType.STARTED, timestamp=at(4), worker="new-worker", **common),
        make_event(
            EventType.SUCCEEDED, timestamp=at(5), worker="new-worker", runtime_ms=900, **common
        ),
    ]
    _ingest(session_factory, project, latest)
    _ingest(
        session_factory,
        project,
        [
            make_event(
                EventType.STARTED,
                task_id="late-retry",
                timestamp=at(1),
                published_at=at(0),
                worker="old-worker",
                retries=0,
            ),
            make_event(
                EventType.FAILED,
                task_id="late-retry",
                timestamp=at(2),
                worker="old-worker",
                retries=0,
                runtime_ms=250,
                exception={"type": "OldError", "message": "previous attempt"},
            ),
        ],
    )
    run = _run(session_factory, project, "late-retry")
    assert run.state == RunState.SUCCEEDED.value
    assert run.worker == "new-worker"
    assert run.published_at == at(3)
    assert run.started_at == at(4) and run.finished_at == at(5)
    assert run.queue_latency_ms == 1000
    assert run.duration_ms == 900
    assert run.exception_type is None
    assert run.retries == 1 and run.last_event_at == at(5)
    with session_scope(session_factory) as session:
        rows = session.scalars(select(TaskEventRow).where(TaskEventRow.task_id == "late-retry"))
        assert len(list(rows)) == 5  # delayed events remain available in the raw timeline


def test_terminal_first_backfills_only_matching_attempt_metadata(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    common = {"task_id": "terminal-first", "retries": 1}
    _ingest(
        session_factory,
        project,
        [make_event(EventType.SUCCEEDED, timestamp=at(5), **common)],
    )
    _ingest(
        session_factory,
        project,
        [
            make_event(
                EventType.STARTED,
                task_id="terminal-first",
                timestamp=at(1),
                published_at=at(0),
                worker="old-worker",
                retries=0,
            )
        ],
    )
    run = _run(session_factory, project, "terminal-first")
    assert run.started_at is None and run.published_at is None
    assert run.worker is None and run.queue_latency_ms is None

    _ingest(
        session_factory,
        project,
        [make_event(EventType.STARTED, timestamp=at(4), worker="new-worker", **common)],
    )
    _ingest(
        session_factory,
        project,
        [make_event(EventType.PUBLISHED, timestamp=at(3), published_at=at(3), **common)],
    )
    run = _run(session_factory, project, "terminal-first")
    assert run.state == RunState.SUCCEEDED.value
    assert run.worker == "new-worker"
    assert run.published_at == at(3)
    assert run.started_at == at(4) and run.finished_at == at(5)
    assert run.queue_latency_ms == 1000
    assert run.duration_ms == 1000  # timestamps fill in when no runtime was supplied
    assert run.attempts == 1 and run.last_event_at == at(5)


def test_delayed_retry_exception_does_not_replace_final_failure(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    common = {"task_id": "retry-exception", "retries": 1}
    _ingest(
        session_factory,
        project,
        [
            make_event(
                EventType.FAILED,
                timestamp=at(5),
                worker="new-worker",
                runtime_ms=900,
                exception={"type": "FinalError", "message": "final attempt", "traceback": "new"},
                **common,
            )
        ],
    )
    _ingest(
        session_factory,
        project,
        [
            make_event(
                EventType.RETRIED,
                timestamp=at(2),
                worker="old-worker",
                exception={"type": "OldError", "message": "previous attempt", "traceback": "old"},
                **common,
            )
        ],
    )
    run = _run(session_factory, project, "retry-exception")
    assert run.state == RunState.FAILED.value and run.worker == "new-worker"
    assert run.finished_at == at(5) and run.duration_ms == 900
    assert run.exception_type == "FinalError"
    assert run.exception_message == "final attempt" and run.traceback == "new"


def test_started_event_carries_published_at_from_header(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    # The instrumented producer stamped the header, but only worker-side telemetry arrived.
    events = [
        make_event(EventType.STARTED, task_id="t5", timestamp=at(2), published_at=at(0.5)),
        make_event(EventType.SUCCEEDED, task_id="t5", timestamp=at(3), runtime_ms=1000.0),
    ]
    _ingest(session_factory, project, events)
    run = _run(session_factory, project, "t5")
    assert run.queue_latency_ms == 1500.0
    assert run.attempts == 0  # no publish event seen


def test_duplicate_events_are_ignored(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    events = lifecycle("t6")
    assert _ingest(session_factory, project, events) == (3, 0)
    assert _ingest(session_factory, project, events) == (0, 3)
    assert _ingest(session_factory, project, [events[0], events[0]]) == (0, 2)
    with session_scope(session_factory) as session:
        rows = session.scalars(select(TaskEventRow).where(TaskEventRow.task_id == "t6")).all()
        assert len(rows) == 3
        assert all(r.payload["schema_version"] == 1 for r in rows)


def test_duration_falls_back_to_timestamps() -> None:
    run = TaskRun(
        project_id=1,
        task_id="x",
        environment="default",
        state="queued",
        last_event_at=at(0),
        retries=0,
        attempts=0,
    )
    apply_event(run, make_event(EventType.STARTED, task_id="x", timestamp=at(1)))
    apply_event(run, make_event(EventType.FAILED, task_id="x", timestamp=at(2.5)))
    assert run.duration_ms == 1500.0
    assert run.state == RunState.FAILED.value


def test_revoked(session_factory: sessionmaker[Session], project: ProjectInfo) -> None:
    events = [
        make_event(EventType.PUBLISHED, task_id="t7", timestamp=at(0)),
        make_event(
            EventType.REVOKED,
            task_id="t7",
            timestamp=at(1),
            exception={"type": "Revoked", "message": "terminated"},
        ),
    ]
    _ingest(session_factory, project, events)
    run = _run(session_factory, project, "t7")
    assert run.state == RunState.REVOKED.value
    assert run.exception_message == "terminated"
