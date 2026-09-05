from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from queueloom.cli import app
from queueloom.events import EventType
from queueloom.server.db import session_scope
from queueloom.server.ingest import ingest_events
from queueloom.server.models import AlertEvent, AlertRule, TaskEventRow, TaskRun
from queueloom.server.projects import get_project_by_name
from queueloom.server.retention import cleanup
from tests.conftest import T0, ProjectInfo, at, lifecycle


def _counts(session: Session) -> tuple[int, int, int]:
    return (
        session.scalar(select(func.count()).select_from(TaskEventRow)) or 0,
        session.scalar(select(func.count()).select_from(TaskRun)) or 0,
        session.scalar(select(func.count()).select_from(AlertEvent)) or 0,
    )


def test_cleanup_removes_only_old_data(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    old = lifecycle("old", offset=-40 * 86400)
    recent = lifecycle("recent", offset=0, outcome=EventType.FAILED)
    with session_scope(session_factory) as session:
        p = get_project_by_name(session, project.name)
        assert p is not None
        ingest_events(session, p, old + recent)
        rule = AlertRule(project_id=p.id, name="r", webhook_url="https://h.example")
        session.add(rule)
        session.flush()
        session.add_all(
            [
                AlertEvent(
                    rule_id=rule.id, project_id=p.id, kind="fired", created_at=at(-45 * 86400)
                ),
                AlertEvent(rule_id=rule.id, project_id=p.id, kind="resolved", created_at=at(0)),
            ]
        )
    with session_scope(session_factory) as session:
        assert _counts(session) == (6, 2, 2)

    result = cleanup(session_factory, retention_days=30, now=T0 + timedelta(days=1), batch_size=2)
    assert (result.task_events, result.task_runs, result.alert_events) == (3, 1, 1)
    assert result.total == 5
    with session_scope(session_factory) as session:
        assert _counts(session) == (3, 1, 1)
        assert session.scalar(select(TaskRun.task_id)) == "recent"

    # Idempotent.
    again = cleanup(session_factory, retention_days=30, now=T0 + timedelta(days=1))
    assert again.total == 0


def test_cleanup_rejects_zero_retention(session_factory: sessionmaker[Session]) -> None:
    with pytest.raises(ValueError):
        cleanup(session_factory, retention_days=0)


def test_cleanup_cli(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("QUEUELOOM_DATABASE_URL", f"sqlite:///{tmp_path / 'r.db'}")
    runner = CliRunner()
    assert runner.invoke(app, ["migrate"]).exit_code == 0
    dry = runner.invoke(app, ["cleanup", "--dry-run", "--days", "7"])
    assert dry.exit_code == 0 and "Would delete" in dry.output
    real = runner.invoke(app, ["cleanup", "--days", "7"])
    assert real.exit_code == 0 and "Deleted 0 events, 0 runs, 0 alert events" in real.output
    disabled = runner.invoke(app, ["cleanup", "--days", "0"])
    assert disabled.exit_code == 0 and "disabled" in disabled.output
