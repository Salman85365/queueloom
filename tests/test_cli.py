from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from queueloom.cli import app

runner = CliRunner()


@pytest.fixture
def db_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite:///{tmp_path / 'cli.db'}"
    monkeypatch.setenv("QUEUELOOM_DATABASE_URL", url)
    return url


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "queueloom" in result.output


def test_project_lifecycle(db_env: str) -> None:
    assert runner.invoke(app, ["init-db"]).exit_code == 0

    created = runner.invoke(app, ["project", "create", "billing"])
    assert created.exit_code == 0, created.output
    assert "API key: ql_" in created.output

    fixed = runner.invoke(app, ["project", "create", "demo", "--api-key", "ql_fixed"])
    assert fixed.exit_code == 0 and "ql_fixed" in fixed.output

    duplicate = runner.invoke(app, ["project", "create", "billing"])
    assert duplicate.exit_code == 1

    idempotent = runner.invoke(app, ["project", "create", "billing", "--if-not-exists"])
    assert idempotent.exit_code == 0

    listed = runner.invoke(app, ["project", "list"])
    assert "billing" in listed.output and "demo" in listed.output
