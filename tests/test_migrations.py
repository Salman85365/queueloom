from __future__ import annotations

from pathlib import Path

from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import inspect

from queueloom.server.db import make_engine
from queueloom.server.migrate import (
    INITIAL_REVISION,
    current_revision,
    downgrade,
    head_revision,
    upgrade,
)
from queueloom.server.models import Base
from tests.conftest import TEST_DATABASE_URL


def _fresh_engine(tmp_path: Path):  # type: ignore[no-untyped-def]
    # SQLite in-memory databases cannot be shared across Alembic's connections reliably,
    # so use a file; on PostgreSQL reuse the configured test database.
    if TEST_DATABASE_URL.startswith("sqlite"):
        return make_engine(f"sqlite:///{tmp_path / 'migrations.db'}")
    engine = make_engine(TEST_DATABASE_URL)
    Base.metadata.drop_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE IF EXISTS alembic_version")
    return engine


def _schema_diff(engine) -> list:  # type: ignore[no-untyped-def]
    with engine.connect() as connection:
        context = MigrationContext.configure(
            connection,
            opts={"compare_type": True, "render_as_batch": engine.dialect.name == "sqlite"},
        )
        return compare_metadata(context, Base.metadata)


def test_head_migration_matches_models(tmp_path: Path) -> None:
    engine = _fresh_engine(tmp_path)
    try:
        upgrade(engine)
        assert current_revision(engine) == head_revision(engine)
        diff = _schema_diff(engine)
        assert diff == [], f"models and migrations disagree: {diff}"
        # Running again is a no-op.
        upgrade(engine)
        assert current_revision(engine) == head_revision(engine)
    finally:
        engine.dispose()


def test_downgrade_to_base_removes_tables(tmp_path: Path) -> None:
    engine = _fresh_engine(tmp_path)
    try:
        upgrade(engine)
        downgrade(engine, "base")
        names = set(inspect(engine).get_table_names()) - {"alembic_version"}
        assert names == set()
        assert current_revision(engine) is None
    finally:
        engine.dispose()


def test_legacy_create_all_database_is_stamped_then_upgraded(tmp_path: Path) -> None:
    engine = _fresh_engine(tmp_path)
    try:
        Base.metadata.create_all(engine)  # how pre-migration builds created the schema
        assert current_revision(engine) is None
        upgrade(engine)
        assert current_revision(engine) == head_revision(engine)
        assert head_revision(engine) >= INITIAL_REVISION
        assert _schema_diff(engine) == []
    finally:
        engine.dispose()
