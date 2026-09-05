"""Programmatic Alembic entry points.

The migration scripts live inside the package so installed copies can migrate their own
database: ``queueloom migrate`` (or ``auto_migrate`` on server startup) brings any database to
the current head. Databases created by early pre-alpha builds with ``create_all`` and no
``alembic_version`` table are stamped at the initial revision first.
"""

from __future__ import annotations

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, inspect

log = logging.getLogger("queueloom.server.migrate")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
INITIAL_REVISION = "0001"


def alembic_config(engine: Engine) -> Config:
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False))
    config.attributes["connection_engine"] = engine
    return config


def current_revision(engine: Engine) -> str | None:
    with engine.connect() as connection:
        return MigrationContext.configure(connection).get_current_revision()


def head_revision(engine: Engine) -> str | None:
    return ScriptDirectory.from_config(alembic_config(engine)).get_current_head()


def _is_legacy_schema(engine: Engine) -> bool:
    """True for databases created with ``Base.metadata.create_all`` before migrations existed."""
    names = set(inspect(engine).get_table_names())
    return "alembic_version" not in names and {"projects", "task_runs", "task_events"} <= names


def upgrade(engine: Engine, revision: str = "head") -> None:
    config = alembic_config(engine)
    if _is_legacy_schema(engine):
        log.warning("stamping pre-migration database at revision %s", INITIAL_REVISION)
        command.stamp(config, INITIAL_REVISION)
    command.upgrade(config, revision)


def downgrade(engine: Engine, revision: str) -> None:
    command.downgrade(alembic_config(engine), revision)


def make_revision(engine: Engine, message: str, *, autogenerate: bool = True) -> None:
    command.revision(alembic_config(engine), message=message, autogenerate=autogenerate)
