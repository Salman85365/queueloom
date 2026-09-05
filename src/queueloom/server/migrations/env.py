from __future__ import annotations

from typing import Any

from alembic import context
from sqlalchemy import DateTime, Engine, create_engine
from sqlalchemy.dialects import postgresql

from queueloom.server.models import Base, UTCDateTime

config = context.config
target_metadata = Base.metadata


def render_item(type_: str, obj: Any, autogen_context: Any) -> Any:
    """Render our custom types as plain SQLAlchemy so migrations do not import models."""
    if type_ == "type":
        if isinstance(obj, UTCDateTime):
            return "sa.DateTime(timezone=True)"
        if isinstance(obj, DateTime):
            return f"sa.DateTime(timezone={obj.timezone})"
        if getattr(obj, "_variant_mapping", None) and "postgresql" in obj._variant_mapping:
            variant = obj._variant_mapping["postgresql"]
            if isinstance(variant, postgresql.JSONB):
                autogen_context.imports.add("from sqlalchemy.dialects import postgresql")
                return 'sa.JSON().with_variant(postgresql.JSONB(), "postgresql")'
    return False


def _engine() -> Engine:
    engine = config.attributes.get("connection_engine")
    if isinstance(engine, Engine):
        return engine
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("sqlalchemy.url is not configured")
    return create_engine(url)


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        render_item=render_item,
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = _engine()
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_item,
            compare_type=True,
            # SQLite cannot ALTER most things; batch mode rebuilds tables instead.
            render_as_batch=connection.dialect.name == "sqlite",
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
