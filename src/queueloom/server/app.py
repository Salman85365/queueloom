from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import Engine

from queueloom import __version__
from queueloom.server.api import router as api_router
from queueloom.server.config import Settings
from queueloom.server.dashboard import router as dashboard_router
from queueloom.server.db import init_db, make_engine, make_session_factory

log = logging.getLogger("queueloom.server")


def create_app(settings: Settings | None = None, *, engine: Engine | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = engine or make_engine(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if settings.auto_create_schema:
            init_db(engine)
        log.info("QueueLoom %s ready (db=%s)", __version__, engine.url.render_as_string())
        yield
        engine.dispose()

    app = FastAPI(
        title="QueueLoom",
        version=__version__,
        description="Observability for Python background jobs.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = make_session_factory(engine)
    app.include_router(api_router)
    app.include_router(dashboard_router)
    return app
