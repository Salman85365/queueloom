from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import Engine

from queueloom import __version__
from queueloom.server.alerts import run_evaluation
from queueloom.server.api import router as api_router
from queueloom.server.config import Settings
from queueloom.server.dashboard import router as dashboard_router
from queueloom.server.db import init_db, make_engine, make_session_factory

log = logging.getLogger("queueloom.server")


def create_app(settings: Settings | None = None, *, engine: Engine | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = engine or make_engine(settings.database_url)

    async def alert_loop(app: FastAPI) -> None:
        interval = settings.alert_eval_interval_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(
                    run_evaluation,
                    app.state.session_factory,
                    base_url=settings.public_base_url,
                    timeout=settings.webhook_timeout_seconds,
                )
            except Exception:
                log.exception("alert evaluation failed")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if settings.auto_create_schema:
            init_db(engine)
        log.info("QueueLoom %s ready (db=%s)", __version__, engine.url.render_as_string())
        task = None
        if settings.alert_eval_interval_seconds > 0:
            task = asyncio.create_task(alert_loop(app), name="queueloom-alerts")
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
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
