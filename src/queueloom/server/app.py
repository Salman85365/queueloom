from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response
from sqlalchemy import Engine
from starlette.middleware.sessions import SessionMiddleware

from queueloom import __version__
from queueloom.server.alerts import run_evaluation
from queueloom.server.api import router as api_router
from queueloom.server.auth import LoginRequired, login_redirect
from queueloom.server.auth import router as auth_router
from queueloom.server.config import Settings
from queueloom.server.dashboard import router as dashboard_router
from queueloom.server.db import make_engine, make_session_factory
from queueloom.server.migrate import upgrade
from queueloom.server.retention import cleanup

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

    async def retention_loop(app: FastAPI) -> None:
        interval = settings.retention_interval_seconds
        while True:
            try:
                await asyncio.to_thread(
                    cleanup, app.state.session_factory, retention_days=settings.retention_days
                )
            except Exception:
                log.exception("retention cleanup failed")
            await asyncio.sleep(interval)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if settings.auto_migrate:
            upgrade(engine)
        log.info("QueueLoom %s ready (db=%s)", __version__, engine.url.render_as_string())
        tasks: list[asyncio.Task[None]] = []
        if settings.alert_eval_interval_seconds > 0:
            tasks.append(asyncio.create_task(alert_loop(app), name="queueloom-alerts"))
        if settings.retention_interval_seconds > 0 and settings.retention_days > 0:
            tasks.append(asyncio.create_task(retention_loop(app), name="queueloom-retention"))
        try:
            yield
        finally:
            for task in tasks:
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

    secret_key = settings.secret_key
    if not secret_key:
        secret_key = secrets.token_urlsafe(48)
        if settings.dashboard_password:
            log.warning("QUEUELOOM_SECRET_KEY is not set; dashboard sessions reset on restart")
    app.add_middleware(
        SessionMiddleware,
        secret_key=secret_key,
        session_cookie="queueloom_session",
        max_age=settings.session_max_age_seconds,
        same_site="lax",
        https_only=settings.https_only,
    )

    @app.exception_handler(LoginRequired)
    async def _login_required(request: Request, exc: LoginRequired) -> Response:
        return login_redirect(exc.next_path)

    app.include_router(api_router)
    app.include_router(auth_router)
    app.include_router(dashboard_router)
    return app
