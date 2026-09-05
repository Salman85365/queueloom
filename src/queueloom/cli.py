"""``queueloom`` command line: run the server and manage projects."""

from __future__ import annotations

from typing import Annotated, Any

import typer

from queueloom import __version__

app = typer.Typer(
    help="QueueLoom — observability for Python background jobs.", no_args_is_help=True
)
project_app = typer.Typer(help="Manage projects and API keys.", no_args_is_help=True)
app.add_typer(project_app, name="project")
alert_app = typer.Typer(help="Manage failure-rate alert rules.", no_args_is_help=True)
app.add_typer(alert_app, name="alert")


def _version(value: bool) -> None:
    if value:
        typer.echo(f"queueloom {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool, typer.Option("--version", callback=_version, is_eager=True, help="Show version.")
    ] = False,
) -> None:
    pass


@app.command()
def serve(
    host: Annotated[str | None, typer.Option(help="Bind address.")] = None,
    port: Annotated[int | None, typer.Option(help="Port.")] = None,
    reload: Annotated[bool, typer.Option(help="Auto-reload (development only).")] = False,
) -> None:
    """Run the ingestion API and dashboard."""
    import uvicorn

    from queueloom.server.config import Settings

    settings = Settings()
    uvicorn.run(
        "queueloom.server.app:create_app",
        factory=True,
        host=host or settings.host,
        port=port or settings.port,
        reload=reload,
        log_level=settings.log_level,
    )


@app.command("migrate")
def migrate_command(
    revision: Annotated[str, typer.Argument(help="Target revision (default: head).")] = "head",
) -> None:
    """Apply database migrations (idempotent)."""
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine
    from queueloom.server.migrate import current_revision, upgrade

    settings = Settings()
    engine = make_engine(settings.database_url)
    upgrade(engine, revision)
    typer.echo(f"Database at revision {current_revision(engine)} ({settings.database_url})")


@app.command()
def cleanup(
    days: Annotated[
        int | None,
        typer.Option(help="Retention window in days (default: QUEUELOOM_RETENTION_DAYS)."),
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Only report the cutoff.")] = False,
) -> None:
    """Delete runs, events and alert history older than the retention window."""
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory
    from queueloom.server.retention import cleanup as run_cleanup

    settings = Settings()
    retention_days = days if days is not None else settings.retention_days
    if retention_days <= 0:
        typer.echo("Retention is disabled (QUEUELOOM_RETENTION_DAYS=0); nothing to do.")
        return
    if dry_run:
        from datetime import UTC, datetime, timedelta

        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        typer.echo(f"Would delete data with last activity before {cutoff.isoformat()}")
        return
    engine = make_engine(settings.database_url)
    result = run_cleanup(make_session_factory(engine), retention_days=retention_days)
    typer.echo(
        f"Deleted {result.task_events} events, {result.task_runs} runs, "
        f"{result.alert_events} alert events older than {result.cutoff.isoformat()}"
    )


@app.command("init-db", hidden=True)
def init_db_command() -> None:
    """Alias for `migrate` kept for early adopters."""
    migrate_command("head")


@app.command("makemigration", hidden=True)
def makemigration_command(
    message: Annotated[str, typer.Option("-m", "--message", help="Revision message.")],
) -> None:
    """Autogenerate a migration from model changes (development only)."""
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine
    from queueloom.server.migrate import make_revision

    make_revision(make_engine(Settings().database_url), message)


@project_app.command("create")
def project_create(
    name: Annotated[str, typer.Argument(help="Project name, e.g. 'billing-prod'.")],
    api_key: Annotated[
        str | None, typer.Option(help="Use this key instead of generating one.")
    ] = None,
    if_not_exists: Annotated[
        bool, typer.Option("--if-not-exists", help="Do nothing if the project already exists.")
    ] = False,
) -> None:
    """Create a project and print its API key (shown only once)."""
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory, session_scope
    from queueloom.server.migrate import upgrade
    from queueloom.server.projects import create_project, get_project_by_name

    settings = Settings()
    engine = make_engine(settings.database_url)
    upgrade(engine)
    with session_scope(make_session_factory(engine)) as session:
        existing = get_project_by_name(session, name)
        if existing is not None:
            if if_not_exists:
                typer.echo(f"Project '{name}' already exists.")
                return
            typer.echo(f"Project '{name}' already exists.", err=True)
            raise typer.Exit(code=1)
        _, key = create_project(session, name, api_key=api_key)
    typer.echo(f"Project: {name}")
    typer.echo(f"API key: {key}")
    typer.echo("Store this key now; it is not shown again.")


@project_app.command("list")
def project_list() -> None:
    """List projects."""
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory, session_scope
    from queueloom.server.migrate import upgrade
    from queueloom.server.projects import list_projects

    settings = Settings()
    engine = make_engine(settings.database_url)
    upgrade(engine)
    with session_scope(make_session_factory(engine)) as session:
        projects = list_projects(session)
        if not projects:
            typer.echo("No projects yet. Create one with: queueloom project create <name>")
            return
        for project in projects:
            typer.echo(f"{project.id}\t{project.name}\t{project.created_at.isoformat()}")


def _project_or_exit(session: Any, name: str) -> Any:
    from queueloom.server.projects import get_project_by_name

    project = get_project_by_name(session, name)
    if project is None:
        typer.echo(f"Project '{name}' not found.", err=True)
        raise typer.Exit(code=1)
    return project


@alert_app.command("create")
def alert_create(
    project: Annotated[str, typer.Argument(help="Project name.")],
    webhook_url: Annotated[str, typer.Option(help="URL to POST alert payloads to.")],
    name: Annotated[str | None, typer.Option(help="Rule name (default: derived).")] = None,
    threshold: Annotated[float, typer.Option(help="Failure rate 0..1 that fires.")] = 0.1,
    window: Annotated[int, typer.Option(help="Sliding window in minutes.")] = 15,
    min_runs: Annotated[int, typer.Option(help="Minimum finished runs to evaluate.")] = 10,
    cooldown: Annotated[int, typer.Option(help="Minutes before re-firing after resolve.")] = 30,
    environment: Annotated[str | None, typer.Option(help="Limit to an environment.")] = None,
    task_name: Annotated[str | None, typer.Option(help="Limit to a task name.")] = None,
    fmt: Annotated[str, typer.Option("--format", help="json or slack")] = "json",
    secret: Annotated[
        str | None, typer.Option(help="HMAC secret for X-QueueLoom-Signature.")
    ] = None,
) -> None:
    """Create an alert rule."""
    from queueloom.server.alerts import WEBHOOK_FORMATS
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory, session_scope
    from queueloom.server.migrate import upgrade
    from queueloom.server.models import AlertRule

    if fmt not in WEBHOOK_FORMATS:
        typer.echo(f"--format must be one of {WEBHOOK_FORMATS}", err=True)
        raise typer.Exit(code=2)
    if not webhook_url.startswith(("http://", "https://")):
        typer.echo("--webhook-url must start with http:// or https://", err=True)
        raise typer.Exit(code=2)
    engine = make_engine(Settings().database_url)
    upgrade(engine)
    with session_scope(make_session_factory(engine)) as session:
        proj = _project_or_exit(session, project)
        rule = AlertRule(
            project_id=proj.id,
            name=name or f"failure-rate>{threshold:g} {task_name or 'all'}",
            environment=environment,
            task_name=task_name,
            window_minutes=window,
            threshold=threshold,
            min_runs=min_runs,
            cooldown_minutes=cooldown,
            webhook_url=webhook_url,
            webhook_format=fmt,
            webhook_secret=secret,
        )
        session.add(rule)
        session.flush()
        typer.echo(f"Created alert rule #{rule.id} '{rule.name}' for project {project}")


@alert_app.command("list")
def alert_list(project: Annotated[str, typer.Argument(help="Project name.")]) -> None:
    """List alert rules for a project."""
    from sqlalchemy import select

    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory, session_scope
    from queueloom.server.migrate import upgrade
    from queueloom.server.models import AlertRule

    engine = make_engine(Settings().database_url)
    upgrade(engine)
    with session_scope(make_session_factory(engine)) as session:
        proj = _project_or_exit(session, project)
        rules = list(
            session.scalars(
                select(AlertRule).where(AlertRule.project_id == proj.id).order_by(AlertRule.id)
            )
        )
        if not rules:
            typer.echo("No alert rules.")
            return
        for r in rules:
            scope = f"{r.environment or '*'}/{r.task_name or '*'}"
            typer.echo(
                f"#{r.id}\t{r.name}\t{r.state}\t{scope}\t>={r.threshold:g} over {r.window_minutes}m"
                f"\t{r.webhook_format} {r.webhook_url}"
            )


@alert_app.command("delete")
def alert_delete(
    project: Annotated[str, typer.Argument(help="Project name.")],
    rule_id: Annotated[int, typer.Argument(help="Rule id.")],
) -> None:
    """Delete an alert rule."""
    from sqlalchemy import select

    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory, session_scope
    from queueloom.server.models import AlertRule

    engine = make_engine(Settings().database_url)
    with session_scope(make_session_factory(engine)) as session:
        proj = _project_or_exit(session, project)
        rule = session.scalar(
            select(AlertRule).where(AlertRule.id == rule_id, AlertRule.project_id == proj.id)
        )
        if rule is None:
            typer.echo(f"Rule #{rule_id} not found in project {project}.", err=True)
            raise typer.Exit(code=1)
        session.delete(rule)
    typer.echo(f"Deleted alert rule #{rule_id}")


@alert_app.command("evaluate")
def alert_evaluate() -> None:
    """Evaluate all enabled rules once and deliver webhooks (cron-friendly)."""
    from queueloom.server.alerts import run_evaluation
    from queueloom.server.config import Settings
    from queueloom.server.db import make_engine, make_session_factory

    settings = Settings()
    engine = make_engine(settings.database_url)
    ids = run_evaluation(
        make_session_factory(engine),
        base_url=settings.public_base_url,
        timeout=settings.webhook_timeout_seconds,
    )
    typer.echo(f"{len(ids)} alert event(s) emitted")


if __name__ == "__main__":
    app()
