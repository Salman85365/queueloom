"""``queueloom`` command line: run the server and manage projects."""

from __future__ import annotations

from typing import Annotated

import typer

from queueloom import __version__

app = typer.Typer(
    help="QueueLoom — observability for Python background jobs.", no_args_is_help=True
)
project_app = typer.Typer(help="Manage projects and API keys.", no_args_is_help=True)
app.add_typer(project_app, name="project")


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


@app.command("init-db")
def init_db_command() -> None:
    """Create database tables (idempotent)."""
    from queueloom.server.config import Settings
    from queueloom.server.db import init_db, make_engine

    settings = Settings()
    init_db(make_engine(settings.database_url))
    typer.echo(f"Schema ready at {settings.database_url}")


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
    from queueloom.server.db import init_db, make_engine, make_session_factory, session_scope
    from queueloom.server.projects import create_project, get_project_by_name

    settings = Settings()
    engine = make_engine(settings.database_url)
    init_db(engine)
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
    from queueloom.server.db import init_db, make_engine, make_session_factory, session_scope
    from queueloom.server.projects import list_projects

    settings = Settings()
    engine = make_engine(settings.database_url)
    init_db(engine)
    with session_scope(make_session_factory(engine)) as session:
        projects = list_projects(session)
        if not projects:
            typer.echo("No projects yet. Create one with: queueloom project create <name>")
            return
        for project in projects:
            typer.echo(f"{project.id}\t{project.name}\t{project.created_at.isoformat()}")


if __name__ == "__main__":
    app()
