from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from queueloom.server.models import Project
from queueloom.server.projects import get_project_by_api_key


def get_session(request: Request) -> Iterator[Session]:
    session: Session = request.app.state.session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


SessionDep = Annotated[Session, Depends(get_session)]


def get_current_project(
    session: SessionDep,
    authorization: Annotated[str | None, Header()] = None,
) -> Project:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing API key; send 'Authorization: Bearer <key>'",
            headers={"WWW-Authenticate": "Bearer"},
        )
    api_key = authorization[7:].strip()
    project = get_project_by_api_key(session, api_key)
    if project is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key")
    return project


ProjectDep = Annotated[Project, Depends(get_current_project)]
