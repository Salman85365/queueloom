from __future__ import annotations

import hashlib
import secrets

from sqlalchemy import select
from sqlalchemy.orm import Session

from queueloom.server.models import Project

API_KEY_PREFIX = "ql_"


def generate_api_key() -> str:
    return API_KEY_PREFIX + secrets.token_urlsafe(32)


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def create_project(
    session: Session, name: str, *, api_key: str | None = None
) -> tuple[Project, str]:
    """Create a project and return it together with its plaintext API key (shown once)."""
    api_key = api_key or generate_api_key()
    project = Project(name=name, api_key_hash=hash_api_key(api_key))
    session.add(project)
    session.flush()
    return project, api_key


def get_project_by_name(session: Session, name: str) -> Project | None:
    return session.scalar(select(Project).where(Project.name == name))


def get_project_by_api_key(session: Session, api_key: str) -> Project | None:
    return session.scalar(select(Project).where(Project.api_key_hash == hash_api_key(api_key)))


def list_projects(session: Session) -> list[Project]:
    return list(session.scalars(select(Project).order_by(Project.name)))
