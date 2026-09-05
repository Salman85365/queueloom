"""Dashboard authentication.

The MVP uses a single shared password (``QUEUELOOM_DASHBOARD_PASSWORD``) and a signed session
cookie. When no password is configured the dashboard is open, which is the right default for
``localhost`` and the wrong one for anything reachable from the internet; the UI says so.

The JSON API is unaffected: it authenticates with per-project API keys.
"""

from __future__ import annotations

import secrets
import time
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

SESSION_AUTHED = "authed"
SESSION_CSRF = "csrf"
LOGIN_PATH = "/login"

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
router = APIRouter(include_in_schema=False)


def auth_enabled(request: Request) -> bool:
    return bool(request.app.state.settings.dashboard_password)


def is_authenticated(request: Request) -> bool:
    return not auth_enabled(request) or bool(request.session.get(SESSION_AUTHED))


def csrf_token(request: Request) -> str:
    token = request.session.get(SESSION_CSRF)
    if not isinstance(token, str) or not token:
        token = secrets.token_urlsafe(32)
        request.session[SESSION_CSRF] = token
    return token


def _safe_next(value: str | None) -> str:
    """Only allow same-site relative redirects."""
    if not value:
        return "/"
    parts = urlsplit(value)
    if parts.scheme or parts.netloc or not value.startswith("/") or value.startswith("//"):
        return "/"
    return value


class LoginRequired(Exception):
    def __init__(self, next_path: str) -> None:
        self.next_path = next_path


def require_dashboard_user(request: Request) -> None:
    if not is_authenticated(request):
        raise LoginRequired(
            request.url.path + (f"?{request.url.query}" if request.url.query else "")
        )


def require_csrf(request: Request, csrf: Annotated[str, Form()] = "") -> None:
    expected = request.session.get(SESSION_CSRF)
    if not expected or not secrets.compare_digest(csrf, str(expected)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid CSRF token")


DashboardUser = Depends(require_dashboard_user)
CsrfProtected = Depends(require_csrf)


def login_redirect(next_path: str) -> RedirectResponse:
    from urllib.parse import urlencode

    return RedirectResponse(
        f"{LOGIN_PATH}?{urlencode({'next': next_path})}", status_code=status.HTTP_303_SEE_OTHER
    )


@router.get(LOGIN_PATH, response_class=HTMLResponse)
def login_form(request: Request, next: str = "/") -> Any:
    if not auth_enabled(request) or is_authenticated(request):
        return RedirectResponse(_safe_next(next), status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"next": _safe_next(next), "error": None, "csrf_token": csrf_token(request)},
    )


@router.post(LOGIN_PATH, response_class=HTMLResponse)
def login_submit(
    request: Request,
    password: Annotated[str, Form()] = "",
    next: Annotated[str, Form()] = "/",
    csrf: Annotated[str, Form()] = "",
) -> Any:
    if not auth_enabled(request):
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    expected_csrf = request.session.get(SESSION_CSRF)
    expected_password: str = request.app.state.settings.dashboard_password
    ok = (
        bool(expected_csrf)
        and secrets.compare_digest(csrf, str(expected_csrf))
        and secrets.compare_digest(password.encode(), expected_password.encode())
    )
    if not ok:
        time.sleep(0.5)  # blunt but effective brute-force damper for a single-password MVP
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "next": _safe_next(next),
                "error": "Wrong password.",
                "csrf_token": csrf_token(request),
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    request.session[SESSION_AUTHED] = True
    request.session[SESSION_CSRF] = secrets.token_urlsafe(32)  # rotate on privilege change
    return RedirectResponse(_safe_next(next), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/logout")
def logout(request: Request, csrf: Annotated[str, Form()] = "") -> Any:
    expected = request.session.get(SESSION_CSRF)
    if expected and secrets.compare_digest(csrf, str(expected)):
        request.session.clear()
    return RedirectResponse(LOGIN_PATH if auth_enabled(request) else "/", status_code=303)
