from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Server configuration. Every field can be set as ``QUEUELOOM_<FIELD>``."""

    model_config = SettingsConfigDict(env_prefix="QUEUELOOM_", extra="ignore")

    database_url: str = "sqlite:///./queueloom.db"
    host: str = "127.0.0.1"
    port: int = 8800
    log_level: str = "info"
    # Run pending Alembic migrations on startup. Disable if you manage schema separately
    # (`queueloom migrate`).
    auto_migrate: bool = True
    dashboard_refresh_seconds: int = 15
    # Upper bound on runs scanned for percentile statistics in one request.
    stats_sample_limit: int = 50_000
    # How often the server evaluates alert rules in the background. 0 disables the loop
    # (use `queueloom alert evaluate` from cron or POST /v1/alerts/evaluate instead).
    alert_eval_interval_seconds: int = 30
    webhook_timeout_seconds: float = 10.0
    # Public URL of this server, used for links inside webhook payloads.
    public_base_url: str | None = None
