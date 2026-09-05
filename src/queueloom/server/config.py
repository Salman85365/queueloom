from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Server configuration. Every field can be set as ``QUEUELOOM_<FIELD>``."""

    model_config = SettingsConfigDict(env_prefix="QUEUELOOM_", extra="ignore")

    database_url: str = "sqlite:///./queueloom.db"
    host: str = "127.0.0.1"
    port: int = 8800
    log_level: str = "info"
    # Auto-create tables on startup. Handy for self-hosting; disable once migrations exist.
    auto_create_schema: bool = True
    dashboard_refresh_seconds: int = 15
    # Upper bound on runs scanned for percentile statistics in one request.
    stats_sample_limit: int = 50_000
