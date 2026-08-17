from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str | None
    database_path: str
    verify_webhook_signatures: bool
    strict_webhook_signatures: bool
    disable_workers: bool
    max_send_attempts: int
    send_rate_limit: int
    send_rate_window_seconds: int
    worker_poll_seconds: float
    status_poll_seconds: float


def get_settings() -> Settings:
    api_key = os.getenv("PSEUDOGRAM_API_KEY") or None
    return Settings(
        api_base_url=os.getenv("PSEUDOGRAM_API_BASE_URL", "https://pseudogram-api.onrender.com").rstrip("/"),
        api_key=api_key,
        database_path=os.getenv("DATABASE_PATH", "./linkplease.db"),
        verify_webhook_signatures=_bool_env("VERIFY_WEBHOOK_SIGNATURES", True),
        strict_webhook_signatures=_bool_env("STRICT_WEBHOOK_SIGNATURES", False),
        disable_workers=_bool_env("DISABLE_WORKERS", False),
        max_send_attempts=int(os.getenv("MAX_SEND_ATTEMPTS", "12")),
        send_rate_limit=int(os.getenv("SEND_RATE_LIMIT", "10")),
        send_rate_window_seconds=int(os.getenv("SEND_RATE_WINDOW_SECONDS", "60")),
        worker_poll_seconds=float(os.getenv("WORKER_POLL_SECONDS", "0.25")),
        status_poll_seconds=float(os.getenv("STATUS_POLL_SECONDS", "3.0")),
    )
