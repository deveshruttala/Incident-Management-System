"""Environment-driven settings.

All knobs live here so the rest of the codebase can stay free of magic
numbers. Override any value via environment variable (e.g. `WORKER_COUNT=16`).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "incident-management-system"
    log_level: str = "INFO"

    # ---- storage -----------------------------------------------------------
    postgres_dsn: str = "postgresql://ims:ims@postgres:5432/ims"
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "ims"
    redis_url: str = "redis://redis:6379/0"

    # ---- ingestion / backpressure -----------------------------------------
    queue_max_size: int = 50_000           # bounded buffer → 429 when full
    worker_count: int = 8                  # async consumers
    debounce_window_seconds: int = 10      # 100 signals/10 s rule
    debounce_max_signals: int = 100

    # ---- rate limiting (per-IP token bucket) ------------------------------
    rate_limit_per_second: int = 12_000
    rate_limit_burst: int = 20_000

    # ---- observability -----------------------------------------------------
    metrics_print_interval_seconds: int = 5

    # ---- security ----------------------------------------------------------
    # If set, all `/ingest*` requests must carry `X-API-Key: <value>`.
    # Empty / unset → auth disabled (useful for local dev).
    ingest_api_key: str = ""
    # Comma-separated list of allowed CORS origins. `*` = unrestricted.
    cors_allow_origins: str = "*"
    # Cap individual signal payloads (defence-in-depth against memory abuse).
    max_request_body_bytes: int = 1_048_576  # 1 MiB


settings = Settings()
