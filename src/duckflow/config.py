"""Environment-driven settings.

Imported by activities and workers, never by workflow code -- workflows must not
read the environment, because a value that changes between the original run and
a replay would make the workflow take a different branch than its history says
it took. The workflow gets everything it needs from `PipelineInput`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # -- Temporal ----------------------------------------------------------
    temporal_address: str = field(default_factory=lambda: _env("TEMPORAL_ADDRESS", "localhost:7233"))
    temporal_namespace: str = field(default_factory=lambda: _env("TEMPORAL_NAMESPACE", "default"))
    max_concurrent_activities: int = field(
        default_factory=lambda: _env_int("MAX_CONCURRENT_ACTIVITIES", 8)
    )

    # -- Object store ------------------------------------------------------
    lake_uri: str = field(default_factory=lambda: _env("LAKE_URI", "s3://lake"))
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("AWS_ACCESS_KEY_ID", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    s3_region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    s3_use_ssl: bool = field(default_factory=lambda: _env("S3_USE_SSL", "0") == "1")

    # -- DuckDB ------------------------------------------------------------
    warehouse_path: str = field(
        default_factory=lambda: _env("WAREHOUSE_PATH", "/data/warehouse.duckdb")
    )
    """The single-writer database file. Lives on a volume the writer owns."""

    duckdb_temp_dir: str = field(default_factory=lambda: _env("DUCKDB_TEMP_DIR", "/data/duck-tmp"))
    """Spill directory. Set it and DuckDB can process more data than it has RAM."""

    duckdb_extension_dir: str = field(
        default_factory=lambda: _env("DUCKDB_EXTENSION_DIR", "/opt/duckdb-extensions")
    )
    """Baked into the image at build time so no activity needs network to
    `INSTALL httpfs` -- an activity that downloads an extension on first run is
    an activity whose first attempt times out."""

    default_threads: int = field(default_factory=lambda: _env_int("DUCKDB_THREADS", 4))
    default_memory_limit: str = field(
        default_factory=lambda: _env("DUCKDB_MEMORY_LIMIT", "2GB")
    )

    # -- Observability -----------------------------------------------------
    prometheus_port: int = field(default_factory=lambda: _env_int("PROMETHEUS_PORT", 0))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    log_json: bool = field(default_factory=lambda: _env("LOG_JSON", "0") == "1")


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings()
