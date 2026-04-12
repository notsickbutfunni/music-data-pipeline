from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ConfigError(ValueError):
    """Raised when required environment variables are missing."""


@dataclass(frozen=True)
class AppConfig:
    vocadb_base_url: str
    vocadb_timeout_seconds: int
    supabase_url: str
    supabase_service_key: str
    supabase_schema: str
    r2_account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket_name: str
    r2_public_base_url: Optional[str]
    r2_endpoint_url: str


def _load_dotenv_if_available(dotenv_path: Path) -> None:
    """Load values from a .env file without external dependencies."""
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ConfigError(f"Missing required environment variable: {name}")
    return value.strip()


def _optional_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    cleaned = value.strip()
    return cleaned if cleaned else default


def _int_env(name: str, default: int) -> int:
    raw = _optional_env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an integer.") from exc


def load_config(env_file: str = ".env") -> AppConfig:
    """Load and validate application configuration from environment variables."""
    _load_dotenv_if_available(Path(env_file))

    account_id = _require_env("R2_ACCOUNT_ID")

    return AppConfig(
        vocadb_base_url=_optional_env("VOCADB_BASE_URL", "https://vocadb.net/api") or "https://vocadb.net/api",
        vocadb_timeout_seconds=_int_env("VOCADB_TIMEOUT_SECONDS", 20),
        supabase_url=_require_env("SUPABASE_URL"),
        supabase_service_key=_require_env("SUPABASE_SERVICE_KEY"),
        supabase_schema=_optional_env("SUPABASE_SCHEMA", "public") or "public",
        r2_account_id=account_id,
        r2_access_key_id=_require_env("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=_require_env("R2_SECRET_ACCESS_KEY"),
        r2_bucket_name=_require_env("R2_BUCKET_NAME"),
        r2_public_base_url=_optional_env("R2_PUBLIC_BASE_URL"),
        r2_endpoint_url=_optional_env(
            "R2_ENDPOINT_URL", f"https://{account_id}.r2.cloudflarestorage.com"
        )
        or f"https://{account_id}.r2.cloudflarestorage.com",
    )
