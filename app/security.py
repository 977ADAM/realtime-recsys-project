import os
from typing import Optional

if __package__:
    from .runtime_utils import env_bool
else:  # pragma: no cover - fallback for direct script execution
    from runtime_utils import env_bool


_INSECURE_MARKERS = (
    "change_me",
    "changeme",
    "password",
    "example",
    "default",
)


def runtime_environment() -> str:
    return (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "dev").strip().lower()


def is_production_env() -> bool:
    return runtime_environment() in {"prod", "production"}


def _is_insecure_secret(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    return any(marker in lowered for marker in _INSECURE_MARKERS)


def _db_secret_text() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return database_url
    user = os.getenv("POSTGRES_USER", "").strip()
    password = os.getenv("POSTGRES_PASSWORD", "").strip()
    db = os.getenv("POSTGRES_DB", "").strip()
    return f"{user}:{password}@{db}"


def enforce_runtime_security(component: str) -> Optional[str]:
    if not is_production_env():
        return None

    if env_bool("ALLOW_INSECURE_PROD_DEFAULTS", False):
        return None

    secret_text = _db_secret_text()
    if _is_insecure_secret(secret_text):
        return (
            f"{component}: insecure DATABASE_URL/POSTGRES_* secret value is not allowed in APP_ENV=production. "
            "Provide secrets via environment/secret manager and avoid placeholder credentials."
        )

    return None
