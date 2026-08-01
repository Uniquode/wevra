from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Final

from wybra.cache import to_cache_name
from wybra.config import ConfigDef, ConfigField, ConfigGroup
from wybra.config.transforms import (
    to_non_blank_string,
    to_optional_non_blank_string,
    to_positive_float,
    to_positive_int,
)

SESSIONS_CONFIG_SECTION: Final = "wybra.sessions"
ENV_SESSIONS_STORAGE_BACKEND: Final = "SESSIONS_STORAGE_BACKEND"
ENV_SESSIONS_LIFETIME_SECONDS: Final = "SESSIONS_LIFETIME_SECONDS"
ENV_SESSIONS_COOKIE_NAME: Final = "SESSIONS_COOKIE_NAME"
ENV_SESSIONS_COOKIE_DOMAIN: Final = "SESSIONS_COOKIE_DOMAIN"
ENV_SESSIONS_COOKIE_PATH: Final = "SESSIONS_COOKIE_PATH"
ENV_SESSIONS_COOKIE_SECURE: Final = "SESSIONS_COOKIE_SECURE"
ENV_SESSIONS_COOKIE_SAME_SITE: Final = "SESSIONS_COOKIE_SAME_SITE"
ENV_SESSIONS_FILE_DIRECTORY: Final = "SESSIONS_FILE_DIRECTORY"
ENV_SESSIONS_CACHE_NAME: Final = "SESSIONS_CACHE_NAME"
ENV_SESSIONS_CACHE_KEY_PREFIX: Final = "SESSIONS_CACHE_KEY_PREFIX"
ENV_SESSIONS_DATABASE_CONNECTION: Final = "SESSIONS_DATABASE_CONNECTION"
ENV_SESSIONS_PAYLOAD_MAX_BYTES: Final = "SESSIONS_PAYLOAD_MAX_BYTES"
ENV_SESSIONS_COOKIE_PAYLOAD_MAX_BYTES: Final = "SESSIONS_COOKIE_PAYLOAD_MAX_BYTES"

DEFAULT_SESSION_LIFETIME_SECONDS: Final = 14 * 24 * 60 * 60
DEFAULT_SESSION_COOKIE_NAME: Final = "wybra_session"
DEFAULT_SESSION_COOKIE_PATH: Final = "/"
DEFAULT_SESSION_COOKIE_SAME_SITE: Final = "lax"
DEFAULT_SESSION_FILE_DIRECTORY: Final = Path(".wybra/sessions")
DEFAULT_SESSION_CACHE_KEY_PREFIX: Final = "wybra:sessions:"
DEFAULT_SESSION_DATABASE_CONNECTION_NAME: Final = "default"
DEFAULT_SESSION_PAYLOAD_MAX_BYTES: Final = 65_536
DEFAULT_SESSION_COOKIE_PAYLOAD_MAX_BYTES: Final = 3_800


class SessionStorageBackend(StrEnum):
    MEMORY = "memory"
    COOKIE = "cookie"
    FILE = "file"
    CACHE = "cache"
    DATABASE = "database"


def storage_backend_choices() -> str:
    return ", ".join(repr(backend.value) for backend in SessionStorageBackend)


def to_optional_storage_backend(value: object) -> SessionStorageBackend | None:
    if value is None:
        return None
    if isinstance(value, SessionStorageBackend):
        return value
    if not isinstance(value, str):
        raise ValueError("sessions storage backend must be a string.")
    try:
        return SessionStorageBackend(value.strip().lower())
    except ValueError as exc:
        raise ValueError(
            f"sessions storage backend must be one of: {storage_backend_choices()}."
        ) from exc


def to_optional_cache_name(value: object) -> str | None:
    if value is None:
        return None
    return to_cache_name(value)


def to_optional_bool(value: object) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"1", "true", "yes", "on"}:
            return True
        if normalised in {"0", "false", "no", "off"}:
            return False
    raise ValueError("must be a boolean when configured.")


def to_cookie_same_site(value: object) -> str:
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"lax", "strict", "none"}:
            return normalised
    raise ValueError("must be one of: 'lax', 'strict', 'none'.")


def to_optional_path(value: object) -> Path | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    if isinstance(value, str) and value.strip():
        return Path(value.strip())
    raise ValueError("must be a non-blank path when configured.")


module_config: Final = ConfigDef(
    {
        SESSIONS_CONFIG_SECTION: ConfigGroup(
            fields=(
                ConfigField(
                    name="storage_backend",
                    default=None,
                    env=ENV_SESSIONS_STORAGE_BACKEND,
                    transform=to_optional_storage_backend,
                ),
                ConfigField(
                    name="lifetime_seconds",
                    default=DEFAULT_SESSION_LIFETIME_SECONDS,
                    env=ENV_SESSIONS_LIFETIME_SECONDS,
                    transform=to_positive_float,
                ),
                ConfigField(
                    name="cookie_name",
                    default=DEFAULT_SESSION_COOKIE_NAME,
                    env=ENV_SESSIONS_COOKIE_NAME,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="cookie_domain",
                    default=None,
                    env=ENV_SESSIONS_COOKIE_DOMAIN,
                    transform=to_optional_non_blank_string,
                ),
                ConfigField(
                    name="cookie_path",
                    default=DEFAULT_SESSION_COOKIE_PATH,
                    env=ENV_SESSIONS_COOKIE_PATH,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="cookie_secure",
                    default=None,
                    env=ENV_SESSIONS_COOKIE_SECURE,
                    transform=to_optional_bool,
                ),
                ConfigField(
                    name="cookie_same_site",
                    default=DEFAULT_SESSION_COOKIE_SAME_SITE,
                    env=ENV_SESSIONS_COOKIE_SAME_SITE,
                    transform=to_cookie_same_site,
                ),
                ConfigField(
                    name="file_directory",
                    default=None,
                    env=ENV_SESSIONS_FILE_DIRECTORY,
                    transform=to_optional_path,
                ),
                ConfigField(
                    name="cache_name",
                    default=None,
                    env=ENV_SESSIONS_CACHE_NAME,
                    transform=to_optional_cache_name,
                ),
                ConfigField(
                    name="cache_key_prefix",
                    default=DEFAULT_SESSION_CACHE_KEY_PREFIX,
                    env=ENV_SESSIONS_CACHE_KEY_PREFIX,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="database_connection_name",
                    default=DEFAULT_SESSION_DATABASE_CONNECTION_NAME,
                    env=ENV_SESSIONS_DATABASE_CONNECTION,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="payload_max_bytes",
                    default=DEFAULT_SESSION_PAYLOAD_MAX_BYTES,
                    env=ENV_SESSIONS_PAYLOAD_MAX_BYTES,
                    transform=to_positive_int,
                ),
                ConfigField(
                    name="cookie_payload_max_bytes",
                    default=DEFAULT_SESSION_COOKIE_PAYLOAD_MAX_BYTES,
                    env=ENV_SESSIONS_COOKIE_PAYLOAD_MAX_BYTES,
                    transform=to_positive_int,
                ),
            ),
        ),
    }
)


__all__ = (
    "DEFAULT_SESSION_CACHE_KEY_PREFIX",
    "DEFAULT_SESSION_COOKIE_NAME",
    "DEFAULT_SESSION_COOKIE_PATH",
    "DEFAULT_SESSION_COOKIE_PAYLOAD_MAX_BYTES",
    "DEFAULT_SESSION_COOKIE_SAME_SITE",
    "DEFAULT_SESSION_DATABASE_CONNECTION_NAME",
    "DEFAULT_SESSION_FILE_DIRECTORY",
    "DEFAULT_SESSION_LIFETIME_SECONDS",
    "DEFAULT_SESSION_PAYLOAD_MAX_BYTES",
    "ENV_SESSIONS_CACHE_KEY_PREFIX",
    "ENV_SESSIONS_CACHE_NAME",
    "ENV_SESSIONS_COOKIE_DOMAIN",
    "ENV_SESSIONS_COOKIE_NAME",
    "ENV_SESSIONS_COOKIE_PATH",
    "ENV_SESSIONS_COOKIE_PAYLOAD_MAX_BYTES",
    "ENV_SESSIONS_COOKIE_SAME_SITE",
    "ENV_SESSIONS_COOKIE_SECURE",
    "ENV_SESSIONS_DATABASE_CONNECTION",
    "ENV_SESSIONS_FILE_DIRECTORY",
    "ENV_SESSIONS_LIFETIME_SECONDS",
    "ENV_SESSIONS_PAYLOAD_MAX_BYTES",
    "ENV_SESSIONS_STORAGE_BACKEND",
    "SESSIONS_CONFIG_SECTION",
    "SessionStorageBackend",
    "module_config",
    "storage_backend_choices",
    "to_cookie_same_site",
    "to_non_blank_string",
    "to_optional_bool",
    "to_optional_cache_name",
    "to_optional_non_blank_string",
    "to_optional_path",
    "to_optional_storage_backend",
    "to_positive_float",
    "to_positive_int",
)
