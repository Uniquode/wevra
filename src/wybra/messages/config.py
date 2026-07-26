from __future__ import annotations

from enum import StrEnum
from typing import Final

from wybra.cache import to_cache_name
from wybra.config import ConfigDef, ConfigField, ConfigGroup
from wybra.config.transforms import (
    to_non_blank_string,
    to_optional_non_blank_string,
    to_positive_float,
    to_positive_int,
)

MESSAGES_CONFIG_SECTION: Final = "wybra.messages"
ENV_MESSAGES_STORAGE_BACKEND: Final = "MESSAGES_STORAGE_BACKEND"
ENV_MESSAGES_QUEUE_DEPTH: Final = "MESSAGES_QUEUE_DEPTH"
ENV_MESSAGES_MESSAGE_MAX_LENGTH: Final = "MESSAGES_MESSAGE_MAX_LENGTH"
ENV_MESSAGES_TTL_SECONDS: Final = "MESSAGES_TTL_SECONDS"
ENV_MESSAGES_CACHE_NAME: Final = "MESSAGES_CACHE_NAME"
ENV_MESSAGES_CACHE_URL: Final = "MESSAGES_CACHE_URL"
ENV_MESSAGES_CACHE_KEY_PREFIX: Final = "MESSAGES_CACHE_KEY_PREFIX"
ENV_MESSAGES_DATABASE_CONNECTION: Final = "MESSAGES_DATABASE_CONNECTION"
DEFAULT_QUEUE_DEPTH: Final = 20
DEFAULT_MESSAGE_MAX_LENGTH: Final = 1000
DEFAULT_MESSAGE_TTL_SECONDS: Final = 86_400.0
DEFAULT_CACHE_KEY_PREFIX: Final = "wybra:messages:"
DEFAULT_DATABASE_CONNECTION_NAME: Final = "default"


class MessageStorageBackend(StrEnum):
    SESSION = "session"
    CACHE = "cache"
    DATABASE = "database"


def storage_backend_choices() -> str:
    return ", ".join(repr(backend.value) for backend in MessageStorageBackend)


def to_storage_backend(value: object) -> MessageStorageBackend:
    if not isinstance(value, MessageStorageBackend | str):
        raise ValueError("messages storage backend must be a string.")
    if isinstance(value, MessageStorageBackend):
        return value
    try:
        return MessageStorageBackend(value.strip().lower())
    except ValueError as exc:
        raise ValueError(
            f"messages storage backend must be one of: {storage_backend_choices()}."
        ) from exc


def to_optional_cache_name(value: object) -> str | None:
    if value is None:
        return None
    return to_cache_name(value)


module_config: Final = ConfigDef(
    {
        MESSAGES_CONFIG_SECTION: ConfigGroup(
            fields=(
                ConfigField(
                    name="storage_backend",
                    default=MessageStorageBackend.SESSION.value,
                    env=ENV_MESSAGES_STORAGE_BACKEND,
                    transform=to_storage_backend,
                ),
                ConfigField(
                    name="queue_depth",
                    default=DEFAULT_QUEUE_DEPTH,
                    env=ENV_MESSAGES_QUEUE_DEPTH,
                    transform=to_positive_int,
                ),
                ConfigField(
                    name="message_max_length",
                    default=DEFAULT_MESSAGE_MAX_LENGTH,
                    env=ENV_MESSAGES_MESSAGE_MAX_LENGTH,
                    transform=to_positive_int,
                ),
                ConfigField(
                    name="message_ttl_seconds",
                    default=DEFAULT_MESSAGE_TTL_SECONDS,
                    env=ENV_MESSAGES_TTL_SECONDS,
                    transform=to_positive_float,
                ),
                ConfigField(
                    name="cache_name",
                    default=None,
                    env=ENV_MESSAGES_CACHE_NAME,
                    transform=to_optional_cache_name,
                ),
                ConfigField(
                    name="cache_url",
                    default=None,
                    env=ENV_MESSAGES_CACHE_URL,
                    transform=to_optional_non_blank_string,
                ),
                ConfigField(
                    name="cache_key_prefix",
                    default=DEFAULT_CACHE_KEY_PREFIX,
                    env=ENV_MESSAGES_CACHE_KEY_PREFIX,
                    transform=to_non_blank_string,
                ),
                ConfigField(
                    name="database_connection_name",
                    default=DEFAULT_DATABASE_CONNECTION_NAME,
                    env=ENV_MESSAGES_DATABASE_CONNECTION,
                    transform=to_non_blank_string,
                ),
            ),
        ),
    }
)


__all__ = (
    "DEFAULT_CACHE_KEY_PREFIX",
    "DEFAULT_DATABASE_CONNECTION_NAME",
    "DEFAULT_MESSAGE_MAX_LENGTH",
    "DEFAULT_MESSAGE_TTL_SECONDS",
    "DEFAULT_QUEUE_DEPTH",
    "ENV_MESSAGES_CACHE_KEY_PREFIX",
    "ENV_MESSAGES_CACHE_NAME",
    "ENV_MESSAGES_CACHE_URL",
    "ENV_MESSAGES_DATABASE_CONNECTION",
    "ENV_MESSAGES_MESSAGE_MAX_LENGTH",
    "ENV_MESSAGES_QUEUE_DEPTH",
    "ENV_MESSAGES_STORAGE_BACKEND",
    "ENV_MESSAGES_TTL_SECONDS",
    "MESSAGES_CONFIG_SECTION",
    "MessageStorageBackend",
    "module_config",
    "storage_backend_choices",
    "to_non_blank_string",
    "to_optional_cache_name",
    "to_optional_non_blank_string",
    "to_positive_float",
    "to_positive_int",
    "to_storage_backend",
)
