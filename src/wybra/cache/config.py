from __future__ import annotations

import re
from typing import Final

from wybra.config import ConfigDef, ConfigField, ConfigGroup, RepeatedConfigSection
from wybra.config.transforms import to_non_blank_string, to_optional_non_blank_string

CACHE_CONFIG_SECTION: Final = "cache"
DEFAULT_CACHE_BACKEND: Final = "memory"
DEFAULT_CACHE_NAME: Final = "default"
ENV_CACHE_BACKEND: Final = "WYBRA_CACHE_BACKEND"
ENV_CACHE_URL: Final = "WYBRA_CACHE_URL"
ENV_NAMED_CACHE_PREFIX: Final = "WYBRA_CACHE"
_CACHE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def to_cache_backend(value: object) -> str:
    backend = to_non_blank_string(value).lower()
    if backend not in {"memory", "redis"}:
        raise ValueError("cache backend must be 'memory' or 'redis'.")
    return backend


CACHE_INSTANCE_CONFIG: Final = ConfigGroup(
    fields=(
        ConfigField(
            name="backend",
            default=DEFAULT_CACHE_BACKEND,
            env=ENV_CACHE_BACKEND,
            transform=to_cache_backend,
        ),
        ConfigField(
            name="url",
            default=None,
            env=ENV_CACHE_URL,
            transform=to_optional_non_blank_string,
        ),
    )
)


def to_cache_name(value: object, *, nested: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError("cache name must be a string.")
    if value != value.strip():
        raise ValueError("cache name must not contain surrounding whitespace.")
    if nested and value == DEFAULT_CACHE_NAME:
        raise ValueError(
            "cache name 'default' is reserved for the root [cache] section."
        )
    if not _CACHE_NAME_PATTERN.fullmatch(value):
        raise ValueError(
            f"invalid cache name {value!r}; use lower-case letters, numbers, "
            "and underscores, starting with a letter."
        )
    return value


def validate_named_cache_name(value: str) -> None:
    to_cache_name(value, nested=True)


module_config: Final = ConfigDef(
    {CACHE_CONFIG_SECTION: CACHE_INSTANCE_CONFIG},
    repeated_sections={
        CACHE_CONFIG_SECTION: RepeatedConfigSection(
            group=CACHE_INSTANCE_CONFIG,
            environment_prefix=ENV_NAMED_CACHE_PREFIX,
            environment_fields=("backend", "url"),
            name_validator=validate_named_cache_name,
        )
    },
)


__all__ = (
    "CACHE_INSTANCE_CONFIG",
    "CACHE_CONFIG_SECTION",
    "DEFAULT_CACHE_BACKEND",
    "DEFAULT_CACHE_NAME",
    "ENV_CACHE_BACKEND",
    "ENV_NAMED_CACHE_PREFIX",
    "ENV_CACHE_URL",
    "module_config",
    "to_cache_backend",
    "to_cache_name",
    "validate_named_cache_name",
)
