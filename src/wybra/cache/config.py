from __future__ import annotations

import re
from typing import Final

from wybra.cache.feature_models import validate_feature_name
from wybra.config import ConfigDef, ConfigField, ConfigGroup, RepeatedConfigSection
from wybra.config.transforms import to_non_blank_string, to_optional_non_blank_string
from wybra.services.secrets import SecretsError, SecretSource, normalise_secret_source

CACHE_CONFIG_SECTION: Final = "cache"
DEFAULT_CACHE_BACKEND: Final = "memory"
DEFAULT_CACHE_NAME: Final = "default"
ENV_CACHE_BACKEND: Final = "WYBRA_CACHE_BACKEND"
ENV_CACHE_FEATURES: Final = "WYBRA_CACHE_FEATURES"
ENV_CACHE_NAMESPACE: Final = "WYBRA_CACHE_NAMESPACE"
ENV_CACHE_URL: Final = "WYBRA_CACHE_URL"
ENV_CACHE_URL_SOURCE: Final = "WYBRA_CACHE_URL_SOURCE"
ENV_CACHE_URL_KEY: Final = "WYBRA_CACHE_URL_KEY"
ENV_CACHE_CREDENTIALS_SOURCE: Final = "WYBRA_CACHE_CREDENTIALS_SOURCE"
ENV_CACHE_CREDENTIALS_KEY: Final = "WYBRA_CACHE_CREDENTIALS_KEY"
ENV_NAMED_CACHE_PREFIX: Final = "WYBRA_CACHE"
_CACHE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def to_cache_backend(value: object) -> str:
    backend = to_non_blank_string(value).lower()
    if backend not in {"memory", "redis"}:
        raise ValueError("cache backend must be 'memory' or 'redis'.")
    return backend


def to_cache_features(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        features: tuple[object, ...] = tuple(
            feature.strip() for feature in value.split(",") if feature.strip()
        )
    elif isinstance(value, list | tuple):
        features = tuple(value)
    else:
        raise ValueError(
            "cache features must be a list, tuple, or comma-separated string."
        )
    feature_names: list[str] = []
    for feature in features:
        if not isinstance(feature, str):
            raise ValueError("cache feature names must be strings.")
        feature_names.append(feature)
    feature_names_tuple = tuple(feature_names)
    for feature in feature_names_tuple:
        validate_feature_name(feature, label="cache feature name")
    duplicates = sorted(
        {
            feature
            for feature in feature_names_tuple
            if feature_names_tuple.count(feature) > 1
        }
    )
    if duplicates:
        raise ValueError(
            "cache feature names must not contain duplicates: "
            + ", ".join(duplicates)
            + "."
        )
    return feature_names_tuple


def to_cache_namespace(value: object) -> str | None:
    namespace = to_optional_non_blank_string(value)
    if namespace is None:
        return None
    if len(namespace) > 64 or _CACHE_NAME_PATTERN.fullmatch(namespace) is None:
        raise ValueError(
            "cache namespace must start with a lowercase letter, contain only "
            "lowercase letters, numbers, and underscores, and be at most 64 "
            "characters."
        )
    return namespace


def to_optional_secret_source(value: object) -> SecretSource | None:
    source = to_optional_non_blank_string(value)
    if source is None:
        return None
    try:
        return normalise_secret_source(source, name="cache secret source")
    except (SecretsError, ValueError) as exc:
        raise ValueError(str(exc)) from exc


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
        ConfigField(
            name="url_source",
            env=ENV_CACHE_URL_SOURCE,
            transform=to_optional_secret_source,
        ),
        ConfigField(
            name="url_key",
            env=ENV_CACHE_URL_KEY,
            transform=to_optional_non_blank_string,
        ),
        ConfigField(
            name="credentials_source",
            env=ENV_CACHE_CREDENTIALS_SOURCE,
            transform=to_optional_secret_source,
        ),
        ConfigField(
            name="credentials_key",
            env=ENV_CACHE_CREDENTIALS_KEY,
            transform=to_optional_non_blank_string,
        ),
        ConfigField(
            name="namespace",
            default=None,
            env=ENV_CACHE_NAMESPACE,
            transform=to_cache_namespace,
        ),
        ConfigField(
            name="features",
            default=None,
            env=ENV_CACHE_FEATURES,
            transform=to_cache_features,
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
            environment_fields=(
                "backend",
                "credentials_key",
                "credentials_source",
                "features",
                "namespace",
                "url",
                "url_key",
                "url_source",
            ),
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
    "ENV_CACHE_CREDENTIALS_KEY",
    "ENV_CACHE_CREDENTIALS_SOURCE",
    "ENV_CACHE_FEATURES",
    "ENV_CACHE_NAMESPACE",
    "ENV_NAMED_CACHE_PREFIX",
    "ENV_CACHE_URL",
    "ENV_CACHE_URL_KEY",
    "ENV_CACHE_URL_SOURCE",
    "module_config",
    "to_cache_backend",
    "to_cache_features",
    "to_cache_name",
    "to_cache_namespace",
    "to_optional_secret_source",
    "validate_named_cache_name",
)
