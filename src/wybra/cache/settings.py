from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, ClassVar, Self
from urllib.parse import parse_qs, unquote, urlsplit

from wybra.cache.config import (
    CACHE_CONFIG_SECTION,
    CACHE_INSTANCE_CONFIG,
    DEFAULT_CACHE_BACKEND,
    DEFAULT_CACHE_NAME,
    module_config,
    to_cache_backend,
    to_cache_name,
)
from wybra.config import BaseSettings, ConfigDef, ConfigService
from wybra.config.transforms import to_optional_non_blank_string
from wybra.core.exceptions import ConfigurationError


@dataclass(frozen=True, slots=True)
class CacheSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = CACHE_CONFIG_SECTION

    backend: str = DEFAULT_CACHE_BACKEND
    url: str | None = field(default=None, repr=False)
    name: str = DEFAULT_CACHE_NAME

    @classmethod
    def load_settings(cls, config: ConfigService | Mapping[str, Any]) -> Self:
        values = cls.settings_kwargs(config)
        return cls(**values)

    def __post_init__(self) -> None:
        name = _cache_name(self.name)
        try:
            backend = to_cache_backend(self.backend)
        except ValueError as exc:
            raise ConfigurationError(f"{_section_name(name)}.backend: {exc}") from exc
        try:
            url = to_optional_non_blank_string(self.url)
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{_section_name(name)}.url: {exc}") from exc
        if backend == "redis" and url is None:
            raise ConfigurationError(
                f"{_section_name(name)}.url is required when backend is 'redis'."
            )
        if backend == "memory" and url is not None:
            raise ConfigurationError(
                f"{_section_name(name)}.url is not valid when backend is 'memory'."
            )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "backend", backend)
        object.__setattr__(self, "url", url)

    @property
    def partition(self) -> str:
        if self.backend == "memory":
            return f"process:{self.name}"
        if self.url is None:
            raise ConfigurationError(
                f"{_section_name(self.name)}.url is required when backend is 'redis'."
            )
        return _redis_partition(self.url)


@dataclass(frozen=True, slots=True)
class CacheConfigurationDiagnostic:
    name: str
    backend: str
    partition: str
    features: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CachesSettings(BaseSettings):
    module_config: ClassVar[ConfigDef] = module_config
    config_section: ClassVar[str | None] = CACHE_CONFIG_SECTION

    instances: tuple[CacheSettings, ...]

    def __post_init__(self) -> None:
        names = tuple(settings.name for settings in self.instances)
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ConfigurationError(
                "Configured cache names contain duplicates: "
                + ", ".join(duplicates)
                + "."
            )
        if DEFAULT_CACHE_NAME not in names:
            raise ConfigurationError("Configured caches must include 'default'.")

    @classmethod
    def load_settings(
        cls,
        config: ConfigService | Mapping[str, Any],
    ) -> Self:
        sections = _cache_sections(config)
        instances = [
            _load_cache_settings(
                DEFAULT_CACHE_NAME,
                sections.get(CACHE_CONFIG_SECTION, {}),
            )
        ]
        for section_name in sorted(sections):
            if not section_name.startswith(f"{CACHE_CONFIG_SECTION}."):
                continue
            name = section_name.removeprefix(f"{CACHE_CONFIG_SECTION}.")
            instances.append(
                _load_cache_settings(
                    name,
                    sections[section_name],
                    nested=True,
                )
            )
        return cls(instances=tuple(instances))

    def require(self, name: str) -> CacheSettings:
        requested = _cache_name(name)
        for settings in self.instances:
            if settings.name == requested:
                return settings
        raise ConfigurationError(f"Configured cache {requested!r} was not found.")

    def diagnostics(self) -> tuple[CacheConfigurationDiagnostic, ...]:
        return tuple(
            CacheConfigurationDiagnostic(
                name=settings.name,
                backend=settings.backend,
                partition=settings.partition,
            )
            for settings in self.instances
        )


def _cache_sections(
    config: ConfigService | Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    source = config.config.values if isinstance(config, ConfigService) else config
    sections: dict[str, Mapping[str, Any]] = {}
    for section_name, values in source.items():
        if section_name != CACHE_CONFIG_SECTION and not section_name.startswith(
            f"{CACHE_CONFIG_SECTION}."
        ):
            continue
        if not isinstance(values, Mapping):
            raise ConfigurationError(
                f"Config section {section_name!r} must be a mapping."
            )
        sections[section_name] = values
    return sections


def _load_cache_settings(
    name: str,
    configured: Mapping[str, Any],
    *,
    nested: bool = False,
) -> CacheSettings:
    name = _cache_name(name, nested=nested)
    section_name = _section_name(name)
    unknown = set(configured) - CACHE_INSTANCE_CONFIG.field_names
    if unknown:
        raise ConfigurationError(
            f"{section_name} contains unsupported field(s): "
            + ", ".join(sorted(unknown))
        )
    values = dict(configured)
    backend_value = values.get("backend", DEFAULT_CACHE_BACKEND)
    try:
        backend = to_cache_backend(backend_value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.backend: {exc}") from exc
    try:
        url = to_optional_non_blank_string(values.get("url"))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{section_name}.url: {exc}") from exc
    return CacheSettings(name=name, backend=backend, url=url)


def _cache_name(value: object, *, nested: bool = False) -> str:
    try:
        return to_cache_name(value, nested=nested)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(str(exc)) from exc


def _section_name(name: str) -> str:
    if name == DEFAULT_CACHE_NAME:
        return CACHE_CONFIG_SECTION
    return f"{CACHE_CONFIG_SECTION}.{name}"


def _redis_partition(url: str) -> str:
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        query_database = query.get("db", (None,))[0]
        database = _redis_database(query_database, parsed.path, parsed.scheme)
        if parsed.scheme.lower() == "unix":
            endpoint = f"unix:{unquote(parsed.path)}"
        else:
            port = parsed.port or 6379
            endpoint = (
                f"{parsed.scheme.lower()}:{parsed.hostname or 'localhost'}:{port}"
            )
        target = f"{endpoint}:{database}"
    except ValueError:
        target = "redis:configured"
    fingerprint = sha256(target.encode("utf-8")).hexdigest()[:12]
    return f"redis:{fingerprint}"


def _redis_database(
    query_database: str | None,
    path: str,
    scheme: str,
) -> int | str:
    if query_database is not None:
        try:
            return int(query_database)
        except ValueError:
            return "configured"
    if scheme.lower() not in {"redis", "rediss"}:
        return 0
    try:
        return int(unquote(path).replace("/", "")) if path else 0
    except ValueError:
        return 0


__all__ = (
    "CacheConfigurationDiagnostic",
    "CacheSettings",
    "CachesSettings",
    "DEFAULT_CACHE_NAME",
)
